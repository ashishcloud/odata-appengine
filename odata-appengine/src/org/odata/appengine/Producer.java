package org.odata.appengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.core4j.Enumerable;
import org.core4j.Func1;
import org.joda.time.LocalDateTime;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityId;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OExtension;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OLink;
import org.odata4j.core.OLinks;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmMultiplicity;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotAuthorizedException;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.NotImplementedException;
import org.odata4j.expression.AndExpression;
import org.odata4j.expression.BinaryCommonExpression;
import org.odata4j.expression.BoolCommonExpression;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.EqExpression;
import org.odata4j.expression.Expression;
import org.odata4j.expression.GeExpression;
import org.odata4j.expression.GtExpression;
import org.odata4j.expression.LeExpression;
import org.odata4j.expression.LiteralExpression;
import org.odata4j.expression.LtExpression;
import org.odata4j.expression.NeExpression;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.expression.OrderByExpression.Direction;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.EntityIdResponse;
import org.odata4j.producer.EntityQueryInfo;
import org.odata4j.producer.EntityResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;
import org.odata4j.producer.edm.MetadataProducer;

import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;

public class Producer implements ODataProducer {

	private static final String ACCESS_CONTROL_KIND = "AccessControl";

	@SuppressWarnings("unchecked")
	private static final Set<EdmType> SUPPORTED_TYPES = Enumerable.create(EdmSimpleType.BOOLEAN, EdmSimpleType.BYTE, EdmSimpleType.STRING, EdmSimpleType.INT16, EdmSimpleType.INT32, EdmSimpleType.INT64, EdmSimpleType.SINGLE, EdmSimpleType.DOUBLE, EdmSimpleType.DATETIME, EdmSimpleType.BINARY).cast(EdmType.class).toSet();

	private final EdmDataServices metadata;
	private final DatastoreService datastore;

	public Producer(EdmDataServices metadata) {
		this.metadata = metadata;
		this.datastore = DatastoreServiceFactory.getDatastoreService();
	}

	@Override
	public EdmDataServices getMetadata() {
		return metadata;
	}

	@Override
	public void close() {
		// noop
	}

	@Override
	public EntityResponse getEntity(String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
		Entity e = findEntity(entitySetName, entityKey);

		EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
		return Responses.entity(toOEntity(ees, e, queryInfo, null));
	}

	@Override
	public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
		final EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
		Query q = new Query(entitySetName);
		if (queryInfo.filter != null)
			applyFilter(q, queryInfo.filter);
		if (queryInfo.orderBy != null && queryInfo.orderBy.size() > 0)
			applySort(q, queryInfo.orderBy);
		PreparedQuery pq = datastore.prepare(q);

		Integer inlineCount = queryInfo.inlineCount == InlineCount.ALLPAGES ? pq.countEntities(FetchOptions.Builder.withDefaults()) : null;

		FetchOptions options = null;
		if (queryInfo.top != null)
			options = FetchOptions.Builder.withLimit(queryInfo.top);
		if (queryInfo.skip != null)
			options = options == null ? FetchOptions.Builder.withOffset(queryInfo.skip) : options.offset(queryInfo.skip);

		Iterable<Entity> iter = options == null ? pq.asIterable() : pq.asIterable(options);

		final QueryInfo qi = queryInfo;
		List<OEntity> entities = Enumerable.create(iter).select(new Func1<Entity, OEntity>() {
			public OEntity apply(Entity input) {
				return toOEntity(ees, input, qi, null);
			}
		}).toList();

		return Responses.entities(entities, ees, inlineCount, null);
	}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntity entity) {
		Entity e = new Entity(entitySetName);
		applyProperties(e, entity.getProperties());
		applyLinks(e, entity.getLinks());
		datastore.put(e);
		EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
		return Responses.entity(toOEntity(ees, e, null, null));
	}

	@Override
	public void deleteEntity(String entitySetName, OEntityKey entityKey) {
		long id = Long.parseLong(entityKey.asSingleValue().toString());
		datastore.delete(KeyFactory.createKey(entitySetName, id));
	}

	@Override
	public void mergeEntity(String entitySetName, OEntity entity) {
		OEntityKey entityKey = entity.getEntityKey();
		Entity e = findEntity(entitySetName, entityKey);

		applyProperties(e, entity.getProperties());
		applyLinks(e, entity.getLinks());
		datastore.put(e);
	}

	@Override
	public void updateEntity(String entitySetName, OEntity entity) {
		OEntityKey entityKey = entity.getEntityKey();
		Entity e = findEntity(entitySetName, entityKey);

		// clear existing props
		for (String name : e.getProperties().keySet())
			e.removeProperty(name);

		applyProperties(e, entity.getProperties());
		applyLinks(e, entity.getLinks());
		datastore.put(e);
	}

	private OEntity toOEntity(EdmEntitySet ees, Entity entity, QueryInfo queryInfo, String parentPropName) {
		final List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
		final List<OLink> links = new ArrayList<OLink>();

		EdmEntityType eet = ees.getType();
		String entityKeyName = eet.getKeys().get(0);
		OEntityKey entityKey = OEntityKey.create(OProperties.int64(entityKeyName, entity.getKey().getId()));
		if (queryInfo == null || queryInfo.select == null || queryInfo.select.size() == 0 || containsProperty(queryInfo.select, entityKeyName, parentPropName)) {
			properties.add(OProperties.int64(entityKeyName, entity.getKey().getId()));
		}

		for (String propName : entity.getProperties().keySet()) {
			Object propValue = entity.getProperty(propName);
			if (propValue == null)
				continue;

			if (queryInfo != null && queryInfo.select != null && queryInfo.select.size() > 0) {
				if (!containsProperty(queryInfo.select, propName, parentPropName)) {
					continue;
				}
			}

			EdmProperty prop = eet.findProperty(propName);
			if (prop != null) {
				if (propValue instanceof Text) {
					propValue = ((Text) propValue).getValue();
				} else if (propValue instanceof ShortBlob) {
					propValue = ((ShortBlob) propValue).getBytes();
				}
				properties.add(OProperties.simple(propName, (EdmSimpleType<?>) prop.getType(), propValue));
			} else {
				EdmNavigationProperty navProp = eet.findNavigationProperty(propName);
				if (navProp != null) {
					List<OEntity> expandedProps = new ArrayList<OEntity>();
					try {
						if (queryInfo != null && queryInfo.expand != null && queryInfo.expand.size() > 0) {
							for (EntitySimpleProperty esp : queryInfo.expand) {
								if (esp.getPropertyName().equals(propName)) {
									EdmEntitySet eesNavProp = metadata.getEdmEntitySet(navProp.getToRole().getRole());
									EdmMultiplicity emNavProp = navProp.getRelationship().getEnd2().getMultiplicity();
									if (emNavProp == EdmMultiplicity.ZERO_TO_ONE) {
										Entity e = datastore.get((Key) propValue);
										expandedProps.add(toOEntity(eesNavProp, e, queryInfo, propName));
									} else if (emNavProp == EdmMultiplicity.MANY) {
										@SuppressWarnings("unchecked")
										Collection<Key> keys = (Collection<Key>) propValue;
										for (Key key : keys) {
											Entity e = datastore.get(key);
											expandedProps.add(toOEntity(eesNavProp, e, queryInfo, propName));
										}
									} else {
										throw new NotImplementedException("Property " + propName + " of type " + propValue.getClass().getName());
									}
								}
							}
						}
					} catch (EntityNotFoundException e) {
						e.printStackTrace();
					}
					if (expandedProps.size() == 0) {
						links.add(OLinks.relatedEntity(navProp.getRelationship().getName(), propName, null));
					} else {
						links.add(OLinks.relatedEntitiesInline(navProp.getRelationship().getName(), propName, null, expandedProps));
					}
				} else {
					throw new NotImplementedException("Property " + propName + " of type " + propValue.getClass().getName());
				}
			}
		}

		return OEntities.create(ees, entityKey, properties, links);
	}

	private boolean containsProperty(List<EntitySimpleProperty> properties, String propertyName, String parentPropName) {
		boolean containsProp = false;
		for (EntitySimpleProperty esp : properties) {
			if (parentPropName == null) {
				if (esp.getPropertyName().equals("*") || esp.getPropertyName().equals(propertyName) || esp.getPropertyName().startsWith(propertyName + "/")) {
					containsProp = true;
					break;
				}
			} else {
				if (esp.getPropertyName().equals(parentPropName) || esp.getPropertyName().equals(parentPropName + "/*") || esp.getPropertyName().equals(parentPropName + "/" + propertyName)) {
					containsProp = true;
					break;
				}
			}
		}
		return containsProp;
	}

	private void applyProperties(Entity e, List<OProperty<?>> properties) {
		for (OProperty<?> prop : properties) {
			EdmType type = prop.getType();
			if (!SUPPORTED_TYPES.contains(type)) {
				throw new NotImplementedException("EdmType not supported: " + type);
			}

			Object value = prop.getValue();
			if (type.equals(EdmSimpleType.STRING)) {
				String sValue = (String) value;
				if (sValue != null && sValue.length() > DataTypeUtils.MAX_STRING_PROPERTY_LENGTH) {
					value = new Text(sValue);
				}
			} else if (type.equals(EdmSimpleType.BINARY)) {
				byte[] bValue = (byte[]) value;
				if (bValue != null) {
					if (bValue.length > DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH) {
						throw new RuntimeException("Bytes " + bValue.length + " exceeds the max supported length " + DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH);
					}
					value = new ShortBlob(bValue);
				}
			} else if (type.equals(EdmSimpleType.DATETIME)) {
				LocalDateTime dValue = (LocalDateTime) value;
				if (dValue != null) {
					value = dValue.toDateTime().toDate(); // TODO review
				}
			}
			e.setProperty(prop.getName(), value);
		}
	}

	private void applyLinks(Entity e, List<OLink> links) {
		for (OLink link : links) {
			try {
				String uri = link.getHref();
				String entitySetName = e.getKind();
				String key = uri.substring(uri.lastIndexOf('('));
				EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
				EdmEntityType eet = ees.getType();
				EdmNavigationProperty enp = eet.findNavigationProperty(link.getRelation());
				EdmMultiplicity em = enp.getRelationship().getEnd2().getMultiplicity();
				Entity entity = findEntity(enp.getToRole().getType().getName(), OEntityKey.parse(key));
				if (em == EdmMultiplicity.ZERO_TO_ONE) {
					e.setProperty(link.getRelation(), entity.getKey());
				} else {
					e.setProperty(link.getRelation(), new ArrayList<Key>(Arrays.asList(entity.getKey())));
				}
			} catch (Exception ex) {

			}
		}
	}

	private Entity findEntity(String entitySetName, OEntityKey entityKey) {
		EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
		String kind = ees.getType().getName();

		long id = Long.parseLong(entityKey.asSingleValue().toString());
		try {
			return datastore.get(KeyFactory.createKey(kind, id));
		} catch (EntityNotFoundException e) {
			throw new NotFoundException("Entity " + entitySetName + " with key " + entityKey + " not found.");
		}
	}

	private void applySort(Query q, List<OrderByExpression> orderBy) {
		for (OrderByExpression ob : orderBy) {
			if (!(ob.getExpression() instanceof EntitySimpleProperty)) {
				throw new NotImplementedException("Appengine only supports simple property expressions");
			}
			String propName = ((EntitySimpleProperty) ob.getExpression()).getPropertyName();
			q.addSort(propName, ob.getDirection() == Direction.ASCENDING ? SortDirection.ASCENDING : SortDirection.DESCENDING);
		}
	}

	private void applyFilter(Query q, BoolCommonExpression filter) {
		// appengine supports simple filter predicates (name op value)

		// one filter
		if (filter instanceof EqExpression)
			applyFilter(q, (EqExpression) filter, FilterOperator.EQUAL);
		else if (filter instanceof NeExpression)
			applyFilter(q, (NeExpression) filter, FilterOperator.NOT_EQUAL);
		else if (filter instanceof GtExpression)
			applyFilter(q, (GtExpression) filter, FilterOperator.GREATER_THAN);
		else if (filter instanceof GeExpression)
			applyFilter(q, (GeExpression) filter, FilterOperator.GREATER_THAN_OR_EQUAL);
		else if (filter instanceof LtExpression)
			applyFilter(q, (LtExpression) filter, FilterOperator.LESS_THAN);
		else if (filter instanceof LeExpression)
			applyFilter(q, (LeExpression) filter, FilterOperator.LESS_THAN_OR_EQUAL);

		// and filter
		else if (filter instanceof AndExpression) {
			AndExpression e = (AndExpression) filter;
			applyFilter(q, e.getLHS());
			applyFilter(q, e.getRHS());
		}

		else
			throw new NotImplementedException("Appengine only supports simple property expressions");
	}

	private void applyFilter(Query q, BinaryCommonExpression e, FilterOperator op) {
		if (!(e.getLHS() instanceof EntitySimpleProperty))
			throw new NotImplementedException("Appengine only supports simple property expressions");
		if (!(e.getRHS() instanceof LiteralExpression))
			throw new NotImplementedException("Appengine only supports simple property expressions");

		EntitySimpleProperty lhs = (EntitySimpleProperty) e.getLHS();
		LiteralExpression rhs = (LiteralExpression) e.getRHS();

		String propName = lhs.getPropertyName();
		Object propValue = Expression.literalValue(rhs);

		// Support for filtering navigation properties by key
		if (propName.contains("/")) {
			propName = propName.substring(0, propName.indexOf("/"));
			EdmEntitySet ees = metadata.getEdmEntitySet(q.getKind());
			EdmEntityType eet = ees.getType();
			EdmNavigationProperty enp = eet.findNavigationProperty(propName);
			long id = Long.parseLong(propValue.toString());
			propValue = KeyFactory.createKey(enp.getToRole().getRole(), id);
		}

		Filter filter = new FilterPredicate(propName, op, propValue);
		q.setFilter(filter);
	}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
		throw new NotImplementedException();
	}

	@Override
	public BaseResponse getNavProperty(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
		final EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
		EdmEntityType eet = ees.getType();
		EdmNavigationProperty enp = eet.findNavigationProperty(navProp);
		if (enp == null) {
			throw new NotFoundException("EdmNavigationProperty " + navProp + " not found.");
		}
		final EdmEntitySet eesNavProp = metadata.getEdmEntitySet(enp.getToRole().getRole());
		EdmMultiplicity relMultiplicity = enp.getRelationship().getEnd2().getMultiplicity();

		Entity entity = findEntity(entitySetName, entityKey);
		Object navPropValue = entity.getProperties().get(navProp);
		if (navPropValue == null) {
			return Responses.entities(new ArrayList<OEntity>(), eesNavProp, 0, null);
		}
		if (relMultiplicity == EdmMultiplicity.ZERO_TO_ONE) {
			try {
				Entity relatedEntity = datastore.get((Key) navPropValue);
				return Responses.entity(toOEntity(eesNavProp, relatedEntity, queryInfo, null));
			} catch (EntityNotFoundException exception) {
				exception.printStackTrace();
				throw new NotImplementedException();
			}
		} else if (relMultiplicity == EdmMultiplicity.MANY) {
			try {
				@SuppressWarnings("unchecked")
				Collection<Key> relatedKeys = (Collection<Key>) navPropValue;
				Map<Key, Entity> relatedEntities = datastore.get(relatedKeys);
				final QueryInfo qi = queryInfo;

				Iterable<Entity> iter = relatedEntities.values();
				List<OEntity> entities = Enumerable.create(iter).select(new Func1<Entity, OEntity>() {
					public OEntity apply(Entity input) {
						return toOEntity(eesNavProp, input, qi, null);
					}
				}).toList();

				Integer inlineCount = queryInfo.inlineCount == InlineCount.ALLPAGES ? entities.size() : null;

				return Responses.entities(entities, eesNavProp, inlineCount, null);
			} catch (Exception exception) {
				exception.printStackTrace();
				throw new NotImplementedException();
			}
		} else {
			throw new NotImplementedException();
		}
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		throw new NotImplementedException();
	}

	@Override
	public EntityIdResponse getLinks(OEntityId sourceEntity, String targetNavProp) {
		BaseResponse r = getNavProperty(sourceEntity.getEntitySetName(), sourceEntity.getEntityKey(), targetNavProp, null);
		if (r instanceof EntitiesResponse) {
			EntitiesResponse er = (EntitiesResponse) r;
			return Responses.multipleIds(er.getEntities());
		}
		if (r instanceof EntityResponse) {
			EntityResponse er = (EntityResponse) r;
			return Responses.singleId(er.getEntity());
		}
		throw new NotFoundException("EdmNavigationProperty " + targetNavProp + " of entity " + sourceEntity + " not found.");
	}

	@Override
	public void createLink(OEntityId sourceEntity, String targetNavProp, OEntityId targetEntity) {
		updateLink(sourceEntity, targetNavProp, null, targetEntity);
	}

	@Override
	public void updateLink(OEntityId sourceEntity, String targetNavProp, OEntityKey oldTargetEntityKey, OEntityId newTargetEntity) {
		Entity entity = findEntity(sourceEntity.getEntitySetName(), sourceEntity.getEntityKey());
		EdmEntitySet ees = metadata.getEdmEntitySet(sourceEntity.getEntitySetName());
		String targetEntityKind = ees.getType().findNavigationProperty(targetNavProp).getToRole().getRole();

		Entity newEntity = null;
		if (newTargetEntity != null) {
			newEntity = findEntity(newTargetEntity.getEntitySetName(), newTargetEntity.getEntityKey());
			if (!newEntity.getKind().equals(targetEntityKind)) {
				throw new NotImplementedException("EdmNavigationProperty " + targetNavProp + " is not of expected kind. Expecting " + targetEntityKind + ", got " + newEntity.getKind() + ".");
			}
		}

		EdmMultiplicity multiplicity = ees.getType().findNavigationProperty(targetNavProp).getToRole().getMultiplicity();
		if (multiplicity == EdmMultiplicity.ZERO_TO_ONE) {
			entity.setProperty(targetNavProp, newEntity != null ? newEntity.getKey() : null);
		} else {
			@SuppressWarnings("unchecked")
			Collection<Key> keys = (Collection<Key>) entity.getProperty(targetNavProp);
			if (keys == null) {
				keys = new ArrayList<Key>();
			}
			if (oldTargetEntityKey != null) {
				long id = Long.parseLong(oldTargetEntityKey.asSingleValue().toString());
				Key oldTargetKey = KeyFactory.createKey(targetEntityKind, id);
				keys.remove(oldTargetKey);
			}
			if (newTargetEntity != null) {
				keys.add(newEntity.getKey());
			}
			entity.setProperty(targetNavProp, keys);
		}
		datastore.put(entity);
	}

	@Override
	public void deleteLink(OEntityId sourceEntity, String targetNavProp, OEntityKey targetEntityKey) {
		updateLink(sourceEntity, targetNavProp, targetEntityKey, null);
	}

	@Override
	public BaseResponse callFunction(EdmFunctionImport name, Map<String, OFunctionParameter> params, QueryInfo queryInfo) {
		throw new NotImplementedException();
	}

	@Override
	public CountResponse getEntitiesCount(String arg0, QueryInfo arg1) {
		throw new NotImplementedException();
	}

	@Override
	public CountResponse getNavPropertyCount(String arg0, OEntityKey arg1, String arg2, QueryInfo arg3) {
		throw new NotImplementedException();
	}

	@Override
	public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> arg0) {
		return null;
	}

	private void createAccess(Entity e) {
		UserService userService = UserServiceFactory.getUserService();
		Entity ac = new Entity(ACCESS_CONTROL_KIND);
		ac.setProperty("Entity", e);
		ac.setProperty("Owner", userService.getCurrentUser());
	}

	private void checkAccess(Entity e) {
		UserService userService = UserServiceFactory.getUserService();
		if (!userService.isUserLoggedIn()) {
			throw new NotAuthorizedException();
		}
		if (e == null) {
			return;
		}
		if (userService.isUserAdmin()) {
			return;
		}

		Query q = new Query(ACCESS_CONTROL_KIND);
		Filter filter = new FilterPredicate("Entity", FilterOperator.EQUAL, e);
		q.setFilter(filter);
		Entity ac = datastore.prepare(q).asSingleEntity();

		User owner = (User) ac.getProperty("owner");
		if (owner.compareTo(userService.getCurrentUser()) != 0) {
			throw new NotAuthorizedException();
		}
	}
}
