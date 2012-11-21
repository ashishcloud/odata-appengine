package org.odata.appengine;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.odata4j.core.ODataConstants;
import org.odata4j.edm.EdmAssociation;
import org.odata4j.edm.EdmAssociationEnd;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmMultiplicity;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotImplementedException;

public class JPAMetadataBuilder implements MetadataBuilder {

	private static final String CONTAINER_NAME = "Container";
	private static final String CLASSES_PROPNAME = "org.odata.appengine.metadata.classes";

	public EdmDataServices buildMetadata(String namespace) {

		List<Class<?>> classes = getClasses(getClassNames());

		List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
		List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();

		// Build properties
		for (Class<?> classEntity : classes) {
			String keyProp = getIdField(classEntity).getName();
			List<EdmProperty.Builder> properties = buildClassProperties(classEntity);
			EdmEntityType.Builder eet = EdmEntityType.newBuilder().setNamespace(namespace).setName(classEntity.getSimpleName()).addKeys(keyProp).addProperties(properties);
			EdmEntitySet.Builder ees = EdmEntitySet.newBuilder().setName(classEntity.getSimpleName()).setEntityType(eet);
			entitySets.add(ees);
			entityTypes.add(eet);
		}

		// Build navigation properties
		for (Class<?> classEntity : classes) {
			List<EdmNavigationProperty.Builder> navigationProperties = buildClassNavigationProperties(entityTypes, namespace, classEntity);
			getEdmEntityType(entityTypes, classEntity.getSimpleName()).addNavigationProperties(navigationProperties);
		}

		EdmEntityContainer.Builder container = EdmEntityContainer.newBuilder().setName(CONTAINER_NAME).setIsDefault(true).addEntitySets(entitySets);
		EdmSchema.Builder schema = EdmSchema.newBuilder().setNamespace(namespace).addEntityTypes(entityTypes).addEntityContainers(container);
		EdmDataServices.Builder metadata = EdmDataServices.newBuilder().setVersion(ODataConstants.DATA_SERVICE_VERSION).addSchemas(schema);

		return metadata.build();
	}

	private List<String> getClassNames() {
		String prop = System.getProperty(CLASSES_PROPNAME);
		String[] classes = prop.split(",");
		return Arrays.asList(classes);
	}

	private List<EdmProperty.Builder> buildClassProperties(Class<?> classEntity) {
		List<EdmProperty.Builder> properties = new ArrayList<EdmProperty.Builder>();
		for (Field field : classEntity.getDeclaredFields()) {
			try {
				if (!isId(field)) {
					properties.add(getEdmPropertyBuilder(field));
				}
			} catch (Exception e) {

			}
		}
		return properties;
	}

	private List<EdmNavigationProperty.Builder> buildClassNavigationProperties(List<EdmEntityType.Builder> entityTypes, String namespace, Class<?> classEntity) {
		List<EdmNavigationProperty.Builder> navProperties = new ArrayList<EdmNavigationProperty.Builder>();
		for (Field field : classEntity.getDeclaredFields()) {
			try {
				if (!isId(field)) {
					navProperties.add(getEdmNavigationPropertyBuilder(entityTypes, namespace, field));
				}
			} catch (Exception e) {

			}
		}
		return navProperties;
	}

	private EdmProperty.Builder getEdmPropertyBuilder(Field field) {
		EdmType type = getPropertyType(field);
		return EdmProperty.newBuilder(field.getName()).setType(type);
	}

	private EdmNavigationProperty.Builder getEdmNavigationPropertyBuilder(List<EdmEntityType.Builder> entityTypes, String namespace, Field field) {
		String fromClass = field.getDeclaringClass().getSimpleName();
		String toClass;
		if (field.getGenericType() instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
			Class<?> relType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
			toClass = relType.getSimpleName();
		} else {
			toClass = field.getType().getSimpleName();
		}
		String assocName = fromClass + "." + field.getName();

		EdmEntityType.Builder fromEntityType = getEdmEntityType(entityTypes, fromClass);
		EdmEntityType.Builder toEntityType = getEdmEntityType(entityTypes, toClass);

		EdmMultiplicity fromAssociationMultiplicity = EdmMultiplicity.ONE;
		EdmMultiplicity toAssociationMultiplicity;
		if (Collection.class.isAssignableFrom(field.getType())) {
			toAssociationMultiplicity = EdmMultiplicity.MANY;
		} else {
			toAssociationMultiplicity = EdmMultiplicity.ONE;
		}

		EdmAssociationEnd.Builder fromAssociationEnd = EdmAssociationEnd.newBuilder().setRole(fromEntityType.getName()).setType(fromEntityType).setMultiplicity(fromAssociationMultiplicity);
		EdmAssociationEnd.Builder toAssociationEnd = EdmAssociationEnd.newBuilder().setRole(toEntityType.getName()).setType(toEntityType).setMultiplicity(toAssociationMultiplicity);
		EdmAssociation.Builder association = EdmAssociation.newBuilder().setNamespace(namespace).setName(assocName).setEnds(fromAssociationEnd, toAssociationEnd);

		return EdmNavigationProperty.newBuilder(field.getName()).setRelationship(association).setFromTo(fromAssociationEnd, toAssociationEnd);
	}

	private EdmEntityType.Builder getEdmEntityType(List<EdmEntityType.Builder> entityTypes, String name) {
		for (EdmEntityType.Builder builder : entityTypes) {
			if (builder.getName().equals(name)) {
				return builder;
			}
		}
		return null;
	}

	private EdmType getPropertyType(Field property) {
		if (property.getType() == String.class) {
			return EdmSimpleType.STRING;
		} else if (property.getType() == Integer.class) {
			return EdmSimpleType.INT32;
		} else if (property.getType() == Long.class) {
			return EdmSimpleType.INT64;
		} else if (property.getType() == Short.class) {
			return EdmSimpleType.INT16;
		} else if (property.getType() == Boolean.class) {
			return EdmSimpleType.BOOLEAN;
		} else if (property.getType() == Float.class) {
			return EdmSimpleType.SINGLE;
		} else if (property.getType() == Double.class) {
			return EdmSimpleType.DOUBLE;
		} else if (property.getType() == Date.class) {
			return EdmSimpleType.DATETIME;
		} else if (property.getType() == Byte.class) {
			return EdmSimpleType.BYTE;
		} else {
			throw new NotImplementedException(property.getType().getName());
		}
	}

	private List<Class<?>> getClasses(List<String> classes) {
		List<Class<?>> classesList = new ArrayList<Class<?>>();
		for (String classEntity : classes) {
			try {
				classesList.add((Class.forName(classEntity)));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return classesList;
	}

	private Field getIdField(Class<?> cls) {
		for (Field field : cls.getDeclaredFields()) {
			if (isId(field)) {
				return field;
			}
		}
		return null;
	}

	private Boolean isId(Field field) {
		return field.isAnnotationPresent(javax.persistence.Id.class);
	}
}
