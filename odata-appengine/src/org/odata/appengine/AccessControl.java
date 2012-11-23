package org.odata.appengine;

import org.odata4j.exceptions.NotAuthorizedException;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;

public class AccessControl {

	private static final String ENTITY_ACCESS_CONTROL_KIND = "AccessControlEntity";
	private static final String KIND_ACCESS_CONTROL_KIND = "AccessControlKind";

	public enum AccessType {
		READ(1), WRITE(2);

		private final Integer accessTypeId;

		private AccessType(Integer id) {
			this.accessTypeId = id;
		}
	}

	public enum UserGroup {
		ADMIN(1), USER(2);

		private final Integer groupId;

		private UserGroup(Integer id) {
			this.groupId = id;
		}
	}

	public static void createAccess(DatastoreService datastore, String kind, UserGroup userGroup) {
		Entity kac = new Entity(ENTITY_ACCESS_CONTROL_KIND);
		kac.setProperty("Kind", kind);
		kac.setProperty("Access", userGroup.groupId);
	}

	public static void createAccess(DatastoreService datastore, Entity e) {
		UserService userService = UserServiceFactory.getUserService();
		Entity eac = new Entity(ENTITY_ACCESS_CONTROL_KIND);
		eac.setProperty("Entity", e);
		eac.setProperty("Owner", userService.getCurrentUser());
	}

	public static void checkAccess(DatastoreService datastore, Entity e, AccessType accessType) {
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

		// Check read access by kind
		if (accessType == AccessType.READ) {
			Query q = new Query(KIND_ACCESS_CONTROL_KIND);
			Filter filter = new FilterPredicate("Kind", FilterOperator.EQUAL, e.getKind());
			q.setFilter(filter);
			Entity kac = datastore.prepare(q).asSingleEntity();
			if (kac != null) {
				Long access = (Long) kac.getProperty("Access");
				if (access.equals(UserGroup.USER.groupId)) {
					return;
				}
			}
		}
		// Check write access by entity
		else if (accessType == AccessType.WRITE) {
			Query q = new Query(ENTITY_ACCESS_CONTROL_KIND);
			Filter filter = new FilterPredicate("Entity", FilterOperator.EQUAL, e);
			q.setFilter(filter);
			Entity eac = datastore.prepare(q).asSingleEntity();

			User owner = (User) eac.getProperty("owner");
			if (owner.compareTo(userService.getCurrentUser()) == 0) {
				return;
			}
		}
		throw new NotAuthorizedException();
	}
}
