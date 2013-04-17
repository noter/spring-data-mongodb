package org.springframework.data.mongodb.core.convert.custom;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeserializationContext {


	private static CustomConversions customConversions = new CustomConversions(new ArrayList<Object>());
	private HashMap<CacheKey, Object> cache = new HashMap<CacheKey, Object>();
	private HashMap<String, DBObject> fields = new HashMap<String, DBObject>();
	private Stack<String> stack;
	private Stack<DBRef> dbRefs;

	public DeserializationContext(HashMap<String, DBObject> fields) {
		this.fields = fields;
		this.stack = new Stack<String>();
		this.dbRefs = new Stack<DBRef>();
	}

	public static DeserializationContext resolveDeserializationContext(Query query, MappingContext<? extends MongoPersistentEntity<?>,
			MongoPersistentProperty> mongoMappingContext, Class<?> clazz) {
		HashMap<String, DBObject> resolvedFields = new HashMap<String, DBObject>();
		if (query != null) {
			DBObject fields = query.getFieldsObject();

			if (fields != null && !fields.keySet().isEmpty()) {
				resolveEntityFields("", "", fields, mongoMappingContext, clazz, resolvedFields);
			}
		}
		return new DeserializationContext(resolvedFields);

	}

	private static void resolveEntityFields(final String path, final String globalContext, final DBObject fieldsToProcess,
											final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mongoMappingContext,
											Class<?> clazz, final HashMap<String, DBObject> fields) {
		final MongoPersistentEntity<?> mongoPersistentEntity = mongoMappingContext.getPersistentEntity(clazz);
		if (mongoPersistentEntity != null) {
			final DBObject entityFields = new BasicDBObject();
			mongoPersistentEntity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

				public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {
					String globalFieldName = (globalContext.length() > 0 ? globalContext + "." : "") + path + persistentProperty.getFieldName();
					Class<?> propertyType = persistentProperty.isCollectionLike() ? persistentProperty.getComponentType() : (persistentProperty
							.isMap() ? persistentProperty.getMapValueType() : persistentProperty.getRawType());
					if (fieldsToProcess.keySet().contains(globalFieldName)) {
						entityFields.put(path + persistentProperty.getFieldName(), fieldsToProcess.get(globalFieldName));
					}
					if (!customConversions.isSimpleType(propertyType)) {
						resolveEntityFields(persistentProperty.getFieldName() + ".", globalContext, fieldsToProcess, mongoMappingContext, propertyType, fields);
					}
				}
			});
			mongoPersistentEntity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {

				public void doWithAssociation(Association<MongoPersistentProperty> association) {
					MongoPersistentProperty persistentProperty = association.getInverse();
					Class<?> propertyType = persistentProperty.isCollectionLike() ? persistentProperty.getComponentType() : (persistentProperty
							.isMap() ? persistentProperty.getMapValueType() : persistentProperty.getRawType());
					String globalFieldName = (globalContext.length() > 0 ? globalContext + "." : "") + path + persistentProperty.getFieldName();
					String fieldName = path + persistentProperty.getFieldName();
					if (persistentProperty.isMap()) {
						for (String s : findMapFields(fieldName, fieldsToProcess.keySet())) {
							entityFields.put(s, 1);
						}
						for (String s : findMapFilteringKeys(fieldName, fieldsToProcess.keySet())) {
							resolveEntityFields("", s, fieldsToProcess, mongoMappingContext, propertyType, fields);
						}
					} else if (containsOrStartWith(globalFieldName, fieldsToProcess.keySet())) {
						entityFields.put(fieldName, 1);
						resolveEntityFields("", fieldName, fieldsToProcess, mongoMappingContext, propertyType, fields);
					}
				}
			});
			if (!entityFields.keySet().isEmpty()) {
				if (!fields.containsKey(globalContext)) {
					fields.put(globalContext, new BasicDBObject());
				}
				fields.get(globalContext).putAll(entityFields);
			}
		}
	}

	private static boolean containsOrStartWith(String field, Set<String> fields) {
		if (fields.contains(field)) {
			return true;
		}
		for (String s : fields) {
			if (s.startsWith(field + ".")) {
				return true;
			}
		}
		return false;
	}

	private static Set<String> findMapFields(String field, Set<String> fields) {
		Set<String> toReturn = new HashSet<String>();
		boolean contains = false;
		for (String s : fields) {
			if (s.equals(field) || s.equals(field + ".*")) {
				toReturn.add(field);
			} else if (s.matches(field + "\\.[^\\.]+")) {
				toReturn.add(s);
			} else if (s.startsWith(field + ".")) {
				contains = true;
			}
		}
		if (toReturn.isEmpty() && contains) {
			toReturn.add(field);
		}
		return toReturn;
	}

	private static Set<String> findMapFilteringKeys(String field, Set<String> fields) {
		Set<String> toReturn = new HashSet<String>();
		for (String s : fields) {
			if (s.equals(field) || s.equals(field + ".*")) {
				toReturn.add(field);
			} else {
				Matcher matcher = Pattern.compile("(" + field + "\\.[^\\.]+).*").matcher(s);
				if (matcher.matches()) {
					toReturn.add(matcher.group(1));
				}
			}
		}
		return toReturn;
	}

	public void pushDBRef(DBRef dbRef){
		dbRefs.push(dbRef);
	}

	public DBRef popDBRef(){
		if(!dbRefs.empty()){
			return dbRefs.pop();
		}
		return  null;
	}

	public boolean push(MongoPersistentProperty persistentProperty) {
		Class<?> propertyType = persistentProperty.isCollectionLike() ? persistentProperty.getComponentType() : (persistentProperty
				.isMap() ? persistentProperty.getMapValueType() : persistentProperty.getRawType());
		if (persistentProperty.isAssociation() || !customConversions.isSimpleType(propertyType)) {
			stack.push(persistentProperty.getFieldName());
			return true;
		}
		return false;
	}

	public void push(String value) {
		stack.push(value);
	}

	public void putToCache(DBRef dbRef, Object value,DBObject fields) {
		cache.put(new CacheKey(value.getClass(), dbRef.getId(),dbRef.getDB().getName(), fields), value);
	}

	public boolean isCached(Class<?> clazz, DBRef dbRef, DBObject fields) {
		return cache.containsKey(new CacheKey(clazz, dbRef.getId(),dbRef.getDB().getName(), fields));
	}

	public Object getCached(Class<?> clazz, DBRef dbRef, DBObject fields) {
		return cache.get(new CacheKey(clazz,dbRef.getId(),dbRef.getDB().getName(), fields));
	}

	public void pop() {
		stack.pop();
	}

	public DBObject fields() {
		String path = null;

		String prev = "";

		for (String s : stack) {
			if (path == null) {
				path = s;
			} else if (s.length() > 0) {
				prev = path;
				path += path.length() > 0 ? ("." + s) : s;
			}
		}
		DBObject toReturn = new BasicDBObject();
		DBObject main = fields.get(path);
		if (main != null) {
			toReturn.putAll(main);
		}
		DBObject wildcard = fields.get(prev + ".*");
		if (wildcard != null) {
			toReturn.putAll(wildcard);
		}
		return toReturn.keySet().isEmpty() ? null : toReturn;
	}

	public static class CacheKey {

		private Class<?> clazz;
		private DBObject fields;
		private Object id;
		private String db;

		public CacheKey(Class<?> clazz, Object id, String db, DBObject fields) {
			this.clazz = clazz;
			this.fields = fields;
			this.id = id;
			this.db = db;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			CacheKey cacheKey = (CacheKey) o;

			if (!clazz.equals(cacheKey.clazz)) return false;
			if (!db.equals(cacheKey.db)) return false;
			if (fields != null ? !fields.equals(cacheKey.fields) : cacheKey.fields != null) return false;
			if (!id.equals(cacheKey.id)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = clazz.hashCode();
			result = 31 * result + (fields != null ? fields.hashCode() : 0);
			result = 31 * result + id.hashCode();
			result = 31 * result + db.hashCode();
			return result;
		}
	}

}


