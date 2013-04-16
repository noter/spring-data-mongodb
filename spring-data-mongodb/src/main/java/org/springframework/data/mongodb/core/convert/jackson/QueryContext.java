package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;

public class QueryContext {

    private static CustomConversions customConversions = new CustomConversions(new ArrayList<Object>());

    private HashMap<String, DBObject> fields = new HashMap<String, DBObject>();

    private Stack<String> stack;

    public QueryContext(HashMap<String, DBObject> fields) {
        this.fields = fields;
        this.stack = new Stack<String>();
    }

    public static QueryContext resolveQueryContext(Query query, MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mongoMappingContext,
                                                   Class<?> clazz) {
        DBObject fields = query.getFieldsObject();
        if (fields.keySet().isEmpty()) {
            return null;
        }
        HashMap<String, DBObject> resolvedFields = new HashMap<String, DBObject>();
        resolveEntityFields("", "", fields, mongoMappingContext, clazz, resolvedFields);
        if (resolvedFields.keySet().isEmpty()) {
            return null;
        }
        return new QueryContext(resolvedFields);

    }

    private static void resolveEntityFields(final String path, final String globalContext, final DBObject fieldsToProcess,
                                            final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mongoMappingContext,
                                            Class<?> clazz, final HashMap<String, DBObject> fields) {
        final MongoPersistentEntity<?> mongoPersistentEntity = mongoMappingContext.getPersistentEntity(clazz);
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
                if (containsOrStartWith(globalFieldName, fieldsToProcess.keySet())) {
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

    private static boolean containsOrStartWith(String field, Set<String> fields) {
        if (fields.contains(field)) {
            return true;
        }
        for (String s : fields) {
            if (s.startsWith(field)) {
                return true;
            }
        }
        return false;
    }

    protected void popRootPath() {
        stack.pop();
    }

    protected DBObject fields(String rootPath) {

        stack.push(rootPath);

        String path = null;

        for (String s : stack) {
            if (path == null) {
                path = s;
            } else if (s.length() > 0) {
                path += path.length() > 0 ? ("." + s) : s;
            }
        }

        return fields.get(path);
    }

}
