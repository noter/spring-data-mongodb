package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.impl.PropertyBasedObjectIdGenerator;
import com.fasterxml.jackson.databind.deser.impl.ReadableObjectId;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.common.base.Optional;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import de.undercouch.bson4jackson.types.ObjectId;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;


import java.io.IOException;

public class DBRefDeserializer extends JsonDeserializer<Object> {

    private final MongoDbFactory mongoDbFactory;

    private final MongoPersistentProperty mongoPersistentProperty;

    private final PropertyBasedObjectIdGenerator idGenerator = new PropertyBasedObjectIdGenerator(null);

    private MongoObjectMapper mongoObjectMapper;

    public DBRefDeserializer(final MongoDbFactory mongoDbFactory, final MongoObjectMapper mongoObjectMapper,
                             final MongoPersistentProperty mongoPersistentProperty) {
        this.mongoDbFactory = mongoDbFactory;
        this.mongoObjectMapper = mongoObjectMapper;
        this.mongoPersistentProperty = mongoPersistentProperty;
    }

    @Override
    public Object deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (!(jp instanceof MongoBsonParser)) {
            throw new IllegalStateException("Only MongoBsonParser is suported by this DBRefDeserializer");
        }
        if (!(ctxt instanceof MongoDeserializationContext)) {
            throw new IllegalStateException("Only MongoDeserializationContext is suported by this DBRefDeserializer");
        }

        MongoBsonParser mongoBsonParser = (MongoBsonParser) jp;

        MongoBsonParser.Context currentContext = mongoBsonParser.getCurrentContext().getParent();
        String path = currentContext.getFieldName();
        currentContext = currentContext.getParent();

        while (currentContext != null) {
            path = currentContext.getFieldName() + "." + path;
            currentContext = currentContext.getParent();
        }

        if (jp.nextToken() == JsonToken.FIELD_NAME) {
            String collection = null;
            String db = null;
            Object id = null;
            while (jp.getCurrentToken() != JsonToken.END_OBJECT) {
                if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                    if (jp.getCurrentName().equals("$ref")) {
                        collection = jp.nextTextValue();
                    } else if (jp.getCurrentName().equals("$id")) {
                        if (jp.nextToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
                            ObjectId objectId = (ObjectId) jp.getEmbeddedObject();
                            id = new org.bson.types.ObjectId(objectId.getTime(), objectId.getMachine(), objectId.getInc());
                        } else {
                            id = jp.getText();
                        }
                    } else if (jp.getCurrentName().equals("$db")) {
                        db = jp.nextTextValue();
                    }
                }
                jp.nextToken();
            }

            QueryContext queryContext = ((MongoDeserializationContext) ctxt).getQueryContext();

            DBObject fields = null;
            if (queryContext != null) {
                fields = queryContext.fields(path);
            }

            IdWithContext idWithContext = new IdWithContext(id, fields);

            ReadableObjectId readableObjectId = ctxt.findObjectId(idWithContext, idGenerator);

            Object toReturn = null;

            if (readableObjectId.item == null) {

                DB mongoDb;
                if (db == null) {
                    mongoDb = mongoDbFactory.getDb();
                } else {
                    mongoDb = mongoDbFactory.getDb(db);
                }
                DBCollection dbCollection = mongoDb.getCollection(collection);
                dbCollection.setDBDecoderFactory(new JacksonDBDecoderFactory(mongoObjectMapper, queryContext));
                DBObject one = dbCollection.findOne(id, fields);
                if ((one != null) && one.containsField(JacksonMappingMongoConverter.OBJECT_KEY)) {
                    Object object = one.get(JacksonMappingMongoConverter.OBJECT_KEY);
                    readableObjectId.bindItem(Optional.fromNullable(object));
                    toReturn = object;
                }
                readableObjectId.bindItem(Optional.absent());
            } else {
                Optional optional = (Optional) readableObjectId.item;
                if (optional.isPresent()) {
                    toReturn = optional.get();
                }
            }
            if (queryContext != null) {
                queryContext.popRootPath();
            }
            return toReturn;
        }

        throw new IllegalStateException("DBRef should containt field_name but found :" + jp.getCurrentToken());
    }

    @Override
    public Object deserializeWithType(final JsonParser jp, final DeserializationContext ctxt, final TypeDeserializer typeDeserializer)
            throws IOException, JsonProcessingException {
        return deserialize(jp, ctxt);
    }

    public static class IdWithContext {

        private DBObject context;

        private Object id;

        public IdWithContext(Object id, DBObject context) {
            this.id = id;
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IdWithContext that = (IdWithContext) o;

            if (context != null ? !context.equals(that.context) : that.context != null) {
                return false;
            }
            if (!id.equals(that.id)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + (context != null ? context.hashCode() : 0);
            return result;
        }
    }

}
