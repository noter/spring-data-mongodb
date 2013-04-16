package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class CustomMongoTemplate extends MongoTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomMongoTemplate.class);

    private static final String ID_FIELD = "_id";

    private final QueryMapper mapper;

    private final JacksonMappingMongoConverter jacksonMappingMongoConverter;

    private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

    private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();


    public CustomMongoTemplate(final MongoDbFactory mongoDbFactory, final JacksonMappingMongoConverter mongoConverter) {
        super(mongoDbFactory, mongoConverter);
        jacksonMappingMongoConverter = mongoConverter;
        mappingContext = jacksonMappingMongoConverter.getMappingContext();
        mapper = new QueryMapper(mongoConverter);
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass) {
        return findOne(query, entityClass, getCollectionName(entityClass));
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
        query.limit(1);
        List<T> list = executeFindMultiInternal(query, entityClass, collectionName);
        return list.size() > 0 ? list.get(0) : null;
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass) {
        return find(query, entityClass, getCollectionName(entityClass));
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {
        return executeFindMultiInternal(query, entityClass, collectionName);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass) {
        return findById(id, entityClass, getCollectionName(entityClass));
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
        MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
        MongoPersistentProperty idProperty = persistentEntity == null ? null : persistentEntity.getIdProperty();
        String idKey = idProperty == null ? ID_FIELD : idProperty.getName();
        return findOne(Query.query(Criteria.where(idKey).is(id)), entityClass, collectionName);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        return findAll(entityClass, getCollectionName(entityClass));
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
        return executeFindMultiInternal(Query.query(new Criteria()), entityClass, collectionName);
    }

    @Override
    public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass) {
        return null;
    }

    @Override
    public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass, String collectionName) {
        return null;
    }

    @Override
    public void remove(final Object object) {
        super.remove(object);
        maybeEmitDeleteEvent(object);
    }

    @Override
    public void remove(final Object object, final String collection) {
        super.remove(object, collection);
        maybeEmitDeleteEvent(object);
    }

    @Override
    protected void prepareCollection(final DBCollection collection) {
        super.prepareCollection(collection);
        collection.setDBDecoderFactory(jacksonMappingMongoConverter.getDecoderFactory());
        collection.setDBEncoderFactory(jacksonMappingMongoConverter.getEncoderFactory());
    }

    private DBCursor prepareDBCursor(Query query, DBCursor cursor) {


        if (query == null) {
            return cursor;
        }

        if (query.getSkip() <= 0 && query.getLimit() <= 0 && query.getSortObject() == null
                && !StringUtils.hasText(query.getHint())) {
            return cursor;
        }

        DBCursor cursorToUse = cursor;

        try {
            if (query.getSkip() > 0) {
                cursorToUse = cursorToUse.skip(query.getSkip());
            }
            if (query.getLimit() > 0) {
                cursorToUse = cursorToUse.limit(query.getLimit());
            }
            if (query.getSortObject() != null) {
                cursorToUse = cursorToUse.sort(query.getSortObject());
            }
            if (StringUtils.hasText(query.getHint())) {
                cursorToUse = cursorToUse.hint(query.getHint());
            }
        } catch (RuntimeException e) {
            throw potentiallyConvertRuntimeException(e);
        }

        return cursorToUse;

    }

    private RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
        RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
        return resolved == null ? ex : resolved;
    }

    private DBCollection getAndPrepareCollection(DB db, String collectionName, QueryContext queryContext) {
        try {
            DBCollection collection = db.getCollection(collectionName);
            super.prepareCollection(collection);
            collection.setDBEncoderFactory(jacksonMappingMongoConverter.getEncoderFactory());
            collection.setDBDecoderFactory(new JacksonDBDecoderFactory(jacksonMappingMongoConverter.getObjectMapper(), queryContext));
            return collection;
        } catch (RuntimeException e) {
            throw potentiallyConvertRuntimeException(e);
        }
    }

    private <T> List<T> executeFindMultiInternal(Query query, Class<T> clazz, String collectionName) {

        try {

            DBCursor cursor = null;


            try {

                DBObject dbQuery = mapper.getMappedObject(query.getQueryObject(), mappingContext.getPersistentEntity(clazz));

                QueryContext queryContext = QueryContext.resolveQueryContext(query, mappingContext, clazz);

                cursor = getAndPrepareCollection(getDb(), collectionName, queryContext).find(dbQuery, queryContext != null ? queryContext.fields("") : null);


                cursor = prepareDBCursor(query, cursor);


                List<T> result = new ArrayList<T>();

                while (cursor.hasNext()) {
                    DBObject object = cursor.next();
                    result.add(jacksonMappingMongoConverter.read(clazz, object));
                }

                return result;

            } finally {

                if (cursor != null) {
                    cursor.close();
                }
            }
        } catch (RuntimeException e) {
            throw potentiallyConvertRuntimeException(e);
        }
    }

    private <T> void maybeEmitDeleteEvent(final T entity) {
        maybeEmitEvent(new AfterDeleteEvent<T>(entity, null));
    }

}
