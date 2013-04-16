package org.springframework.data.mongodb.core.convert.custom;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ContextualMongoTemplate extends MongoTemplate {

	private static final Logger LOGGER = LoggerFactory.getLogger(ContextualMongoTemplate.class);
	private static final String ID_FIELD = "_id";
	private final ContextualMappingMongoConverter mongoConverter;
	private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private QueryMapper queryMapper;

	public ContextualMongoTemplate(MongoDbFactory mongoDbFactory, ContextualMappingMongoConverter mongoConverter) {
		super(mongoDbFactory, mongoConverter);
		this.mongoConverter = mongoConverter;
		queryMapper = new QueryMapper(getConverter());
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

	private <T> void maybeEmitDeleteEvent(final T entity) {
		if (entity != null) {
			maybeEmitEvent(new AfterDeleteEvent<T>(entity, null));
		}
	}

	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, getCollectionName(entityClass));
	}

	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(Query.query(new Criteria()), entityClass, collectionName);
	}

	public <T> T findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, getCollectionName(entityClass));
	}

	public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
		query.limit(1);
		List<T> list = executeFindMultiInternal(query, entityClass, collectionName);
		return !list.isEmpty() ? list.get(0) : null;
	}

	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, getCollectionName(entityClass));
	}

	public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {
		if (query == null) {
			return findAll(entityClass, collectionName);
		}
		return executeFindMultiInternal(query, entityClass, collectionName);
	}

	public <T> T findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, getCollectionName(entityClass));
	}

	public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
		MongoPersistentEntity<T> mongoPersistentEntity = (MongoPersistentEntity<T>) getConverter().getMappingContext().getPersistentEntity(entityClass);
		MongoPersistentProperty idProperty = mongoPersistentEntity != null ? mongoPersistentEntity.getIdProperty() : null;
		String idField = idProperty != null ? idProperty.getName() : ID_FIELD;
		Query query = Query.query(Criteria.where(idField).is(id));
		List<T> list = executeFindMultiInternal(query, entityClass, collectionName);
		return !list.isEmpty() ? list.get(0) : null;
	}

	private <T> List<T> executeFindMultiInternal(Query query, Class<T> type, String collectionName) {

		try {

			DBCursor cursor = null;

			try {

				MongoPersistentEntity<?> mongoPersistentEntity = getConverter().getMappingContext().getPersistentEntity(type);

				DBObject queryDBDbObject = queryMapper.getMappedObject(query.getQueryObject(), mongoPersistentEntity);

				DeserializationContext context = DeserializationContext.resolveDeserializationContext(query, getConverter().getMappingContext(), type);


				context.push("");


				cursor = getAndPrepareCollection(getDb(), collectionName).find(queryDBDbObject, context.fields());

				prepare(cursor, query);

				List<T> result = new ArrayList<T>();


				while (cursor.hasNext()) {
					DBObject object = cursor.next();
					result.add(mongoConverter.read(type, object, context));
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

	public DBCursor prepare(DBCursor cursor, Query query) {

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

	private DBCollection getAndPrepareCollection(DB db, String collectionName) {
		try {
			DBCollection collection = db.getCollection(collectionName);
			prepareCollection(collection);
			return collection;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	private RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}
}
