package org.springframework.data.mongodb.core.convert.custom;

import com.mongodb.*;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.*;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import java.util.*;

public class ContextualMappingMongoConverter extends MappingMongoConverter {


	private SpELContext spELContext;

	public ContextualMappingMongoConverter(MongoDbFactory mongoDbFactory, MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		super(mongoDbFactory, mappingContext);

	}

	public <S> S read(Class<S> clazz, DBObject dbo, DeserializationContext context) {
		return read(ClassTypeInformation.from(clazz), dbo, context);
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	protected <S extends Object> S read(TypeInformation<S> type, DBObject dbo, DeserializationContext context) {
		return read(type, dbo, null, context);
	}

	@SuppressWarnings("unchecked")
	protected <S extends Object> S read(TypeInformation<S> type, DBObject dbo, Object parent, DeserializationContext context) {

		if (null == dbo) {
			return null;
		}

		TypeInformation<? extends S> typeToUse = typeMapper.readType(dbo, type);
		Class<? extends S> rawType = typeToUse.getType();

		if (conversions.hasCustomReadTarget(dbo.getClass(), rawType)) {
			return conversionService.convert(dbo, rawType);
		}

		if (DBObject.class.isAssignableFrom(rawType)) {
			return (S) dbo;
		}

		if (typeToUse.isCollectionLike() && dbo instanceof BasicDBList) {
			return (S) readCollectionOrArray(typeToUse, (BasicDBList) dbo, parent, context);
		}

		if (typeToUse.isMap()) {
			return (S) readMap(typeToUse, dbo, parent, context);
		}

		// Retrieve persistent entity info
		MongoPersistentEntity<S> persistentEntity = (MongoPersistentEntity<S>) mappingContext
				.getPersistentEntity(typeToUse);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return read(persistentEntity, dbo, parent, context);
	}

	@SuppressWarnings("unchecked")
	protected Map<Object, Object> readMap(TypeInformation<?> type, DBObject dbObject, Object parent, DeserializationContext context) {

		Assert.notNull(dbObject);

		Class<?> mapType = typeMapper.readType(dbObject, type).getType();
		Map<Object, Object> map = CollectionFactory.createMap(mapType, dbObject.keySet().size());
		Map<String, Object> sourceMap = dbObject.toMap();

		for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
			if (typeMapper.isTypeKey(entry.getKey())) {
				continue;
			}

			Object key = potentiallyUnescapeMapKey(entry.getKey());

			TypeInformation<?> keyTypeInformation = type.getComponentType();
			if (keyTypeInformation != null) {
				Class<?> keyType = keyTypeInformation.getType();
				key = conversionService.convert(key, keyType);
			}

			context.push(key.toString());

			Object value = entry.getValue();
			TypeInformation<?> valueType = type.getMapValueType();
			Class<?> rawValueType = valueType == null ? null : valueType.getType();

			if (value instanceof DBObject) {
				map.put(key, read(valueType, (DBObject) value, parent, context));
			} else if (value instanceof DBRef) {
				map.put(key, DBRef.class.equals(rawValueType) ? value : read(valueType, ((DBRef) value), null, context));
			} else {
				Class<?> valueClass = valueType == null ? null : valueType.getType();
				map.put(key, getPotentiallyConvertedSimpleRead(value, valueClass));
			}

			context.pop();
		}

		return map;
	}

	@Override
	protected DBObject writeMapInternal(Map<Object, Object> obj, DBObject dbo, TypeInformation<?> propertyType) {
		return super.writeMapInternal(obj, dbo, propertyType);
	}

	@SuppressWarnings("unchecked")
	protected <S extends Object> S read(TypeInformation<S> type, DBRef dbRef, Object parent, DeserializationContext context) {
		DBObject fields = context.fields();
		if (context.isCached(type.getType(), dbRef.getId(), fields)) {
			return (S) context.getCached(type.getType(), dbRef.getId(), fields);
		}

		DB db = dbRef.getDB();
		DBCollection collection = db.getCollection(dbRef.getRef());
		DBObject one = collection.findOne(dbRef.getId(), fields);
		return read(type, one, parent, context);
	}

	protected Object getValueInternal(MongoPersistentProperty prop, DBObject dbo, SpELExpressionEvaluator eval,
									  Object parent, DeserializationContext context) {

		MongoDbPropertyValueProvider provider = new MongoDbPropertyValueProvider(dbo, spELContext, parent, context);
		return provider.getPropertyValue(prop);
	}

	private Object getPotentiallyConvertedSimpleRead(Object value, Class<?> target) {

		if (value == null || target == null) {
			return value;
		}

		if (conversions.hasCustomReadTarget(value.getClass(), target)) {
			return conversionService.convert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return target.isAssignableFrom(value.getClass()) ? value : conversionService.convert(value, target);
	}

	@SuppressWarnings("unchecked")
	private Object readCollectionOrArray(TypeInformation<?> targetType, BasicDBList sourceValue, Object parent, DeserializationContext context) {

		Assert.notNull(targetType);

		Class<?> collectionType = targetType.getType();

		if (sourceValue.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(new HashSet<Object>(), collectionType);
		}

		collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;

		Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>() : CollectionFactory
				.createCollection(collectionType, sourceValue.size());
		TypeInformation<?> componentType = targetType.getComponentType();
		Class<?> rawComponentType = componentType == null ? null : componentType.getType();

		for (int i = 0; i < sourceValue.size(); i++) {

			Object dbObjItem = sourceValue.get(i);

			if (dbObjItem instanceof DBRef) {
				items.add(DBRef.class.equals(rawComponentType) ? dbObjItem : read(componentType, ((DBRef) dbObjItem),
						parent, context));
			} else if (dbObjItem instanceof DBObject) {
				items.add(read(componentType, (DBObject) dbObjItem, parent));
			} else {
				items.add(getPotentiallyConvertedSimpleRead(dbObjItem, rawComponentType));
			}
		}

		return getPotentiallyConvertedSimpleRead(items, targetType.getType());
	}

	@SuppressWarnings("unchecked")
	private <T> T readValue(Object value, TypeInformation<?> type, Object parent, DeserializationContext context) {

		Class<?> rawType = type.getType();

		if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
			return (T) conversionService.convert(value, rawType);
		} else if (value instanceof DBRef) {
			return (T) (rawType.equals(DBRef.class) ? value : read(type, ((DBRef) value), parent, context));
		} else if (value instanceof BasicDBList) {
			return (T) readCollectionOrArray(type, (BasicDBList) value, parent, context);
		} else if (value instanceof DBObject) {
			return (T) read(type, (DBObject) value, parent, context);
		} else {
			return (T) getPotentiallyConvertedSimpleRead(value, rawType);
		}
	}

	private <S extends Object> S read(final MongoPersistentEntity<S> entity, final DBObject dbo, Object parent, final DeserializationContext context) {

		final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(dbo, spELContext);

		ParameterValueProvider<MongoPersistentProperty> provider = getParameterProvider(entity, dbo, evaluator, parent, context);
		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		final BeanWrapper<MongoPersistentEntity<S>, S> wrapper = BeanWrapper.create(instance, conversionService);
		final S result = wrapper.getBean();
		if (entity.getIdProperty() != null) {
			Object id = dbo.get(entity.getIdProperty().getFieldName());
			context.putToCache(id, context.fields(), result);
		}

		// Set properties not already set in the constructor
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
			public void doWithPersistentProperty(MongoPersistentProperty prop) {

				boolean isConstructorProperty = entity.isConstructorArgument(prop);
				boolean hasValueForProperty = dbo.containsField(prop.getFieldName());

				if (!hasValueForProperty || isConstructorProperty) {
					return;
				}

				Object obj = getValueInternal(prop, dbo, evaluator, result, context);
				wrapper.setProperty(prop, obj, useFieldAccessOnly);
			}
		});

		// Handle associations
		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {
			public void doWithAssociation(Association<MongoPersistentProperty> association) {
				MongoPersistentProperty inverseProp = association.getInverse();
				Object obj = getValueInternal(inverseProp, dbo, evaluator, result, context);

				wrapper.setProperty(inverseProp, obj);

			}
		});

		return result;
	}

	private ParameterValueProvider<MongoPersistentProperty> getParameterProvider(MongoPersistentEntity<?> entity,
																				 DBObject source, DefaultSpELExpressionEvaluator evaluator, Object parent,
																				 DeserializationContext context) {

		MongoDbPropertyValueProvider provider = new MongoDbPropertyValueProvider(source, evaluator, parent, context);
		PersistentEntityParameterValueProvider<MongoPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<MongoPersistentProperty>(
				entity, provider, parent);

		return new ConverterAwareSpELExpressionParameterValueProvider(evaluator, conversionService, parameterProvider,
				parent, context);
	}

	private class MongoDbPropertyValueProvider implements PropertyValueProvider<MongoPersistentProperty> {

		private final DBObject source;
		private final SpELExpressionEvaluator evaluator;
		private final Object parent;
		private DeserializationContext context;

		public MongoDbPropertyValueProvider(DBObject source, SpELContext factory, Object parent, DeserializationContext context) {
			this(source, new DefaultSpELExpressionEvaluator(source, factory), parent, context);
		}

		public MongoDbPropertyValueProvider(DBObject source, DefaultSpELExpressionEvaluator evaluator, Object parent, DeserializationContext context) {

			Assert.notNull(source);
			Assert.notNull(evaluator);

			this.source = source;
			this.evaluator = evaluator;
			this.parent = parent;
			this.context = context;

		}

		public <T> T getPropertyValue(MongoPersistentProperty property) {

			String expression = property.getSpelExpression();
			Object value = expression != null ? evaluator.evaluate(expression) : source.get(property.getFieldName());

			if (value == null) {
				return null;
			}

			boolean pushed = context.push(property);
			T t = readValue(value, property.getTypeInformation(), parent, context);
			if (pushed) {
				context.pop();
			}
			return t;
		}
	}

	private class ConverterAwareSpELExpressionParameterValueProvider extends
			SpELExpressionParameterValueProvider<MongoPersistentProperty> {

		private final Object parent;
		private DeserializationContext context;


		public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
																  ConversionService conversionService, ParameterValueProvider<MongoPersistentProperty>
				delegate, Object parent, DeserializationContext context) {

			super(evaluator, conversionService, delegate);
			this.parent = parent;
			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, PreferredConstructor.Parameter<T, MongoPersistentProperty> parameter) {
			return readValue(object, parameter.getType(), parent, context);
		}
	}

}
