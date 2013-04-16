package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.DeserializerFactoryConfig;
import com.fasterxml.jackson.databind.deser.*;
import com.fasterxml.jackson.databind.deser.impl.FieldProperty;
import com.fasterxml.jackson.databind.deser.impl.MethodProperty;
import com.fasterxml.jackson.databind.deser.std.CollectionDeserializer;
import com.fasterxml.jackson.databind.deser.std.MapDeserializer;
import com.fasterxml.jackson.databind.deser.std.ObjectArrayDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdValueInstantiator;
import com.fasterxml.jackson.databind.introspect.*;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MongoBeanDeserializerFactory extends BeanDeserializerFactory {

    @SuppressWarnings("rawtypes")
    final static HashMap<String, Class<? extends Collection>> _collectionFallbacks =
            new HashMap<String, Class<? extends Collection>>();

    static {
        _collectionFallbacks.put(Collection.class.getName(), ArrayList.class);
        _collectionFallbacks.put(List.class.getName(), ArrayList.class);
        _collectionFallbacks.put(Set.class.getName(), HashSet.class);
        _collectionFallbacks.put(SortedSet.class.getName(), TreeSet.class);
        _collectionFallbacks.put(Queue.class.getName(), LinkedList.class);

        _collectionFallbacks.put("java.util.Deque", LinkedList.class);
        _collectionFallbacks.put("java.util.NavigableSet", TreeSet.class);
    }

    @SuppressWarnings("rawtypes")
    final static HashMap<String, Class<? extends Map>> _mapFallbacks =
            new HashMap<String, Class<? extends Map>>();

    static {
        _mapFallbacks.put(Map.class.getName(), LinkedHashMap.class);
        _mapFallbacks.put(ConcurrentMap.class.getName(), ConcurrentHashMap.class);
        _mapFallbacks.put(SortedMap.class.getName(), TreeMap.class);

        _mapFallbacks.put("java.util.NavigableMap", TreeMap.class);
        try {
            Class<?> key = Class.forName("java.util.concurrent.ConcurrentNavigableMap");
            Class<?> value = Class.forName("java.util.concurrent.ConcurrentSkipListMap");
            @SuppressWarnings("unchecked")
            Class<? extends Map<?, ?>> mapValue = (Class<? extends Map<?, ?>>) value;
            _mapFallbacks.put(key.getName(), mapValue);
        } catch (ClassNotFoundException cnfe) {
        } catch (SecurityException se) {
        }
    }

    private static final long serialVersionUID = 1L;

    private final MongoMappingContext mongoMappingContext;

    private final boolean useFieldAccessOnly;

    private final ConversionService conversionService;

    private MongoDbFactory mongoDbFactory;

    private MongoObjectMapper mongoObjectMapper;


    public MongoBeanDeserializerFactory(final DeserializerFactoryConfig config, final MongoMappingContext mongoMappingContext,
                                        final ConversionService conversionService, MongoDbFactory mongoDbFactory, final MongoObjectMapper mongoObjectMapper,
                                        final boolean useFieldAccessOnly) {
        super(config);
        this.mongoMappingContext = mongoMappingContext;
        this.conversionService = conversionService;
        this.mongoDbFactory = mongoDbFactory;
        this.mongoObjectMapper = mongoObjectMapper;
        this.useFieldAccessOnly = useFieldAccessOnly;
    }

    public MongoBeanDeserializerFactory(final MongoMappingContext mongoMappingContext, final ConversionService conversionService, MongoDbFactory mongoDbFactory,
                                        final MongoObjectMapper mongoObjectMapper, final boolean useFieldAccessOnly) {
        this(new DeserializerFactoryConfig(), mongoMappingContext, conversionService, mongoDbFactory, mongoObjectMapper, useFieldAccessOnly);
    }

    @SuppressWarnings("unchecked")
    @Override
    public JsonDeserializer<Object> buildBeanDeserializer(final DeserializationContext context, final JavaType type, final BeanDescription beanDesc)
            throws JsonMappingException {
        if (isMongoEntity(type.getRawClass())) {
            final BeanDeserializerBuilder builder = constructBeanDeserializerBuilder(context, beanDesc);
            MongoPersistentEntity<?> entity = mongoMappingContext.getPersistentEntity(type.getRawClass());

            ValueInstantiator valueInstantiator = findValueInstantiator(context, beanDesc, entity);

            builder.setValueInstantiator(valueInstantiator);

            SettableBeanProperty[] constructorProperties = valueInstantiator.getFromObjectArguments(context.getConfig());
            if (constructorProperties == null) {
                constructorProperties = new SettableBeanProperty[0];
            }
            final ImmutableMap<String, SettableBeanProperty> constructorPropertiesMap = Maps.uniqueIndex(Arrays.asList(constructorProperties),
                    new Function<SettableBeanProperty, String>() {

                        public String apply(final SettableBeanProperty input) {
                            return input.getName();
                        }

                    });

            entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

                public void doWithPersistentProperty(final MongoPersistentProperty persistentProperty) {
                    if (persistentProperty.shallBePersisted()) {
                        try {
                            addProperty(context, beanDesc, builder, constructorPropertiesMap, persistentProperty);
                        } catch (JsonMappingException e) {
                            throw new RuntimeException("Can't add property", e);
                        }
                    }
                }
            });

            entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {


                public void doWithAssociation(final Association<MongoPersistentProperty> association) {
                    try {
                        addProperty(context, beanDesc, builder, constructorPropertiesMap, association.getInverse());
                    } catch (JsonMappingException e) {
                        throw new RuntimeException("Can't add property", e);
                    }
                }
            });

            JsonDeserializer<?> deserializer;

            if (type.isAbstract() && !valueInstantiator.canInstantiate()) {
                deserializer = builder.buildAbstract();
            } else {
                deserializer = builder.build();
            }

            if (_factoryConfig.hasDeserializerModifiers()) {
                for (BeanDeserializerModifier mod : _factoryConfig.deserializerModifiers()) {
                    deserializer = mod.modifyDeserializer(context.getConfig(), beanDesc, deserializer);
                }
            }
            return (JsonDeserializer<Object>) deserializer;

        } else {
            return super.buildBeanDeserializer(context, type, beanDesc);
        }
    }

    @Override
    public DeserializerFactory withConfig(final DeserializerFactoryConfig config) {
        if (_factoryConfig == config) {
            return this;
        }
        if (getClass() != MongoBeanDeserializerFactory.class) {
            throw new IllegalStateException("Subtype of BeanDeserializerFactory (" + getClass().getName()
                    + ") has not properly overridden method 'withAdditionalDeserializers': can not instantiate subtype with "
                    + "additional deserializer definitions");
        }
        return new MongoBeanDeserializerFactory(config, mongoMappingContext, conversionService, mongoDbFactory, mongoObjectMapper,
                useFieldAccessOnly);
    }

    public void setMongoObjectMapper(MongoObjectMapper mongoObjectMapper) {
        this.mongoObjectMapper = mongoObjectMapper;
    }

    private void addProperty(final DeserializationContext context, final BeanDescription beanDesc, final BeanDeserializerBuilder builder,
                             final Map<String, SettableBeanProperty> constructorPropertiesMap, final MongoPersistentProperty persistentProperty)
            throws JsonMappingException {
        if (constructorPropertiesMap.containsKey(persistentProperty.getFieldName())) {
            builder.addCreatorProperty(constructorPropertiesMap.get(persistentProperty.getFieldName()));
        } else {
            JavaType javaType = JacksonHelper.createJavaType(persistentProperty);
            try {
                javaType = resolveType(context, beanDesc, javaType, JacksonHelper.createAnnotatedField(persistentProperty));
            } catch (JsonMappingException e) {
                throw new RuntimeException(e);
            }
            SettableBeanProperty beanProperty;
            TypeDeserializer typeDeserializer = javaType.getTypeHandler();
            if (typeDeserializer == null) {
                try {
                    typeDeserializer = findTypeDeserializer(context.getConfig(), javaType);
                } catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                }
            }
            if (useFieldAccessOnly || (persistentProperty.getSetter() == null)) {
                BeanPropertyDefinition beanPropertyDefinition = JacksonHelper.createBeanPropertyDefinition(persistentProperty, false);
                beanProperty = new FieldProperty(beanPropertyDefinition, javaType, typeDeserializer, new AnnotationMap(),
                        beanPropertyDefinition.getField());
            } else {
                BeanPropertyDefinition beanPropertyDefinition = JacksonHelper.createBeanPropertyDefinition(persistentProperty, true);
                beanProperty = new MethodProperty(beanPropertyDefinition, javaType, typeDeserializer, new AnnotationMap(),
                        beanPropertyDefinition.getSetter());
            }

            if (persistentProperty.isIdProperty()) {
                beanProperty = beanProperty.withValueDeserializer(new IdDeserializer(conversionService, persistentProperty.getType()));
            }
            if (persistentProperty.isAssociation()) {
                beanProperty = beanProperty.withValueDeserializer(createDbRefDeserializer(context, persistentProperty, beanProperty,
                        new DBRefDeserializer(mongoDbFactory, mongoObjectMapper, persistentProperty)));
            }

            builder.addProperty(beanProperty);
        }
    }

    private AnnotatedConstructor createAnnotatedConstructor(final PreferredConstructor<?, MongoPersistentProperty> preferredConstructor) {
        return new AnnotatedConstructor(preferredConstructor.getConstructor(), JacksonHelper.createAnnotationMap(preferredConstructor
                .getConstructor()), new AnnotationMap[]{});
    }

    private AnnotatedParameter createAnnotatedParameter(final Parameter<Object, MongoPersistentProperty> parameter, final AnnotatedWithParams owner,
                                                        final int index) {
        return new AnnotatedParameter(owner, parameter.getRawType(), new AnnotationMap(), index);
    }

    private CreatorProperty[] createCreatorProperties(final DeserializationContext context, final BeanDescription beanDesc,
                                                      final AnnotatedWithParams constructor, final PreferredConstructor<?, MongoPersistentProperty> preferredConstructor,
                                                      final MongoPersistentEntity<?> entity) throws JsonMappingException {
        ArrayList<CreatorProperty> creatorProperties = new ArrayList<CreatorProperty>();
        int i = 0;
        for (Parameter<Object, MongoPersistentProperty> parameter : preferredConstructor.getParameters()) {
            MongoPersistentProperty property = entity.getPersistentProperty(parameter.getName());
            JavaType javaType = JacksonHelper.createJavaType(property);
            try {
                javaType = resolveType(context, beanDesc, javaType, JacksonHelper.createAnnotatedField(property));
            } catch (JsonMappingException e) {
                throw new RuntimeException(e);
            }
            TypeDeserializer typeDeserializer = javaType.getTypeHandler();
            if (typeDeserializer == null) {
                try {
                    typeDeserializer = findTypeDeserializer(context.getConfig(), javaType);
                } catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                }
            }
            CreatorProperty creatorProperty = new CreatorProperty(property.getFieldName(), javaType, typeDeserializer, new AnnotationMap(),
                    createAnnotatedParameter(parameter, constructor, i), i++, null);
            if (property.isAssociation()) {
                creatorProperty = creatorProperty.withValueDeserializer(createDbRefDeserializer(context, property, creatorProperty,
                        new DBRefDeserializer(
                                mongoDbFactory, mongoObjectMapper, property)));
            }
            if (property.isIdProperty()) {
                creatorProperty = creatorProperty.withValueDeserializer(new IdDeserializer(conversionService, property.getType()));
            }
            creatorProperties.add(creatorProperty);
        }
        return creatorProperties.toArray(new CreatorProperty[0]);
    }

    @SuppressWarnings("unchecked")
    private JsonDeserializer<?> createDbRefDeserializer(final DeserializationContext context, final MongoPersistentProperty property,
                                                        final BeanProperty beanProperty, final JsonDeserializer<?> jsonDeserializer) throws JsonMappingException {
        JavaType mainType = JacksonHelper.createJavaType(property);
        if (property.isCollectionLike()) {
            if (property.isArray()) {
                return new ObjectArrayDeserializer((ArrayType) mainType, (JsonDeserializer<Object>) jsonDeserializer, findTypeDeserializer(
                        context.getConfig(), mainType.getContentType()));
            } else {
                if (mainType.isInterface() || mainType.isAbstract()) {
                    @SuppressWarnings({"rawtypes"})
                    Class<? extends Collection> fallback = _collectionFallbacks.get(mainType.getRawClass().getName());
                    if (fallback == null) {
                        throw new IllegalArgumentException("Can not find a deserializer for non-concrete Collection type " + mainType);
                    }
                    mainType = context.getConfig().constructSpecializedType(mainType, fallback);
                }
                BeanDescription collectionBeanDescription = context.getConfig().introspectForCreation(mainType);
                ValueInstantiator inst = findValueInstantiator(context, collectionBeanDescription);
                return new CollectionDeserializer(mainType, (JsonDeserializer<Object>) jsonDeserializer, findTypeDeserializer(
                        context.getConfig(), mainType), inst);

            }
        } else if (property.isMap()) {
            KeyDeserializer keyDeserializer = context.findKeyDeserializer(SimpleType.construct(property.getComponentType()), beanProperty);

            if (mainType.isInterface() || mainType.isAbstract()) {
                @SuppressWarnings("rawtypes")
                Class<? extends Map> fallback = _mapFallbacks.get(mainType.getRawClass().getName());
                if (fallback == null) {
                    throw new IllegalArgumentException("Can not find a deserializer for non-concrete Map type " + mainType);
                }
                mainType = context.getConfig().constructSpecializedType(mainType, fallback);
            }

            BeanDescription mapBeanDescription = context.getConfig().introspectForCreation(mainType);

            return new MapDeserializer(mainType, findValueInstantiator(
                    context, mapBeanDescription), keyDeserializer, (JsonDeserializer<Object>) jsonDeserializer, findTypeDeserializer(
                    context.getConfig(), mainType));
        }
        return jsonDeserializer;
    }

    private ValueInstantiator findValueInstantiator(final DeserializationContext context, final BeanDescription beanDesc,
                                                    final MongoPersistentEntity<?> entity) throws JsonMappingException {
        StdValueInstantiator instantiator = new StdValueInstantiator(context.getConfig(), entity.getType());
        PreferredConstructor<?, MongoPersistentProperty> constructor = entity.getPersistenceConstructor();
        if (constructor == null) {
            return instantiator;
        }

        AnnotatedConstructor annotatedConstructor = createAnnotatedConstructor(constructor);
        if (constructor.isNoArgConstructor()) {
            instantiator.configureFromObjectSettings(annotatedConstructor, null, null, null, null, null);
        } else {
            instantiator.configureFromObjectSettings(null, null, null, null, annotatedConstructor,
                    createCreatorProperties(context, beanDesc, annotatedConstructor, constructor, entity));
        }
        return instantiator;
    }

    private boolean isMongoEntity(final Class<?> clazz) {
        return mongoMappingContext.getPersistentEntity(clazz) != null;
    }

}
