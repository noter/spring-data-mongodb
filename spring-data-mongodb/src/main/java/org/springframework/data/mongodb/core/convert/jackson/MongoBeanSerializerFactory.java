package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.ser.*;
import com.fasterxml.jackson.databind.ser.std.MapSerializer;
import com.fasterxml.jackson.databind.ser.std.ObjectArraySerializer;
import com.fasterxml.jackson.databind.ser.std.StdContainerSerializers;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import java.util.ArrayList;
import java.util.List;

public class MongoBeanSerializerFactory extends BeanSerializerFactory {

    private static final long serialVersionUID = 1L;

    private final MongoMappingContext mongoMappingContext;

    private final ConversionService conversionService;

    public MongoBeanSerializerFactory(final MongoMappingContext mappingContext, final ConversionService conversionService) {
        this(null, mappingContext, conversionService);
    }

    public MongoBeanSerializerFactory(final SerializerFactoryConfig config, final MongoMappingContext mongoMappingContext,
                                      final ConversionService conversionService) {
        super(config);
        this.mongoMappingContext = mongoMappingContext;
        this.conversionService = conversionService;
    }

    @Override
    public SerializerFactory withConfig(final SerializerFactoryConfig config) {
        if (_factoryConfig == config) {
            return this;
        }
        if (getClass() != MongoBeanSerializerFactory.class) {
            throw new IllegalStateException("Subtype of BeanSerializerFactory (" + getClass().getName()
                    + ") has not properly overridden method 'withAdditionalSerializers': can not instantiate subtype with "
                    + "additional serializer definitions");
        }
        return new MongoBeanSerializerFactory(config, mongoMappingContext, conversionService);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected JsonSerializer<Object> constructBeanSerializer(final SerializerProvider prov, final BeanDescription beanDesc)
            throws JsonMappingException {
        if (isMongoEntity(beanDesc.getBeanClass())) {
            BeanSerializerBuilder builder = constructBeanSerializerBuilder(beanDesc);
            MongoPersistentEntity<?> mongoPersistentEntity = mongoMappingContext.getPersistentEntity(beanDesc.getBeanClass());

            final List<BeanPropertyWriter> beanPropertyWriters = new ArrayList<BeanPropertyWriter>();

            mongoPersistentEntity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

                public void doWithPersistentProperty(final MongoPersistentProperty persistentProperty) {
                    if (persistentProperty.shallBePersisted()) {
                        PropertyBuilder pb = constructPropertyBuilder(prov.getConfig(), beanDesc);
                        BeanPropertyDefinition propDef = JacksonHelper.createBeanPropertyDefinition(persistentProperty, false);
                        try {
                            BeanPropertyWriter writer = _constructWriter(prov, propDef, beanDesc.bindingsForBeanType(), pb, false, propDef.getAccessor());
                            if (persistentProperty.isIdProperty()) {
                                writer.assignSerializer(new IdSerializer(conversionService));
                            }
                            beanPropertyWriters.add(writer);
                        } catch (JsonMappingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
            mongoPersistentEntity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {


                public void doWithAssociation(final Association<MongoPersistentProperty> association) {
                    MongoPersistentProperty persistentProperty = association.getInverse();
                    PropertyBuilder pb = constructPropertyBuilder(prov.getConfig(), beanDesc);
                    BeanPropertyDefinition propDef = JacksonHelper.createBeanPropertyDefinition(persistentProperty, false);
                    try {
                        BeanPropertyWriter writer = _constructWriter(prov, propDef, beanDesc.bindingsForBeanType(), pb, false, propDef.getAccessor());

                        JsonSerializer<Object> jsonSerializer = new DBRefSerializer(mongoMappingContext, conversionService, persistentProperty
                                .getDBRef());

                        if (persistentProperty.isCollectionLike()) {
                            JavaType type = JacksonHelper.createJavaType(persistentProperty.getTypeInformation().getComponentType());
                            if (persistentProperty.isArray()) {
                                JsonSerializer<?> arraySerializer = new ObjectArraySerializer(type, true, null, jsonSerializer);
                                writer.assignSerializer((JsonSerializer<Object>) arraySerializer);
                            } else {
                                JsonSerializer<?> collectionSerializer = StdContainerSerializers.collectionSerializer(type, true, null,
                                        jsonSerializer);
                                writer.assignSerializer((JsonSerializer<Object>) collectionSerializer);
                            }
                        } else if (persistentProperty.isMap()) {
                            JavaType type = JacksonHelper.createJavaType(persistentProperty.getTypeInformation().getMapValueType());
                            SimpleType keyType = SimpleType.construct(persistentProperty.getComponentType());
                            JsonSerializer<Object> keySerializer = prov.findKeySerializer(keyType, writer);
                            JsonSerializer<?> mapSerializer = MapSerializer.construct(new String[0],
                                    MapType.construct(persistentProperty.getType(), keyType,
                                            type), true, null, keySerializer, jsonSerializer);
                            writer.assignSerializer((JsonSerializer<Object>) mapSerializer);
                        } else {
                            writer.assignSerializer(jsonSerializer);
                        }

                        beanPropertyWriters.add(writer);
                    } catch (JsonMappingException e) {
                        throw new RuntimeException(e);
                    }

                }
            });

            builder.setProperties(beanPropertyWriters);

            JsonSerializer<Object> ser = (JsonSerializer<Object>) builder.build();

            if (ser == null) {
                if (beanDesc.hasKnownClassAnnotations()) {
                    return builder.createDummy();
                }
            }
            return ser;

        } else {
            return super.constructBeanSerializer(prov, beanDesc);
        }
    }

    private boolean isMongoEntity(final Class<?> clazz) {
        return mongoMappingContext.getPersistentEntity(clazz) != null;
    }

}
