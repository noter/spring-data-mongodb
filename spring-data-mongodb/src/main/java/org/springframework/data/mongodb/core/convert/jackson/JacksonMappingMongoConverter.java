package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.DBObject;
import de.undercouch.bson4jackson.BsonGenerator;
import de.undercouch.bson4jackson.BsonParser;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.*;

public class JacksonMappingMongoConverter extends MappingMongoConverter {

    public static final String OBJECT_KEY = "^#object#^";

    private final MongoObjectMapper objectMapper;

    private final JacksonDBEncoderFactory encoderFactory;

    private JacksonDBDecoderFactory decoderFactory;


    public JacksonMappingMongoConverter(final MongoDbFactory mongoDbFactory, final MongoMappingContext mappingContext) {
        super(mongoDbFactory, mappingContext);

        MongoBsonFactory bsonFactory = new MongoBsonFactory();
        bsonFactory.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
        bsonFactory.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);

        MongoBeanDeserializerFactory mongoBeanDeserializerFactory = new MongoBeanDeserializerFactory(mappingContext, getConversionService(),
                mongoDbFactory, null, false);

        objectMapper = new MongoObjectMapper(bsonFactory, null, new MongoDeserializationContext(mongoBeanDeserializerFactory));

        encoderFactory = new JacksonDBEncoderFactory(objectMapper, mappingContext, getConversionService());
        decoderFactory = new JacksonDBDecoderFactory(objectMapper, null);
        mongoBeanDeserializerFactory.setMongoObjectMapper(objectMapper);

        objectMapper.setSerializerFactory(new MongoBeanSerializerFactory(mappingContext, conversionService));

        // Serialization
        objectMapper.disable(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS);
        objectMapper.disable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        objectMapper.enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
        objectMapper.setSerializationInclusion(Include.NON_NULL);

        // Deserialization
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

        // Default implementation
        SimpleModule simpleModule = new SimpleModule("mongo_module", Version.unknownVersion());
        simpleModule.addAbstractTypeMapping(List.class, ArrayList.class);
        simpleModule.addAbstractTypeMapping(Map.class, HashMap.class);
        simpleModule.addAbstractTypeMapping(Set.class, HashSet.class);
        simpleModule.addSerializer(Date.class, new DateSerializer());
        simpleModule.addDeserializer(Date.class, new DateDeserializer());
        objectMapper.registerModule(simpleModule);

        // Annotations
        objectMapper.setAnnotationIntrospector(new MongoAnnotationIntrospector());
    }

    @Override
    public <R> R read(final Class<R> type, final DBObject source) {
        if (source == null) {
            return null;
        }
        if (source.containsField(OBJECT_KEY)) {
            return (R) source.get(OBJECT_KEY);
        }
        return super.read(type, source);
    }

    @Override
    public void write(final Object source, final DBObject sink) {
        if (source == null) {
            return;
        }
        sink.put(OBJECT_KEY, source);
    }

    public JacksonDBDecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    public JacksonDBEncoderFactory getEncoderFactory() {
        return encoderFactory;
    }

    public MongoObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static class DBRefSuspender {

        boolean resolveAll = true;

        Set<Class<?>> resolveTypes = new HashSet<Class<?>>();

        public void clear() {
            resolveAll = true;
            resolveTypes.clear();
        }

        public void doNotResolve() {
            resolveAll = false;
        }

        public void resolveOnly(final Class<?> clazz) {
            resolveAll = false;
            resolveTypes.add(clazz);
        }

        public boolean shouldBeResolved(final Class<?> clazz) {
            return resolveTypes.contains(clazz);
        }

        public boolean isResolveAll() {
            return resolveAll;
        }

    }

}
