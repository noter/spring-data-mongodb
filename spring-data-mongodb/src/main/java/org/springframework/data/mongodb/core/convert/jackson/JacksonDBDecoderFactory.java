package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.DBDecoder;
import com.mongodb.DBDecoderFactory;

public class JacksonDBDecoderFactory implements DBDecoderFactory {

    private final MongoObjectMapper objectMapper;

    private QueryContext queryContext;

    public JacksonDBDecoderFactory(final MongoObjectMapper objectMapper, QueryContext queryContext) {
        this.objectMapper = objectMapper;
        this.queryContext = queryContext;
    }


    public DBDecoder create() {
        return new JacksonDBDecoder(objectMapper, queryContext);
    }
}
