package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;

public class MongoObjectMapper extends ObjectMapper {

    protected MongoDeserializationContext dc;

    public MongoObjectMapper(JsonFactory jf, DefaultSerializerProvider sp, MongoDeserializationContext dc) {
        super(jf, sp, dc);
        this.dc = dc;
    }

    public MongoObjectReader with(QueryContext queryContext) {
        return new MongoObjectReader(this, _deserializationConfig, queryContext);
    }


}
