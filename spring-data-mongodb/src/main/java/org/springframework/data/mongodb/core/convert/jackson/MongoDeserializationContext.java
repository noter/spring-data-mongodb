package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.deser.DeserializerFactory;

public class MongoDeserializationContext extends DefaultDeserializationContext {

    private QueryContext queryContext;

    public MongoDeserializationContext(MongoBeanDeserializerFactory df) {
        super(df, null);
    }

    protected MongoDeserializationContext(MongoDeserializationContext src,
                                          DeserializationConfig config, JsonParser jp, InjectableValues values, QueryContext queryContext) {
        super(src, config, jp, values);
        this.queryContext = queryContext;
    }

    protected MongoDeserializationContext(MongoDeserializationContext src,
                                          DeserializationConfig config, JsonParser jp, InjectableValues values) {
        super(src, config, jp, values);
    }


    protected MongoDeserializationContext(MongoDeserializationContext src, DeserializerFactory factory) {
        super(src, factory);
    }

    @Override
    public MongoDeserializationContext createInstance(DeserializationConfig config,
                                                      JsonParser jp, InjectableValues values) {
        return new MongoDeserializationContext(this, config, jp, values);
    }

    public MongoDeserializationContext createInstance(DeserializationConfig config,
                                                      JsonParser jp, InjectableValues values, QueryContext queryContext) {
        return new MongoDeserializationContext(this, config, jp, values, queryContext);
    }

    @Override
    public MongoDeserializationContext with(DeserializerFactory factory) {
        return new MongoDeserializationContext(this, factory);
    }

    public QueryContext getQueryContext() {
        return queryContext;
    }


}
