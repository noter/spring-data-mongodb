package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;

public class MongoObjectReader extends ObjectReader {

    private MongoDeserializationContext context;

    private QueryContext queryContext;

    protected MongoObjectReader(MongoObjectMapper mapper, DeserializationConfig config, QueryContext queryContext) {
        super(mapper, config);
        this.queryContext = queryContext;
        this.context = mapper.dc;
    }

    protected Object _bindAndClose(JsonParser jp, Object valueToUpdate)
            throws IOException, JsonParseException, JsonMappingException {
        if (_schema != null) {
            jp.setSchema(_schema);
        }
        try {
            Object result;
            JsonToken t = _initForReading(jp);
            if (t == JsonToken.VALUE_NULL) {
                if (valueToUpdate == null) {
                    DeserializationContext ctxt = customCreateDeserializationContext(jp, _config);
                    result = _findRootDeserializer(ctxt, _valueType).getNullValue();
                } else {
                    result = valueToUpdate;
                }
            } else if (t == JsonToken.END_ARRAY || t == JsonToken.END_OBJECT) {
                result = valueToUpdate;
            } else {
                DeserializationContext ctxt = customCreateDeserializationContext(jp, _config);
                JsonDeserializer<Object> deser = _findRootDeserializer(ctxt, _valueType);
                if (_unwrapRoot) {
                    result = _unwrapAndDeserialize(jp, ctxt, _valueType, deser);
                } else {
                    if (valueToUpdate == null) {
                        result = deser.deserialize(jp, ctxt);
                    } else {
                        deser.deserialize(jp, ctxt, valueToUpdate);
                        result = valueToUpdate;
                    }
                }
            }
            return result;
        } finally {
            try {
                jp.close();
            } catch (IOException ioe) {
            }
        }
    }

    //TODO nie potrzebne po  https://github.com/FasterXML/jackson-databind/issues/206
    private MongoDeserializationContext customCreateDeserializationContext(JsonParser jsonParser, DeserializationConfig config) {
        return context.createInstance(config, jsonParser, _injectableValues, queryContext);
    }
}
