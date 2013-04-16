package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import de.undercouch.bson4jackson.types.ObjectId;
import org.springframework.core.convert.ConversionService;

import java.io.IOException;

public class IdDeserializer extends JsonDeserializer<Object> {

	private final ConversionService conversionService;

	private final Class<?> idType;

	public IdDeserializer(final ConversionService conversionService, final Class<?> idType) {
		this.conversionService = conversionService;
		this.idType = idType;
	}

	@Override
	public Object deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
		if (jp.getCurrentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
			ObjectId objectId = (ObjectId) jp.getEmbeddedObject();
			return conversionService.convert(new org.bson.types.ObjectId(objectId.getTime(), objectId.getMachine(), objectId.getInc()), idType);
		}
		return conversionService.convert(jp.getText(), idType);
	}

	@Override
	public Object deserialize(final JsonParser jp, final DeserializationContext ctxt, final Object intoValue) throws IOException,
			JsonProcessingException {
		return deserialize(jp, ctxt);
	}

	@Override
	public Object deserializeWithType(final JsonParser jp, final DeserializationContext ctxt, final TypeDeserializer typeDeserializer)
			throws IOException, JsonProcessingException {
		return deserialize(jp, ctxt);
	}
}
