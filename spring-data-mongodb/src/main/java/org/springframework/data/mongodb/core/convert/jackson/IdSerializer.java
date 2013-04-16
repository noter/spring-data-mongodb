package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import de.undercouch.bson4jackson.BsonGenerator;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionService;

import java.io.IOException;

public class IdSerializer extends JsonSerializer<Object> {

	private final ConversionService conversionService;

	public IdSerializer(final ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	@Override
	public void serialize(final Object value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException,
			JsonProcessingException {
		if (value == null)
			return;

		ObjectId id = null;
		try {
			id = conversionService.convert(value, ObjectId.class);
		} catch (Exception e) {
		}
		if (id != null) {
			BsonGenerator bsonGenerator = (BsonGenerator) jgen;
			bsonGenerator.writeObjectId(new de.undercouch.bson4jackson.types.ObjectId(id.getTimeSecond(), id.getMachine(), id.getInc()));
		} else {
			jgen.writeString(conversionService.convert(value, String.class));
		}

	}

	@Override
	public void serializeWithType(final Object value, final JsonGenerator jgen, final SerializerProvider provider, final TypeSerializer typeSer)
			throws IOException, JsonProcessingException {
		serialize(value, jgen, provider);
	}
}
