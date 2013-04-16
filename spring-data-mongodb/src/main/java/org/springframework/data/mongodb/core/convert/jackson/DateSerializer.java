package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import de.undercouch.bson4jackson.BsonGenerator;

import java.io.IOException;
import java.util.Date;

public class DateSerializer extends JsonSerializer<Date> {

	@Override
	public void serialize(final Date value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
		BsonGenerator bsonGenerator = (BsonGenerator) jgen;
		bsonGenerator.writeDateTime(value);
	}

	@Override
	public void serializeWithType(final Date value, final JsonGenerator jgen, final SerializerProvider provider, final TypeSerializer typeSer)
			throws IOException, JsonProcessingException {
		serialize(value, jgen, provider);
	}

}
