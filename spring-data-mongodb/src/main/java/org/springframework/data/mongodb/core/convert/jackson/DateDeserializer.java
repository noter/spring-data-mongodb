package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Date;

public class DateDeserializer extends StdDeserializer<Date> {

	private static final long serialVersionUID = 1L;

	protected DateDeserializer() {
		super(Date.class);
	}

	@Override
	public Date deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonToken token = jp.getCurrentToken();
		if (token == JsonToken.VALUE_EMBEDDED_OBJECT) {
			// See if it's a date
			Object date = jp.getEmbeddedObject();
			if (date instanceof Date) {
				return (Date) date;
			} else {
				throw ctxt.mappingException(Date.class);
			}
		} else {
			return _parseDate(jp, ctxt);
		}
	}

}
