package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;

import java.io.IOException;

public class CustomAsPropertyTypeSerializer extends AsPropertyTypeSerializer {

	public CustomAsPropertyTypeSerializer(final TypeIdResolver idRes, final BeanProperty property, final String propName) {
		super(idRes, property, propName);
	}

	@Override
	public void writeCustomTypePrefixForArray(final Object value, final JsonGenerator jgen, final String typeId) throws IOException,
			JsonProcessingException {
		jgen.writeStartArray();
	}

	@Override
	public void writeCustomTypeSuffixForArray(final Object value, final JsonGenerator jgen, final String typeId) throws IOException,
			JsonProcessingException {
		jgen.writeEndArray();
	}

	@Override
	public void writeTypePrefixForArray(final Object value, final JsonGenerator jgen) throws IOException, JsonProcessingException {
		jgen.writeStartArray();
	}

	@Override
	public void writeTypePrefixForArray(final Object value, final JsonGenerator jgen, final Class<?> type) throws IOException,
			JsonProcessingException {
		jgen.writeStartArray();
	}

	@Override
	public void writeTypeSuffixForArray(final Object value, final JsonGenerator jgen) throws IOException, JsonProcessingException {
		jgen.writeEndArray();
	}

}
