package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Strings;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

import java.io.IOException;

public class DBRefSerializer extends JsonSerializer<Object> {

	private final MongoMappingContext mappingContext;

	private final DBRef dbRef;

	private final ConversionService conversionService;

	private final IdSerializer idSerializer;

	public DBRefSerializer(final MongoMappingContext mappingContext, final ConversionService conversionService, final DBRef dbRef) {
		this.mappingContext = mappingContext;
		this.conversionService = conversionService;
		this.dbRef = dbRef;
		idSerializer = new IdSerializer(conversionService);
	}

	@Override
	public void serialize(final Object value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException,
			JsonProcessingException {
		BasicMongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(value.getClass());
		BeanWrapper<MongoPersistentEntity<Object>, Object> beanWrapper = BeanWrapper.create(value, conversionService);
		jgen.writeStartObject();
		// ref
		jgen.writeFieldName("$ref");
		jgen.writeString(persistentEntity.getCollection());

		// id
		jgen.writeFieldName("$id");

		idSerializer.serialize(beanWrapper.getProperty(persistentEntity.getIdProperty()), jgen, provider);

		// db
		if (!Strings.isNullOrEmpty(dbRef.db())) {
			jgen.writeFieldName("$db");
			jgen.writeString(dbRef.db());
		}
		jgen.writeEndObject();
	}

	@Override
	public void serializeWithType(final Object value, final JsonGenerator jgen, final SerializerProvider provider, final TypeSerializer typeSer)
			throws IOException, JsonProcessingException {
		serialize(value, jgen, provider);
	}

}
