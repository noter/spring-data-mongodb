package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBEncoder;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DefaultDBEncoder;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

public class JacksonDBEncoderFactory implements DBEncoderFactory {

	private final ObjectMapper objectMapper;
	private final DBEncoder defaultDBEncoder = DefaultDBEncoder.FACTORY.create();
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mongoMappingContext;
	private final ConversionService conversionService;

	public JacksonDBEncoderFactory(final ObjectMapper objectMapper,
			final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			final ConversionService conversionService) {
		this.objectMapper = objectMapper;
		mongoMappingContext = mappingContext;
		this.conversionService = conversionService;
	}


	public DBEncoder create() {
		return new JacksonDBEncoder(objectMapper, defaultDBEncoder, mongoMappingContext, conversionService);
	}

}
