package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBEncoder;
import org.bson.BSONObject;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

public class JacksonDBEncoder implements DBEncoder {
	private final ObjectMapper objectMapper;
	private final DBEncoder defaultDBEncoder;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final ConversionService conversionService;

	public JacksonDBEncoder(final ObjectMapper objectMapper, final DBEncoder defaultDbEncoder,
			final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			final ConversionService conversionService) {
		this.objectMapper = objectMapper;
		defaultDBEncoder = defaultDbEncoder;
		this.mappingContext = mappingContext;
		this.conversionService = conversionService;
	}


	public int writeObject(final OutputBuffer buf, final BSONObject o) {
		if (o.containsField(JacksonMappingMongoConverter.OBJECT_KEY)) {
			Object actualObject = o.get(JacksonMappingMongoConverter.OBJECT_KEY);
			MongoPersistentEntity<?> mongoPersistentEntity = mappingContext.getPersistentEntity(actualObject.getClass());
			if (mongoPersistentEntity != null) {
				MongoPersistentProperty idProperty = mongoPersistentEntity.getIdProperty();
				if (idProperty != null) {
					BeanWrapper<MongoPersistentEntity<Object>, Object> beanWrapper = BeanWrapper.create(actualObject, conversionService);
					Object property = beanWrapper.getProperty(idProperty);
					if (property == null) {
						property = new ObjectId(o.get("_id").toString());
						beanWrapper.setProperty(idProperty, property);
					}
				}
			}
			int start = buf.getPosition();
			try {
				objectMapper.writeValue(buf, actualObject);
				int size = buf.getPosition() - start;
				return size;
			} catch (Exception e) {
				throw new RuntimeException("Can't serialize Object as BSON", e);
			}
		} else {
			return defaultDBEncoder.writeObject(buf, o);
		}
	}
}
