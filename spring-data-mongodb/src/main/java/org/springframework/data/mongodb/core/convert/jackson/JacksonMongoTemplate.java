package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

public class JacksonMongoTemplate extends MongoTemplate {


	private JacksonMappingMongoConverter mongoConverter;

	public JacksonMongoTemplate(MongoDbFactory mongoDbFactory, JacksonMappingMongoConverter mongoConverter) {
		super(mongoDbFactory, mongoConverter);
		this.mongoConverter = mongoConverter;
	}

	@Override
	protected void prepareCollection(DBCollection collection) {
		super.prepareCollection(collection);
		collection.setDBDecoderFactory(mongoConverter.getDecoderFactory());
		collection.setDBEncoderFactory(mongoConverter.getEncoderFactory());
	}
}
