package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;

public class AfterDeleteEvent<E> extends MongoMappingEvent<E> {

	private static final long serialVersionUID = 1L;

	public AfterDeleteEvent(final E source, final DBObject dbo) {
		super(source, dbo);
	}

}
