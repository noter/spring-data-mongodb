package org.springframework.data.mongodb.core.convert.custom;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.mongodb.core.mapping.event.*;

public class CustomAbstractMongoEventListner<E> extends AbstractMongoEventListener<E> {

	private final Class<? extends Object> domainClass;

	private static final Logger LOG = LoggerFactory.getLogger(CustomAbstractMongoEventListner.class);

	public CustomAbstractMongoEventListner() {
		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(this.getClass(), AbstractMongoEventListener.class);
		this.domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	public void onAfterDalete(final E source, final DBObject dbo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("onAfterSave(" + source + ", " + dbo + ")");
		}
	}

	@Override
	public void onApplicationEvent(final MongoMappingEvent<?> event) {

		if (event instanceof AfterLoadEvent) {
			AfterLoadEvent<?> afterLoadEvent = (AfterLoadEvent<?>) event;

			if (domainClass.isAssignableFrom(afterLoadEvent.getType())) {
				onAfterLoad(event.getDBObject());
			}

			return;
		}

		@SuppressWarnings("unchecked")
		E source = (E) event.getSource();

		// Check for matching domain type and invoke callbacks
		if ((source != null) && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeConvertEvent) {
			onBeforeConvert(source);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave(source, event.getDBObject());
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave(source, event.getDBObject());
		} else if (event instanceof AfterConvertEvent) {
			onAfterConvert(event.getDBObject(), source);
		} else if (event instanceof AfterDeleteEvent) {
			onAfterDalete(source, event.getDBObject());
		}
	}

}
