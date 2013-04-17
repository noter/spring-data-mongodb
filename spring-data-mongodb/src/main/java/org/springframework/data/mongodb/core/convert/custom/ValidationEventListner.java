package org.springframework.data.mongodb.core.convert.custom;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.ValidatingMongoEventListener;
import org.springframework.util.Assert;

import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import java.util.Set;

public class ValidationEventListner extends AbstractMongoEventListener<Object> {

	private Class<?>[] groups;

	private static final Logger LOG = LoggerFactory.getLogger(ValidatingMongoEventListener.class);

	private final Validator validator;

	/**
	 * Creates a new {@link org.springframework.data.mongodb.core.mapping.event.ValidatingMongoEventListener} using the given {@link javax.validation.Validator}.
	 * 
	 * @param validator
	 *            must not be {@literal null}.
	 */
	public ValidationEventListner(final Validator validator) {
		Assert.notNull(validator);
		this.validator = validator;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void onBeforeSave(final Object source, final DBObject dbo) {

		LOG.debug("Validating object: {}", source);
		Set violations;
		if (groups != null) {
			violations = validator.validate(source);
		} else {
			violations = validator.validate(source, groups);
		}

		if (!violations.isEmpty()) {

			LOG.debug("During object: {} validation violations found: {}", source, violations);
			throw new ConstraintViolationException(violations);
		}
	}

	public void setGroups(final Class<?>[] groups) {
		this.groups = groups;
	}

}
