package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import java.util.Collection;

public class CustomTypeResolverBuilder extends StdTypeResolverBuilder {

	@Override
	public TypeSerializer buildTypeSerializer(final SerializationConfig config, final JavaType baseType, final Collection<NamedType> subtypes) {
		if (_idType == JsonTypeInfo.Id.NONE) {
			return null;
		}
		TypeIdResolver idRes = idResolver(config, baseType, subtypes, true, false);
		return new CustomAsPropertyTypeSerializer(idRes, null, _typeProperty);
	}

}
