package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.DatabindVersion;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.google.common.collect.Sets;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoAnnotationIntrospector extends AnnotationIntrospector {

	private TypeResolverBuilder<?> typeResolverBuilder;

	private final SimpleTypeHolder simpleTypeHolder;

	public MongoAnnotationIntrospector() {
		simpleTypeHolder = new SimpleTypeHolder(Sets.newHashSet(BigDecimal.class, BigInteger.class, List.class, Map.class, Set.class), true);

		// Type resolver
		typeResolverBuilder = new CustomTypeResolverBuilder();
		typeResolverBuilder = typeResolverBuilder.init(com.fasterxml.jackson.annotation.JsonTypeInfo.Id.CLASS, null);
		typeResolverBuilder = typeResolverBuilder.inclusion(As.PROPERTY);
		typeResolverBuilder = typeResolverBuilder.typeProperty("_class");

	}

	@Override
	public String findTypeName(final AnnotatedClass ac) {
		return ac.getRawType().getName();
	}

	@Override
	public TypeResolverBuilder<?> findTypeResolver(final MapperConfig<?> config, final AnnotatedClass ac, final JavaType baseType) {
		if (!simpleTypeHolder.isSimpleType(baseType.getRawClass()) || baseType.getRawClass().equals(Object.class)) {
			return typeResolverBuilder;
		}
		return super.findTypeResolver(config, ac, baseType);
	}

	@Override
	public boolean hasIgnoreMarker(final AnnotatedMember m) {
		return m.hasAnnotation(Transient.class);
	}

	@Override
	public Version version() {
		return DatabindVersion.instance.version();
	}

}
