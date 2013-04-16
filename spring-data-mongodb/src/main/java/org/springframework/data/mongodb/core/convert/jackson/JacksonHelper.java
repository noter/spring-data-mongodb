package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotationMap;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.util.SimpleBeanPropertyDefinition;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;

public class JacksonHelper {
	public static AnnotatedField createAnnotatedField(final MongoPersistentProperty property) {
		AnnotationMap annotationMap = createAnnotationMap(property.getField());
		if (property.getGetter() != null) {
			annotationMap = createAnnotationMap(property.getGetter(), annotationMap);
		}

		return new AnnotatedField(property.getField(), annotationMap);
	}

	public static AnnotatedMethod createAnnotatedSetter(final MongoPersistentProperty property) {
		AnnotationMap annotationMap = createAnnotationMap(property.getField());
		annotationMap = createAnnotationMap(property.getSetter(), annotationMap);
		return new AnnotatedMethod(property.getSetter(), annotationMap, new AnnotationMap[0]);
	}

	public static AnnotationMap createAnnotationMap(final AccessibleObject accessibleObject, final AnnotationMap... toJoin) {
		AnnotationMap annotationMap = new AnnotationMap();
		if (accessibleObject.getAnnotations() != null) {
			for (Annotation annotation : accessibleObject.getAnnotations()) {
				annotationMap.add(annotation);
			}
		}
		if (toJoin.length > 0) {
			return AnnotationMap.merge(annotationMap, toJoin[0]);
		}
		return annotationMap;
	}

	public static BeanPropertyDefinition createBeanPropertyDefinition(final MongoPersistentProperty persistentProperty, final boolean setter) {
		return new SimpleBeanPropertyDefinition(setter ? createAnnotatedSetter(persistentProperty) : createAnnotatedField(persistentProperty),
				persistentProperty.getFieldName());
	}

	public static JavaType createJavaType(final MongoPersistentProperty persistentProperty) {
		return createJavaType(persistentProperty.getTypeInformation());
	}

	public static JavaType createJavaType(final TypeInformation<?> typeInformation) {
		if (typeInformation.isCollectionLike() || typeInformation.isMap()) {
			if (typeInformation.isCollectionLike()) {
				if (typeInformation.getType().isArray()) {
					return ArrayType.construct(createJavaType(typeInformation.getComponentType()), null, null);
				} else {
					return CollectionType.construct(typeInformation.getType(), createJavaType(typeInformation.getComponentType()));
				}
			} else {
				return MapType.construct(typeInformation.getType(), createJavaType(typeInformation.getComponentType()),
						createJavaType(typeInformation.getMapValueType()));
			}
		} else {
			return SimpleType.construct(typeInformation.getType());
		}
	}
}
