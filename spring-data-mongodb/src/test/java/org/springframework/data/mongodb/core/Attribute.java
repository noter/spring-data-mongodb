package org.springframework.data.mongodb.core;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigInteger;

@Document
public class Attribute {
	@Id
	BigInteger id;
	String name;
	String otherField;
}
