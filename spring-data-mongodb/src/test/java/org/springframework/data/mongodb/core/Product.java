package org.springframework.data.mongodb.core;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

@Document
public class Product {
	@Id
	BigInteger id;
	@DBRef
	Map<BigInteger, Stock> stocks;
	String name;
	String otherField;
	@DBRef
	List<Attribute> attributes;


}
