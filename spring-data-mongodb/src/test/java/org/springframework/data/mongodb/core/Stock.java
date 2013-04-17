package org.springframework.data.mongodb.core;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.math.BigInteger;

@Document
public class Stock {

	@Id
	BigInteger id;

	BigDecimal price;

	@DBRef
	Product product;

	String otherField;


}
