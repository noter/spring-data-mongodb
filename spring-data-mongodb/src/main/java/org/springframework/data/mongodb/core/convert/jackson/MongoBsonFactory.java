package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonParser;

import java.io.IOException;
import java.io.InputStream;

public class MongoBsonFactory extends BsonFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected BsonParser _createJsonParser(final InputStream in, final IOContext ctxt) throws IOException, JsonParseException {
		MongoBsonParser p = new MongoBsonParser(ctxt, _parserFeatures, _bsonParserFeatures, in);
		ObjectCodec codec = getCodec();
		if (codec != null) {
			p.setCodec(codec);
		}
		return p;
	}

}
