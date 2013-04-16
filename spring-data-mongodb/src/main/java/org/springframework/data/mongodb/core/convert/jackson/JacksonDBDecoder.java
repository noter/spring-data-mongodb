package org.springframework.data.mongodb.core.convert.jackson;

import com.mongodb.*;
import org.bson.BSONCallback;
import org.bson.BSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class JacksonDBDecoder implements DBDecoder {

    private final MongoObjectMapper objectMapper;

    private QueryContext queryContext;

    public JacksonDBDecoder(final MongoObjectMapper objectMapper, QueryContext queryContext) {
        this.objectMapper = objectMapper;

        this.queryContext = queryContext;
    }


    public int decode(final byte[] b, final BSONCallback callback) {
        return 0;
    }


    public DBObject decode(final byte[] b, final DBCollection collection) {
        try {
            return decode(new ByteArrayInputStream(b), collection);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public int decode(final InputStream in, final BSONCallback callback) throws IOException {
        return 0;
    }


    public DBObject decode(final InputStream in, final DBCollection collection) throws IOException {
        BasicDBObject basicDBObject = new BasicDBObject(1);
        basicDBObject.put(JacksonMappingMongoConverter.OBJECT_KEY, objectMapper.readValue(in,Object.class));
        return basicDBObject;
    }


    public BSONObject readObject(final byte[] b) {
        try {
            return readObject(new ByteArrayInputStream(b));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public BSONObject readObject(final InputStream in) throws IOException {
        return decode(in, (DBCollection) null);
    }


    public DBCallback getDBCallback(final DBCollection collection) {
        return null;
    }

}
