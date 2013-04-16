package org.springframework.data.mongodb.core.convert.jackson;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.JsonReadContext;
import com.fasterxml.jackson.core.type.TypeReference;
import de.undercouch.bson4jackson.BsonConstants;
import de.undercouch.bson4jackson.BsonParser;
import de.undercouch.bson4jackson.io.*;
import de.undercouch.bson4jackson.types.JavaScript;
import de.undercouch.bson4jackson.types.ObjectId;
import de.undercouch.bson4jackson.types.Symbol;
import de.undercouch.bson4jackson.types.Timestamp;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class MongoBsonParser extends BsonParser {

	/**
	 * The features for this parser
	 */
	private final int _bsonFeatures;

	/**
	 * The input stream to read from
	 */
	private LittleEndianInputStream _in;

	/**
	 * Counts the number of bytes read from {@link #_in}
	 */
	private CountingInputStream _counter;

	/**
	 * The raw input stream passed in
	 */
	private final InputStream _rawInputStream;

	/**
	 * True if the parser has been closed
	 */
	private boolean _closed;

	/**
	 * The ObjectCodec used to parse the Bson object(s)
	 */
	private ObjectCodec _codec;

	/**
	 * The position of the current token
	 */
	private int _tokenPos;

	/**
	 * The current parser state
	 */
	private Context _currentContext;

	/**
	 * Constructs a new parser
	 * 
	 * @param ctxt
	 *            the Jackson IO context
	 * @param jsonFeatures
	 *            bit flag composed of bits that indicate which {@link com.fasterxml.jackson.core.JsonParser.Feature}s are enabled.
	 * @param bsonFeatures
	 *            bit flag composed of bits that indicate which {@link Feature}s are enabled.
	 * @param in
	 *            the input stream to parse.
	 */
	public MongoBsonParser(final IOContext ctxt, final int jsonFeatures, final int bsonFeatures, InputStream in) {
		super(ctxt, jsonFeatures, bsonFeatures, new ByteArrayInputStream(new byte[0]));
		_bsonFeatures = bsonFeatures;
		_rawInputStream = in;
		// only initialize streams here if document length isn't going to be honored
		if (!isEnabled(Feature.HONOR_DOCUMENT_LENGTH)) {
			if (!(in instanceof BufferedInputStream)) {
				in = new StaticBufferedInputStream(in);
			}
			_counter = new CountingInputStream(in);
			_in = new LittleEndianInputStream(_counter);
		}
	}

	@Override
	public void close() throws IOException {
		if (isEnabled(JsonParser.Feature.AUTO_CLOSE_SOURCE)) {
			_in.close();
		}
		_closed = true;
	}

	@Override
	public BigInteger getBigIntegerValue() throws IOException,
			JsonParseException {
		Number n = getNumberValue();
		if (n == null) {
			return null;
		}
		if ((n instanceof Byte) || (n instanceof Integer) ||
				(n instanceof Long) || (n instanceof Short)) {
			return BigInteger.valueOf(n.longValue());
		} else if ((n instanceof Double) || (n instanceof Float)) {
			return BigDecimal.valueOf(n.doubleValue()).toBigInteger();
		}
		return new BigInteger(n.toString());
	}

	@Override
	public byte[] getBinaryValue(final Base64Variant b64variant) throws IOException,
			JsonParseException {
		return getText().getBytes();
	}

	@Override
	public ObjectCodec getCodec() {
		return _codec;
	}

	/**
	 * @return the context of the current element
	 * @throws java.io.IOException
	 *             if there is no context
	 */
	public Context getCurrentContext() throws IOException {
		if (_currentContext == null) {
			throw new IOException("Context unknown");
		}
		return _currentContext;
	}

	@Override
	public JsonLocation getCurrentLocation() {
		return new BsonLocation(_in, _counter.getPosition());
	}

	@Override
	public String getCurrentName() throws IOException, JsonParseException {
		if (_currentContext == null) {
			return null;
		}
		return _currentContext.fieldName;
	}

	@Override
	public BigDecimal getDecimalValue() throws IOException, JsonParseException {
		Number n = getNumberValue();
		if (n == null) {
			return null;
		}
		if ((n instanceof Byte) || (n instanceof Integer) ||
				(n instanceof Long) || (n instanceof Short)) {
			return BigDecimal.valueOf(n.longValue());
		} else if ((n instanceof Double) || (n instanceof Float)) {
			return BigDecimal.valueOf(n.doubleValue());
		}
		return new BigDecimal(n.toString());
	}

	@Override
	public double getDoubleValue() throws IOException, JsonParseException {
		return ((Number) getCurrentContext().value).doubleValue();
	}

	@Override
	public Object getEmbeddedObject() throws IOException, JsonParseException {
		return (_currentContext != null ? _currentContext.value : null);
	}

	@Override
	public float getFloatValue() throws IOException, JsonParseException {
		return ((Number) getCurrentContext().value).floatValue();
	}

	@Override
	public int getIntValue() throws IOException, JsonParseException {
		return ((Number) getCurrentContext().value).intValue();
	}

	@Override
	public long getLongValue() throws IOException, JsonParseException {
		return ((Number) getCurrentContext().value).longValue();
	}

	@Override
	public JsonParser.NumberType getNumberType() throws IOException, JsonParseException {
		if (_currentContext == null) {
			return null;
		}
		if (_currentContext.value instanceof Integer) {
			return NumberType.INT;
		} else if (_currentContext.value instanceof Long) {
			return NumberType.LONG;
		} else if (_currentContext.value instanceof BigInteger) {
			return NumberType.BIG_INTEGER;
		} else if (_currentContext.value instanceof Float) {
			return NumberType.FLOAT;
		} else if (_currentContext.value instanceof Double) {
			return NumberType.DOUBLE;
		} else if (_currentContext.value instanceof BigDecimal) {
			return NumberType.BIG_DECIMAL;
		}
		return null;
	}

	@Override
	public Number getNumberValue() throws IOException, JsonParseException {
		return (Number) getCurrentContext().value;
	}

	@Override
	public JsonReadContext getParsingContext() {
		// this parser does not use JsonStreamContext
		return null;
	}

	@Override
	public String getText() throws IOException, JsonParseException {
		if ((_currentContext == null) || (_currentContext.state == State.FIELDNAME)) {
			return null;
		}
		if (_currentContext.state == State.VALUE) {
			return _currentContext.fieldName;
		}
		return String.valueOf(_currentContext.value);
	}

	@Override
	public char[] getTextCharacters() throws IOException, JsonParseException {
		// not very efficient; that's why hasTextCharacters()
		// always returns false
		return getText().toCharArray();
	}

	@Override
	public int getTextLength() throws IOException, JsonParseException {
		return getText().length();
	}

	@Override
	public int getTextOffset() throws IOException, JsonParseException {
		return 0;
	}

	@Override
	public JsonLocation getTokenLocation() {
		return new BsonLocation(_in, _tokenPos);
	}

	@Override
	public boolean hasTextCharacters() {
		// getTextCharacters is obviously not the most efficient way
		return false;
	}

	@Override
	public boolean isClosed() {
		return _closed;
	}

	@Override
	public JsonToken nextToken() throws IOException, JsonParseException {
		Context ctx = _currentContext;
		if ((_currToken == null) && (ctx == null)) {
			try {
				_currToken = handleNewDocument(false);
			} catch (EOFException e) {
				// there is nothing more to read. indicate EOF
				return null;
			}
		} else {
			_tokenPos = _counter.getPosition();
			if (ctx == null) {
				if (_currToken == JsonToken.END_OBJECT) {
					// end of input
					return null;
				}
				throw new JsonParseException("Found element outside the document",
						getTokenLocation());
			}

			if (ctx.state == State.DONE) {
				// next field
				ctx.reset();
			}

			boolean readValue = true;
			if (ctx.state == State.FIELDNAME) {
				readValue = false;
				while (true) {
					// read field name or end of document
					ctx.type = _in.readByte();
					if (ctx.type == BsonConstants.TYPE_END) {
						// end of document
						_currToken = (ctx.array ? JsonToken.END_ARRAY : JsonToken.END_OBJECT);
						_currentContext = _currentContext.parent;
					} else if (ctx.type == BsonConstants.TYPE_UNDEFINED) {
						// skip field name and then ignore this token
						skipCString();
						continue;
					} else {
						ctx.state = State.VALUE;
						_currToken = JsonToken.FIELD_NAME;

						if (ctx.array) {
							// immediately read value of array element (discard field name)
							readValue = true;
							skipCString();
							ctx.fieldName = null;
						} else {
							// read field name
							ctx.fieldName = readCString();
						}
					}
					break;
				}
			}

			if (readValue) {
				// parse element's value
				switch (ctx.type) {
				case BsonConstants.TYPE_DOUBLE:
					ctx.value = _in.readDouble();
					_currToken = JsonToken.VALUE_NUMBER_FLOAT;
					break;

				case BsonConstants.TYPE_STRING:
					ctx.value = readString();
					_currToken = JsonToken.VALUE_STRING;
					break;

				case BsonConstants.TYPE_DOCUMENT:
					_currToken = handleNewDocument(false);
					break;

				case BsonConstants.TYPE_ARRAY:
					_currToken = handleNewDocument(true);
					break;

				case BsonConstants.TYPE_BINARY:
					_currToken = handleBinary();
					break;

				case BsonConstants.TYPE_OBJECTID:
					ctx.value = readObjectId();
					_currToken = JsonToken.VALUE_EMBEDDED_OBJECT;
					break;

				case BsonConstants.TYPE_BOOLEAN:
					boolean b = _in.readBoolean();
					ctx.value = b;
					_currToken = (b ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE);
					break;

				case BsonConstants.TYPE_DATETIME:
					ctx.value = new Date(_in.readLong());
					_currToken = JsonToken.VALUE_EMBEDDED_OBJECT;
					break;

				case BsonConstants.TYPE_NULL:
					_currToken = JsonToken.VALUE_NULL;
					break;

				case BsonConstants.TYPE_REGEX:
					_currToken = handleRegEx();
					break;

				case BsonConstants.TYPE_DBPOINTER:
					_currToken = handleDBPointer();
					break;

				case BsonConstants.TYPE_JAVASCRIPT:
					ctx.value = new JavaScript(readString());
					_currToken = JsonToken.VALUE_EMBEDDED_OBJECT;
					break;

				case BsonConstants.TYPE_SYMBOL:
					ctx.value = readSymbol();
					_currToken = JsonToken.VALUE_EMBEDDED_OBJECT;
					break;

				case BsonConstants.TYPE_JAVASCRIPT_WITH_SCOPE:
					_currToken = handleJavascriptWithScope();
					break;

				case BsonConstants.TYPE_INT32:
					ctx.value = _in.readInt();
					_currToken = JsonToken.VALUE_NUMBER_INT;
					break;

				case BsonConstants.TYPE_TIMESTAMP:
					ctx.value = readTimestamp();
					_currToken = JsonToken.VALUE_EMBEDDED_OBJECT;
					break;

				case BsonConstants.TYPE_INT64:
					ctx.value = _in.readLong();
					_currToken = JsonToken.VALUE_NUMBER_INT;
					break;

				case BsonConstants.TYPE_MINKEY:
					ctx.value = "MinKey";
					_currToken = JsonToken.VALUE_STRING;
					break;

				case BsonConstants.TYPE_MAXKEY:
					ctx.value = "MaxKey";
					_currToken = JsonToken.VALUE_STRING;
					break;

				default:
					throw new JsonParseException("Unknown element type " + ctx.type,
							getTokenLocation());
				}
				ctx.state = State.DONE;
			}
		}
		return _currToken;
	}

	@Override
	public void setCodec(final ObjectCodec c) {
		_codec = c;
	}

	@Override
	protected void _closeInput() throws IOException {
		_rawInputStream.close();
	}

	@Override
	protected void _finishString() throws IOException, JsonParseException {
		// Not used
	}

	@Override
	protected void _handleEOF() throws JsonParseException {
		_reportInvalidEOF();
	}

	/**
	 * Reads binary data from the input stream
	 *
	 * @return the json token read
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected JsonToken handleBinary() throws IOException {
		int size = _in.readInt();
		byte subtype = _in.readByte();
		Context ctx = getCurrentContext();
		switch (subtype) {
		case BsonConstants.SUBTYPE_BINARY_OLD:
			int size2 = _in.readInt();
			byte[] buf2 = new byte[size2];
			_in.readFully(buf2);
			ctx.value = buf2;
			break;

		case BsonConstants.SUBTYPE_UUID:
			long l1 = _in.readLong();
			long l2 = _in.readLong();
			ctx.value = new UUID(l1, l2);
			break;

		default:
			byte[] buf = new byte[size];
			_in.readFully(buf);
			ctx.value = buf;
			break;
		}

		return JsonToken.VALUE_EMBEDDED_OBJECT;
	}

	/**
	 * Reads a DBPointer from the stream
	 *
	 * @return the json token read
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected JsonToken handleDBPointer() throws IOException {
		Map<String, Object> pointer = new LinkedHashMap<String, Object>();
		pointer.put("$ns", readString());
		pointer.put("$id", readObjectId());
		getCurrentContext().value = pointer;
		return JsonToken.VALUE_EMBEDDED_OBJECT;
	}

	/**
	 * Can be called when embedded javascript code with scope is found. Reads the code and the embedded document.
	 *
	 * @return the json token read
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected JsonToken handleJavascriptWithScope() throws IOException {
		// skip size
		_in.readInt();
		String code = readString();
		Map<String, Object> doc = readDocument();
		getCurrentContext().value = new JavaScript(code, doc);
		return JsonToken.VALUE_EMBEDDED_OBJECT;
	}

	/**
	 * Can be called when a new embedded document is found. Reads the document's header and creates a new context on the stack.
	 *
	 * @param array
	 *            true if the document is an embedded array
	 * @return the json token read
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected JsonToken handleNewDocument(final boolean array) throws IOException {
		if (_in == null) {
			// this means Feature.HONOR_DOCUMENT_LENGTH is enabled, and we
			// haven't yet started reading. Read the first int to find out the
			// length of the document.
			byte[] buf = new byte[Integer.SIZE / Byte.SIZE];
			int len = 0;
			while (len < buf.length) {
				int l = _rawInputStream.read(buf, len, buf.length - len);
				if (l == -1) {
					throw new IOException("Not enough bytes for length of document");
				}
				len += l;
			}

			// wrap the input stream by a bounded stream, subtract buf.length from the
			// length because the size itself is included in the length
			int documentLength = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
			InputStream in = new BoundedInputStream(_rawInputStream, documentLength - buf.length);

			// buffer if the raw input stream is not already buffered
			if (!(_rawInputStream instanceof BufferedInputStream)) {
				in = new StaticBufferedInputStream(in);
			}
			_counter = new CountingInputStream(in);
			_in = new LittleEndianInputStream(_counter);
		} else {
			// read document header (skip size, we're not interested)
			_in.readInt();
		}

		_currentContext = new Context(_currentContext, array);
		return (array ? JsonToken.START_ARRAY : JsonToken.START_OBJECT);
	}

	/**
	 * Reads and compiles a regular expression
	 *
	 * @return the json token read
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected JsonToken handleRegEx() throws IOException {
		String regex = readCString();
		String pattern = readCString();
		getCurrentContext().value = Pattern.compile(regex, regexStrToFlags(pattern));
		return JsonToken.VALUE_EMBEDDED_OBJECT;
	}

	/**
	 * Checks if a generator feature is enabled
	 *
	 * @param f
	 *            the feature
	 * @return true if the given feature is enabled
	 */
	@Override
	protected boolean isEnabled(final Feature f) {
		return (_bsonFeatures & f.getMask()) != 0;
	}

	@Override
	protected boolean loadMore() throws IOException {
		// We don't actually use this
		return true;
	}

	/**
	 * @return a null-terminated string read from the input stream
	 * @throws java.io.IOException
	 *             if the string could not be read
	 */
	@Override
	protected String readCString() throws IOException {
		return _in.readUTF(-1);
	}

	/**
	 * Fully reads an embedded document, reusing this parser
	 *
	 * @return the parsed document
	 * @throws java.io.IOException
	 *             if the document could not be read
	 */
	@Override
	protected Map<String, Object> readDocument() throws IOException {
		ObjectCodec codec = getCodec();
		if (codec == null) {
			throw new IllegalStateException("Could not parse embedded document " +
					"because BSON parser has no codec");
		}
		_currToken = handleNewDocument(false);
		return codec.readValue(this, new TypeReference<Map<String, Object>>() {
		});
	}

	/**
	 * Reads a ObjectID from the input stream
	 *
	 * @return the ObjectID
	 * @throws java.io.IOException
	 *             if the ObjectID could not be read
	 */
	@Override
	protected ObjectId readObjectId() throws IOException {
		int time = ByteOrderUtil.flip(_in.readInt());
		int machine = ByteOrderUtil.flip(_in.readInt());
		int inc = ByteOrderUtil.flip(_in.readInt());
		return new ObjectId(time, machine, inc);
	}

	/**
	 * Reads a string that consists of a integer denoting the number of bytes, the bytes (including a terminating 0 byte)
	 *
	 * @return the string
	 * @throws java.io.IOException
	 *             if the string could not be read
	 */
	@Override
	protected String readString() throws IOException {
		// read number of bytes
		int bytes = _in.readInt();
		if (bytes <= 0) {
			throw new IOException("Invalid number of string bytes");
		}
		String s;
		if (bytes > 1) {
			s = _in.readUTF(bytes - 1);
		} else {
			s = "";
		}
		// read terminating zero
		_in.readByte();
		return s;
	}

	/**
	 * Reads a symbol object from the input stream
	 *
	 * @return the symbol
	 * @throws java.io.IOException
	 *             if the symbol could not be read
	 */
	@Override
	protected Symbol readSymbol() throws IOException {
		return new Symbol(readString());
	}

	/**
	 * Reads a timestamp object from the input stream
	 *
	 * @return the timestamp
	 * @throws java.io.IOException
	 *             if the timestamp could not be read
	 */
	@Override
	protected Timestamp readTimestamp() throws IOException {
		int inc = _in.readInt();
		int time = _in.readInt();
		return new Timestamp(time, inc);
	}

	/**
	 * Converts a BSON regex pattern string to a combined value of Java flags that can be used in {@link java.util.regex.Pattern#compile(String, int)}
	 *
	 * @param pattern
	 *            the regex pattern string
	 * @return the Java flags
	 * @throws JsonParseException
	 *             if the pattern string contains a unsupported flag
	 */
	@Override
	protected int regexStrToFlags(final String pattern) throws JsonParseException {
		int flags = 0;
		for (int i = 0; i < pattern.length(); ++i) {
			char c = pattern.charAt(i);
			switch (c) {
			case 'i':
				flags |= Pattern.CASE_INSENSITIVE;
				break;

			case 'm':
				flags |= Pattern.MULTILINE;
				break;

			case 's':
				flags |= Pattern.DOTALL;
				break;

			case 'u':
				flags |= Pattern.UNICODE_CASE;
				break;

			case 'l':
			case 'x':
				// unsupported
				break;

			default:
				throw new JsonParseException("Invalid regex", getTokenLocation());
			}
		}
		return flags;
	}

	/**
	 * Skips over a null-terminated string in the input stream
	 *
	 * @throws java.io.IOException
	 *             if an I/O error occurs
	 */
	@Override
	protected void skipCString() throws IOException {
		while (_in.readByte() != 0)
			;
	}

	/**
	 * Information about the element currently begin parsed
	 */
	public static class Context {
		/**
		 * The parent context (may be null if the context is the top-level one)
		 */
		private final Context parent;

		/**
		 * True if the document currently being parsed is an array
		 */
		private final boolean array;

		/**
		 * The bson type of the current element
		 */
		private byte type;

		/**
		 * The field name of the current element
		 */
		private String fieldName;

		/**
		 * The value of the current element
		 */
		private Object value;

		/**
		 * The parsing state of the current token
		 */
		State state = State.FIELDNAME;

		public Context(final Context parent, final boolean array) {
			this.parent = parent;
			this.array = array;
		}

		public String getFieldName() {
			return fieldName;
		}

		public Context getParent() {
			return parent;
		}

		public void reset() {
			type = 0;
			fieldName = null;
			value = null;
			state = State.FIELDNAME;
		}
	}

	/**
	 * Extends {@link JsonLocation} to offer a specialized string representation
	 */
	private static class BsonLocation extends JsonLocation {
		private static final long serialVersionUID = -5441597278886285168L;

		public BsonLocation(final Object srcRef, final long totalBytes) {
			super(srcRef, totalBytes, -1, -1, -1);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder(80);
			sb.append("[Source: ");
			if (getSourceRef() == null) {
				sb.append("UNKNOWN");
			} else {
				sb.append(getSourceRef().toString());
			}
			sb.append("; pos: ");
			sb.append(getByteOffset());
			sb.append(']');
			return sb.toString();
		}
	}

	/**
	 * Specifies what the parser is currently parsing (field name or value) or if it is done with the current element
	 */
	private enum State {
		FIELDNAME,
		VALUE,
		DONE
	}

}
