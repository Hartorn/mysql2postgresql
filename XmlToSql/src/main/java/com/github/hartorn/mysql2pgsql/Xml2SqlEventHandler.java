/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Bazire
 *
 */
public class Xml2SqlEventHandler extends DefaultHandler {
	protected enum DbTypes {
		VARCHAR("character varying"),

		BIT("bit"),

		BIGINT("bigint"),

		CHAR("character"),

		SMALLINT("smallint"),

		INT("integer"), REAL("real"),

		TIMESTAMP("timestamp without time zone"),

		TEXT("text");

		public static DbTypes getEnumEltByDbType(final String dbType) throws SAXException {
			for (final DbTypes type : DbTypes.values()) {
				if (type.dbType.equals(dbType)) {
					return type;
				}
			}
			throw new SAXException("Missing enum type for db type:" + dbType);
		}

		private final String dbType;

		private DbTypes(final String type) {
			this.dbType = type;
		}

		public String formatForSql(final String value) throws SAXException {
			String formattedValue;
			switch (this) {
			case BIT:
				formattedValue = value + "::bit";
				break;
			case VARCHAR:
			case CHAR:
			case TEXT:
			case TIMESTAMP:
				formattedValue = "'" + value.replaceAll("'", "''") + "'";
				break;
			case SMALLINT:
			case BIGINT:
			case INT:
			case REAL:
				formattedValue = value;
				break;
			default:
				throw new SAXException("Missing enum type:" + name());
			}
			return formattedValue;
		}
	}

	final private Properties dbMapping;
	final private String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2});\n";
	final private Writer writer;
	final private Map<String, String> attributesMap = new HashMap<>();
	private String tableName = null;
	private String fieldName = null;

	public Xml2SqlEventHandler(final Writer writer) throws IOException {
		this.writer = writer;

		final Properties prop = new Properties();
		try (InputStream input = getClass().getResourceAsStream("columnMapping.properties");
				Reader inputReader = new InputStreamReader(input, XmlToSql.CHARSET);) {
			prop.load(inputReader);
		}
		this.dbMapping = prop;
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) throws SAXException {
		if (this.fieldName != null) {
			this.attributesMap.put(this.fieldName, String.copyValueOf(ch, start, length));
		}
	}

	@Override
	public void endDocument() throws SAXException {
		try {
			endSql();
		} catch (final IOException e) {
			throw new SAXException(e);
		}
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName) throws SAXException {
		switch (localName) {
		case "table_data":
			this.tableName = null;
			break;
		case "row":
			try {
				writeSql();
			} catch (final IOException e) {
				throw new SAXException(e);
			}
			this.attributesMap.clear();
			break;
		case "field":
			this.fieldName = null;
			break;
		default:
			break;
		}

	}

	private void endSql() throws IOException {
		this.writer.write("\n\ncommit;");
	}

	private String prepareStringValueForSql(final String attrName, final String value) throws SAXException {
		// System.out.println("Clef:" + this.tableName + "." + attrName);
		// System.out.println("Property:" + this.dbMapping.getProperty(this.tableName + "." + attrName));
		// System.out.println();
		final DbTypes type = DbTypes
				.getEnumEltByDbType(this.dbMapping.getProperty((this.tableName + "." + attrName).toLowerCase()));
		return type.formatForSql(value);
	}

	@Override
	public void startDocument() throws SAXException {
		try {
			startSql();
		} catch (final IOException e) {
			throw new SAXException(e);
		}
	}

	@Override
	public void startElement(final String uri, final String localName, final String qName, final Attributes attributes)
			throws SAXException {
		switch (localName) {
		case "table_data":
			this.tableName = attributes.getValue(attributes.getIndex("name"));
			break;
		case "field":
			this.fieldName = attributes.getValue(attributes.getIndex("name"));
			break;
		default:
			break;
		}
	}

	private void startSql() throws IOException {
		this.writer.write("SET CLIENT_ENCODING TO '" + XmlToSql.CHARSET + "';\n\n");
		this.writer.write("start transaction;\n\n");
	}

	private void writeSql() throws IOException, SAXException {
		final StringBuilder attrNames = new StringBuilder();
		final StringBuilder attrValues = new StringBuilder();

		final Set<Entry<String, String>> row = this.attributesMap.entrySet();
		for (final Entry<String, String> entry : row) {
			attrNames.append(entry.getKey()).append(',');
		}
		if (attrNames.length() > 0) {
			attrNames.setLength(attrNames.length() - 1);
		}

		for (final Entry<String, String> entry : row) {
			attrValues.append(prepareStringValueForSql(entry.getKey(), entry.getValue())).append(',');
		}
		if (attrValues.length() > 0) {
			attrValues.setLength(attrValues.length() - 1);
		}

		this.writer.write(
				MessageFormat.format(this.SQL_TEMPLATE, this.tableName, attrNames.toString(), attrValues.toString()));
	}
}
