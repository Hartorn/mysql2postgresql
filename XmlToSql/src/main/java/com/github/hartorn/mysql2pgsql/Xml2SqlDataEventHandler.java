/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Bazire
 *
 */
public class Xml2SqlDataEventHandler extends DefaultHandler {

	final private String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2});\n";
	final private Writer writer;
	final private Map<String, TableStruct> dbProperties;
	final private Map<String, String> attributesMap = new HashMap<>();
	private TableStruct currentTable = null;
	private String fieldName = null;

	public Xml2SqlDataEventHandler(final Writer writer, final Map<String, TableStruct> tableMap) throws IOException {
		this.writer = writer;
		this.dbProperties = tableMap;
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
			this.currentTable = null;
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
		final DbTypesMapping dbType = this.currentTable.getDbType(attrName.toLowerCase().trim());
		return dbType.formatForSql(value);
		// Converters<T,U> MySQL Type-> Pg Type ? TODO

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
			this.currentTable = this.dbProperties
					.get(attributes.getValue(attributes.getIndex("name")).toLowerCase().trim());
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

		this.writer.write(MessageFormat.format(this.SQL_TEMPLATE, this.currentTable.getTableName(),
				attrNames.toString(), attrValues.toString()));
	}
}
