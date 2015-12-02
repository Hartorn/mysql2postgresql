/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Bazire
 *
 */
public class Xml2SqlStructEventHandler extends DefaultHandler {

	final private SortedMap<String, TableStruct> tableMap = new TreeMap<>();

	private TableStruct currentTable = null;

	@Override
	public void endElement(final String uri, final String localName, final String qName) throws SAXException {
		switch (localName) {
		case "table_structure":
			this.currentTable = null;
			break;
		default:
			break;
		}

	}

	/**
	 * Getter of tableMap attribute.
	 *
	 * @return the map tableName -> TableStruct
	 */
	public Map<String, TableStruct> getTableMap() {
		return Collections.unmodifiableMap(this.tableMap);
	}

	@Override
	public void startElement(final String uri, final String localName, final String qName, final Attributes attributes)
			throws SAXException {
		switch (localName) {
		case "table_structure":
			final String tableName = attributes.getValue(attributes.getIndex("name")).toLowerCase().trim();
			final TableStruct newTable = new TableStruct(tableName);
			this.currentTable = newTable;
			this.tableMap.put(tableName, newTable);
			break;
		case "field":
			this.currentTable.addColumn(new ColumnStruct(attributes));
			break;
		default:
			break;
		}
	}

	public void writeSql(final Writer writer) throws IOException {
		writer.write("SET CLIENT_ENCODING TO '" + XmlToSql.CHARSET + "';\n\n");
		writer.write("start transaction;\n\n");

		for (final TableStruct table : this.tableMap.values()) {
			table.writeSql(writer);
		}

		writer.write("\n\ncommit;");
	}

	// private void startSql() throws IOException {
	// this.writer.write("SET CLIENT_ENCODING TO '" + XmlToSql.CHARSET + "';\n\n");
	// this.writer.write("start transaction;\n\n");
	// }
	//
	// private void writeSql() throws IOException, SAXException {
	// final StringBuilder attrNames = new StringBuilder();
	// final StringBuilder attrValues = new StringBuilder();
	//
	// final Set<Entry<String, String>> row = this.attributesMap.entrySet();
	// for (final Entry<String, String> entry : row) {
	// attrNames.append(entry.getKey()).append(',');
	// }
	// if (attrNames.length() > 0) {
	// attrNames.setLength(attrNames.length() - 1);
	// }
	//
	// for (final Entry<String, String> entry : row) {
	// attrValues.append(prepareStringValueForSql(entry.getKey(), entry.getValue())).append(',');
	// }
	// if (attrValues.length() > 0) {
	// attrValues.setLength(attrValues.length() - 1);
	// }
	//
	// this.writer.write(
	// MessageFormat.format(this.SQL_TEMPLATE, this.tableName, attrNames.toString(), attrValues.toString()));
	// }
	//
	// private void endSql() throws IOException {
	// this.writer.write("\n\ncommit;");
	// }

}
