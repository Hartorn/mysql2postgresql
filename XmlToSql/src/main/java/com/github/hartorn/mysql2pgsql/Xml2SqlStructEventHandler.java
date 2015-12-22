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
 * Class used to convert XML database structure to SQL String. Class extending a DefaultHandler for a SaxParser, to use
 * to render a string to add this row (data row) to a SQL database.
 *
 * @author Bazire
 *
 */
public class Xml2SqlStructEventHandler extends DefaultHandler {

    private static final SortedMap<String, TableStruct> TABLE_MAP = new TreeMap<>();

    private TableStruct currentTable = null;

    @Override
    public void endElement(final String uri, final String localName, final String otherName) throws SAXException {
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
        return Collections.unmodifiableMap(Xml2SqlStructEventHandler.TABLE_MAP);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void startElement(final String uri, final String localName, final String oterName, final Attributes attributes) throws SAXException {
        switch (localName) {
            case "table_structure":
                final String tableName = attributes.getValue(attributes.getIndex("name")).toLowerCase().trim();
                final TableStruct newTable = new TableStruct(tableName);
                this.currentTable = newTable;
                Xml2SqlStructEventHandler.TABLE_MAP.put(tableName, newTable);
                break;
            case "field":
                this.currentTable.addColumn(new ColumnStruct(attributes));
                break;
            case "key":
                this.currentTable.addKey(attributes);
                // <key Table="actions" Non_unique="0" Key_name="PRIMARY"
                break;
            default:
                break;
        }
    }

    /**
     * Method to write the SQL structure as a string.
     *
     * @param writer
     *            the writer to use to write the SQL string.
     * @throws IOException
     *             IOException when writing
     */
    public void writeSql(final Writer writer) throws IOException {
        writer.write("SET CLIENT_ENCODING TO '" + XmlToSql.CHARSET + "';\n\n");
        writer.write("start transaction;\n\n");

        for (final TableStruct table : Xml2SqlStructEventHandler.TABLE_MAP.values()) {
            table.writeSql(writer);
        }
        writer.write("\n\ncommit;");
    }
}
