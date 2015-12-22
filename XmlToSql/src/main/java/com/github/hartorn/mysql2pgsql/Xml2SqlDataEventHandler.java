package com.github.hartorn.mysql2pgsql;

import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Class used to convert data Row in XML to SQL String. Class extending a DefaultHandler for a SaxParser, to use to
 * render a string to add this row (data row) to a SQL database.
 *
 * @author Bazire
 *
 */
public class Xml2SqlDataEventHandler extends DefaultHandler {

    private static final Logger LOG = LogManager.getLogger(Xml2SqlDataEventHandler.class);
    private static final String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2});\n";

    private final Writer writer;
    private final Map<String, TableStruct> dbProperties;
    private final Map<String, String> attributesMap = new HashMap<>();

    private TableStruct currentTable = null;
    private String fieldName = null;
    private long nbElts;

    public Xml2SqlDataEventHandler(final Writer writer, final Map<String, TableStruct> tableMap) {
        this.writer = writer;
        this.dbProperties = tableMap;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        if (this.fieldName != null) {
            this.attributesMap.put(this.fieldName, String.copyValueOf(ch, start, length));
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void endDocument() throws SAXException {
        try {
            endSql();
        } catch (final IOException e) {
            throw new SAXException(e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void endElement(final String uri, final String localName, final String otherName) throws SAXException {
        switch (localName) {
            case "table_data":
                Xml2SqlDataEventHandler.LOG.info("Finished SQL for table :\"{}\" Nb lines : {}", this.currentTable.getTableName(), this.nbElts);
                this.currentTable = null;
                this.nbElts = 0;
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
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void startDocument() throws SAXException {
        try {
            startSql();
        } catch (final IOException e) {
            throw new SAXException(e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void startElement(final String uri, final String localName, final String otherName, final Attributes attributes) throws SAXException {
        switch (localName) {
            case "row":
                this.nbElts++;
                break;
            case "table_data":
                this.currentTable = this.dbProperties.get(attributes.getValue(attributes.getIndex("name")).toLowerCase().trim());
                this.nbElts = 0;
                Xml2SqlDataEventHandler.LOG.info("Writing SQL for table :\"{}\"", this.currentTable.getTableName());
                break;
            case "field":
                this.fieldName = attributes.getValue(attributes.getIndex("name"));
                if (this.fieldName != null) {
                    this.attributesMap.put(this.fieldName, this.currentTable.getDbType(this.fieldName.toLowerCase().trim()).nullValueForSql());
                }
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

        this.writer.write(MessageFormat.format(Xml2SqlDataEventHandler.SQL_TEMPLATE, this.currentTable.getTableName(), attrNames.toString(),
                attrValues.toString()));
    }
}
