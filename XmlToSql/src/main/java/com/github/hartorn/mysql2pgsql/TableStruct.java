package com.github.hartorn.mysql2pgsql;

import java.io.IOException;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * Class describing the structure of a table in database.
 *
 * @author Bazire
 *
 */
public final class TableStruct {

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE {0} (\n{1}{2}\n);\n\n";
    private static final String PK_CK_TEMPLATE = "\t\tCONSTRAINT {0} PRIMARY KEY ({1})";

    private final SortedMap<String, ColumnStruct> columns = new TreeMap<>();
    private final SortedSet<String> primaryKeys = new TreeSet<>();
    private final String tableName;

    public TableStruct(final String tableName) {
        this.tableName = tableName;
    }

    public void addColumn(final ColumnStruct column) {
        this.columns.put(column.getColumnName().toLowerCase().trim(), column);
    }

    /**
     * Adding a constraint (key), to the table (Handle only primary key at the moment).
     *
     * @param xmlKey
     *            the xml attributes of the SQL
     */
    public void addKey(final Attributes xmlKey) {
        switch (xmlKey.getValue("Key_name")) {
            case "PRIMARY":
                final String columnName = xmlKey.getValue("Column_name").toLowerCase().trim();
                this.primaryKeys.add(columnName);
                break;
            default:
                break;
        }
    }

    /**
     * Getter for the db type.
     *
     * @return the type
     */
    public DbTypesMapping getDbType(final String columnName) {
        return this.columns.get(columnName).getDbType();
    }

    private String getPrimaryKeyCk() {
        if (this.primaryKeys.isEmpty()) {
            return null;
        }
        final StringBuilder pkConcat = new StringBuilder();
        for (final String value : this.primaryKeys) {
            pkConcat.append(value).append(',');
        }
        pkConcat.setLength(pkConcat.length() - 1);

        return MessageFormat.format(TableStruct.PK_CK_TEMPLATE, "pk_" + getTableName(), pkConcat);
    }

    /**
     * Getter for the table name.
     *
     * @return table name
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Write the sql to create the table.
     *
     * @param writer
     *            the writer
     * @throws IOException
     *             Exception when writing SQL
     */
    public void writeSql(final Writer writer) throws IOException {
        final StringBuilder columnsSql = new StringBuilder();
        for (final ColumnStruct column : this.columns.values()) {
            columnsSql.append('\t').append(column.getColumnName()).append(' ');
            columnsSql.append(column.getPgType()).append(' ');
            columnsSql.append(column.getNullString());
            try {
                if (column.getDefaultValue() != null) {
                    columnsSql.append(" DEFAULT ").append(column.getDefaultValue());
                }
            } catch (final SAXException e) {
                throw new IOException(e);
            }
            columnsSql.append(',').append('\n');
        }

        final String primaryCk = getPrimaryKeyCk();

        if ((columnsSql.length() > 0) && (primaryCk == null)) {
            columnsSql.setLength(columnsSql.length() - 2);
        }

        writer.write(
                MessageFormat.format(TableStruct.CREATE_TABLE_TEMPLATE, this.tableName, columnsSql.toString(), primaryCk == null ? "" : primaryCk));

    }

}
