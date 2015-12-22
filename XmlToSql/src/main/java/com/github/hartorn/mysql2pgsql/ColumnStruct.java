package com.github.hartorn.mysql2pgsql;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * Class describing the structure of a column in database.
 *
 * @author Bazire
 *
 */
public class ColumnStruct {

    private final String columnName;
    private final String pgType;

    private final DbTypesMapping type;
    private final boolean isNotNull;
    private final String defaultValue;

    /**
     * Constructor.
     *
     * @param xmlField
     *            Attributes map from sax parse
     * @throws SAXException
     *             Exception when getting values
     */
    public ColumnStruct(final Attributes xmlField) throws SAXException {
        this.columnName = xmlField.getValue("Field");
        final String mySqlType = xmlField.getValue("Type");
        this.type = DbTypesMapping.getMappingFromMySqlType(mySqlType);
        this.pgType = this.type.getPostgreSqlType(mySqlType);
        this.isNotNull = "NO".equals(xmlField.getValue("Null"));
        final String defaultVal = xmlField.getValue("Default");
        this.defaultValue = (defaultVal == null) ? null : defaultVal.trim();
    }

    /**
     * Getter for columnName.
     *
     * @return the column name
     */
    public String getColumnName() {
        return this.columnName;
    }

    /**
     * Getter for the db type.
     *
     * @return the type
     */
    public DbTypesMapping getDbType() {
        return this.type;
    }

    /**
     * Method returning the default value for this column, based on the MySQL default value.
     *
     * @return the defaultValue
     * @throws SAXException
     *             If the datatype is not handled for default value.
     */
    public String getDefaultValue() throws SAXException {
        if (DbTypesMapping.TIMESTAMP.equals(this.type) || DbTypesMapping.DATETIME.equals(this.type)) {
            if ("CURRENT_TIMESTAMP".equals(this.defaultValue)) {
                return this.defaultValue;
            } else if ("0000-00-00 00:00:00".equals(this.defaultValue)) {
                return null;
            }
        }
        return this.defaultValue != null ? this.type.formatForSql(this.defaultValue) : null;
    }

    /**
     *
     * @return the null String (NULL OR NOT NULL).
     */
    public String getNullString() {
        return this.isNotNull ? "NOT NULL" : "NULL";
    }

    /**
     * Getter for the pgType.
     *
     * @return the pgType
     */
    public String getPgType() {
        return this.pgType;
    }

}
