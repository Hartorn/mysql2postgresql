/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * @author Bazire
 *
 */
public class ColumnStruct {

	final private String columnName;
	final private String pgType;

	final private DbTypesMapping type;
	final private boolean isNotNull;
	final private String defaultValue;

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
		this.defaultValue = (defaultVal == null) || "".equals(defaultVal.trim()) ? null : defaultVal;
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
	 * @return the defaultValue
	 * @throws SAXException
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
