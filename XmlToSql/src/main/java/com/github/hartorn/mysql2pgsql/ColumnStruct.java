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
	// final String defaultValue;

	/**
	 * Constructor.
	 *
	 * @param xmlField
	 *            Attributes map from sax parse
	 * @throws SAXException
	 *             Exception when getting values
	 */
	public ColumnStruct(final Attributes xmlField) throws SAXException {
		this.columnName = xmlField.getValue(xmlField.getIndex("Field"));
		final String mySqlType = xmlField.getValue(xmlField.getIndex("Type"));
		this.type = DbTypesMapping.getMappingFromMySqlType(mySqlType);
		this.pgType = this.type.getPostgreSqlType(mySqlType);
		this.isNotNull = "NO".equals(xmlField.getValue(xmlField.getIndex("Null")));
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
}
