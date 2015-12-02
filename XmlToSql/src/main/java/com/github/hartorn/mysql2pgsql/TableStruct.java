/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bazire
 *
 */
public final class TableStruct {

	final private Map<String, ColumnStruct> columns = new HashMap<>();
	final private String tableName;

	public TableStruct(final String tableName) {
		this.tableName = tableName;
	}

	public void addColumn(final ColumnStruct column) {
		this.columns.put(column.getColumnName().toLowerCase().trim(), column);
	}

	/**
	 * Getter for the db type.
	 *
	 * @return the type
	 */
	public DbTypesMapping getDbType(final String columnName) {
		return this.columns.get(columnName).getDbType();
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
	 */
	public void writeSql(final Writer writer) {

	}

}
