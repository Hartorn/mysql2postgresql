/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import org.xml.sax.SAXException;

/**
 * @author Bazire
 *
 */
public enum DbTypesMapping {
	BIT("BIT", "BIT", false, true),

	TINYINT("TINYINT", "SMALLINT", true, true),

	BOOL("BOOL", "BOOLEAN", false, false),

	BOOLEAN("BOOLEAN", "BOOLEAN", false, false),

	SMALLINT("SMALLINT", "SMALLINT", true, true, false, "INTEGER"),

	MEDIUMINT("MEDIUMINT", "INT", true, true),

	INT("INT", "INT", true, true, false, "BIGINT"),

	INTEGER("INTEGER", "INT", true, true, false, "BIGINT"),

	BIGINT("BIGINT", "BIGINT", true, true, false, "NUMERIC(20,0)"),

	DECIMAL("DECIMAL", "NUMERIC", true, true, true, null),

	DEC("DEC", "NUMERIC", true, true, true, null),

	FIXED("FIXED", "NUMERIC", true, true, true, null),

	FLOAT("FLOAT", "REAL", true, true, true, null),

	DOUBLE("FLOAT", "DOUBLE PRECISION", true, true, true, null),

	DOUBLE_PRECISION("DOUBLE_PRECISION", "DOUBLE PRECISION", true, true, true, null),

	DATE("DATE", "DATE"),

	TIME("TIME", "TIME"),

	DATETIME("DATETIME", "TIMESTAMP"),

	TIMESTAMP("TIMESTAMP", "TIMESTAMP"),

	CHAR("CHAR", "CHARACTER", false, true),

	VARCHAR("VARCHAR", "CHARACTER VARYING", false, true),

	TEXT("TEXT", "TEXT"),

	BINARY("BINARY", "BYTEA", false, true),

	VARBINARY("VARBINARY", "BYTEA", false, true)

	;

	public static DbTypesMapping getMappingFromMySqlType(final String mySqlType) throws SAXException {
		for (final DbTypesMapping dbType : DbTypesMapping.values()) {
			if (dbType.isMatching(mySqlType)) {
				return dbType;
			}
		}
		throw new SAXException("Database type not handled :" + mySqlType);
	}

	private final String mySqlType;
	private final String pgType;

	private final String pgTypeUnsigneg;
	private final boolean canBeUnsigned;
	private final boolean canHaveLength;

	private final boolean canHavePrecision;

	private DbTypesMapping(final String mySqlType, final String pgType) {
		this(mySqlType, pgType, false, false);
	}

	private DbTypesMapping(final String mySqlType, final String pgType, final boolean canBeUnsigned,
			final boolean canHaveLength) {
		this(mySqlType, pgType, canBeUnsigned, canHaveLength, false, null);
	}

	private DbTypesMapping(final String mySqlType, final String pgType, final boolean canBeUnsigned,
			final boolean canHaveLength, final boolean canHavePrecision, final String pgTypeUnsigned) {
		this.mySqlType = mySqlType;
		this.pgType = pgType;
		this.pgTypeUnsigneg = pgTypeUnsigned;
		this.canBeUnsigned = canBeUnsigned;
		this.canHaveLength = canHaveLength;
		this.canHavePrecision = canHavePrecision;
	}

	/**
	 * Build the PostgreSql database type from the MySql one.
	 *
	 * @param mySqlType
	 *            the MySql type
	 * @return the pg Type
	 */
	public String getPostgreSqlType(final String mySqlType) {
		// TODO add code

		return null;
	}

	private boolean isMatching(final String mySqlType) {
		// TODO add code
		return false;
	}
}
