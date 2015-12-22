package com.github.hartorn.mysql2pgsql;

import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.SAXException;

/**
 * Enumeration of MySQL datatypes (used to convert to PostgresSQL, to format).
 *
 * @author Bazire
 *
 */
public enum DbTypesMapping {
    BIT("BIT", "BIT({0})", false, true),

    TINYINT("TINYINT", "SMALLINT", true, true),

    BOOL("BOOL", "BOOLEAN", false, false),

    BOOLEAN("BOOLEAN", "BOOLEAN", false, false),

    SMALLINT("SMALLINT", "SMALLINT", true, true, false, "INTEGER"),

    MEDIUMINT("MEDIUMINT", "INT", true, true),

    INT("INT", "INT", true, true, false, "BIGINT"),

    INTEGER("INTEGER", "INT", true, true, false, "BIGINT"),

    BIGINT("BIGINT", "BIGINT", true, true, false, "NUMERIC(20,0)"),

    DECIMAL("DECIMAL", "NUMERIC({0},{1})", true, true, true, null),

    DEC("DEC", "NUMERIC({0},{1})", true, true, true, null),

    FIXED("FIXED", "NUMERIC({0},{1})", true, true, true, null),

    FLOAT("FLOAT", "REAL", true, true, true, null),

    DOUBLE("FLOAT", "DOUBLE PRECISION", true, true, true, null),

    DOUBLE_PRECISION("DOUBLE_PRECISION", "DOUBLE PRECISION", true, true, true, null),

    DATE("DATE", "DATE"),

    TIME("TIME", "TIME"),

    DATETIME("DATETIME", "TIMESTAMP"),

    TIMESTAMP("TIMESTAMP", "TIMESTAMP"),

    CHAR("CHAR", "CHARACTER({0})", false, true),

    VARCHAR("VARCHAR", "CHARACTER VARYING({0})", false, true),

    TINYTEXT("TINYTEXT", "CHARACTER VARYING(255)"),

    MEDIUMTEXT("MEDIUMTEXT", "TEXT"),

    TEXT("TEXT", "TEXT"),

    LONGTEXT("LONGTEXT", "TEXT"),

    BINARY("BINARY", "BYTEA", false, true),

    VARBINARY("VARBINARY", "BYTEA", false, true)

    ;

    /**
     * Pattern to match UNSIGNED (avec ou sans espace, sans casse).
     */
    private static final Pattern UNSIGNED_REGEX = Pattern.compile("\\s*UNSIGNED\\s*", Pattern.CASE_INSENSITIVE);
    /**
     * Pattern to match (length).
     */
    private static final Pattern LENGTH_REGEX = Pattern.compile("\\s*\\((\\s*\\d+\\s*)\\)\\s*", Pattern.CASE_INSENSITIVE);
    /**
     * Pattern to match (length, precision).
     */
    private static final Pattern PRECISION_REGEX = Pattern.compile("\\s*\\((\\s*\\d+\\s*),(\\s*\\d+\\s*)\\)\\s*", Pattern.CASE_INSENSITIVE);

    private static final String QUOTE = "'";

    /**
     * Return the element of the enum, corresponding to the given MySQL type.
     *
     * @param mySqlType
     *            the MySQL type
     * @return the correponding element of the enum
     * @throws SAXException
     *             Exception if the MySQL type is not handled
     */
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

    private final String pgTypeUnsigned;

    private final StringBuilder forConcat = new StringBuilder();
    private final boolean canBeUnsigned;
    private final boolean canHaveLength;

    private final boolean canHavePrecision;

    private DbTypesMapping(final String mySqlType, final String pgType) {
        this(mySqlType, pgType, false, false);
    }

    private DbTypesMapping(final String mySqlType, final String pgType, final boolean canBeUnsigned, final boolean canHaveLength) {
        this(mySqlType, pgType, canBeUnsigned, canHaveLength, false, null);
    }

    private DbTypesMapping(final String mySqlType, final String pgType, final boolean canBeUnsigned, final boolean canHaveLength,
            final boolean canHavePrecision, final String pgTypeUnsigned) {
        this.mySqlType = mySqlType;
        this.pgType = pgType;
        this.pgTypeUnsigned = pgTypeUnsigned;
        this.canBeUnsigned = canBeUnsigned;
        this.canHaveLength = canHaveLength;
        this.canHavePrecision = canHavePrecision;
    }

    private String applyAndTrim(final String value, final Pattern regex) {
        return regex.matcher(value).replaceAll("").trim();
    }

    private final String concat(final String... toConcat) {
        this.forConcat.setLength(0);
        if ((toConcat != null) && (toConcat.length > 0)) {
            for (final String toAdd : toConcat) {
                this.forConcat.append(toAdd);
            }
        }
        return this.forConcat.toString();
    }

    private String emptyIfNull(final String value) {
        return value != null ? value : "";
    }

    /**
     * Method returning the null value for this database type ("" for string, null for integer, etc).
     *
     * @return the string with the null value for this type.
     * @throws SAXException
     *             If this enum element is not handled for this method.
     */
    public String nullValueForSql() throws SAXException {
        String formattedValue;
        switch (this) {
            case BIT:
                formattedValue = concat("B", DbTypesMapping.QUOTE, "", DbTypesMapping.QUOTE);
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:

            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:

                formattedValue = concat(DbTypesMapping.QUOTE, "", DbTypesMapping.QUOTE);
                break;

            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
            case INT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
            case DEC:
            case FIXED:
            case DOUBLE:
            case DOUBLE_PRECISION:

                // case REAL:
                formattedValue = "null";
                break;

            case BINARY:
            case VARBINARY:
                formattedValue = concat("E", DbTypesMapping.QUOTE, "", DbTypesMapping.QUOTE, "::bytea");
                break;

            case BOOL:
            case BOOLEAN:
                formattedValue = "null";
                break;

            default:
                throw new SAXException("Missing enum type:" + name());
        }
        return formattedValue;
    }

    /**
     * Format the given string value for this datatype (excaping quote, casting, etc...).
     *
     * @param value
     *            the value to format
     * @return the formatted value
     * @throws SAXException
     *             If this enum element is not handled for this method.
     */
    public String formatForSql(final String value) throws SAXException {
        String formattedValue;
        switch (this) {
            case BIT:
                formattedValue = concat("B", DbTypesMapping.QUOTE, value, DbTypesMapping.QUOTE);
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:

            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:

                formattedValue = concat(DbTypesMapping.QUOTE, emptyIfNull(value).replaceAll("'", "''"), DbTypesMapping.QUOTE);
                break;

            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
            case INT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
            case DEC:
            case FIXED:
            case DOUBLE:
            case DOUBLE_PRECISION:

                // case REAL:
                formattedValue = value;
                break;

            case BINARY:
            case VARBINARY:
                formattedValue = concat("E", DbTypesMapping.QUOTE, value, DbTypesMapping.QUOTE, "::bytea");
                break;

            case BOOL:
            case BOOLEAN:
                formattedValue = value;
                break;

            default:
                throw new SAXException("Missing enum type:" + name());
        }
        return formattedValue;
    }

    /**
     * Build the PostgreSql database type from the MySql one.
     *
     * @param mySqlType
     *            the MySql type
     * @return the pg Type
     */
    public String getPostgreSqlType(final String mySqlType) {
        final String baseFormat;
        // Choose the base pg type to use
        if (this.canBeUnsigned && DbTypesMapping.UNSIGNED_REGEX.matcher(mySqlType).find()) {
            baseFormat = this.pgTypeUnsigned != null ? this.pgTypeUnsigned : this.pgType;
        } else {
            baseFormat = this.pgType;
        }

        String result = baseFormat;

        // Apply length and precision if needed
        if (this.canHaveLength && this.canHavePrecision) {
            final Matcher matcher = DbTypesMapping.PRECISION_REGEX.matcher(mySqlType);
            String length = "";
            String precision = "";
            if (matcher.find()) {
                length = matcher.group(1).trim();
                precision = matcher.group(2).trim();
            }
            result = MessageFormat.format(baseFormat, length, precision);
        } else if (this.canHaveLength) {
            final Matcher matcher = DbTypesMapping.LENGTH_REGEX.matcher(mySqlType);
            String length = "";
            if (matcher.find()) {
                length = matcher.group(1).trim();
            }
            result = MessageFormat.format(baseFormat, length);
        }
        return result;
    }

    private boolean isMatching(final String mySqlType) {
        String value = emptyIfNull(mySqlType);
        // Removing unsigned part
        if (this.canBeUnsigned) {
            value = applyAndTrim(value, DbTypesMapping.UNSIGNED_REGEX);
        }
        // Removing length or precision part
        if (this.canHaveLength && this.canHavePrecision) {
            value = applyAndTrim(value, DbTypesMapping.PRECISION_REGEX);
        } else if (this.canHaveLength) {
            value = applyAndTrim(value, DbTypesMapping.LENGTH_REGEX);
        }

        return Pattern.compile(this.mySqlType, Pattern.CASE_INSENSITIVE).matcher(value).matches();
    }

}
