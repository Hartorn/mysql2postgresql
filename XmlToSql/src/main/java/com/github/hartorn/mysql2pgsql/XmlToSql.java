/**
 *
 */
package com.github.hartorn.mysql2pgsql;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author Bazire
 *
 */
public class XmlToSql {
	private static final int BUFFER_SIZE = 1024 * 8;
	private static final Logger LOG = LogManager.getLogger(XmlToSql.class);
	public static final String CHARSET = "ISO-8859-1";

	private static File copyToCorrectedFile(final String filename) throws IOException {
		final File xmlFile = new File(filename);
		final File xmlFileCorrected = new File(filename + ".corrected");
		if (xmlFileCorrected.exists()) {
			xmlFileCorrected.createNewFile();
		}
		final char[] buffer = new char[XmlToSql.BUFFER_SIZE];
		int nbChars;

		try (final OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(xmlFileCorrected));
				final InputStream inputStream = new BufferedInputStream(new FileInputStream(xmlFile),
						XmlToSql.BUFFER_SIZE);
				final Reader reader = new InputStreamReader(inputStream, XmlToSql.CHARSET);
				final Writer writer = new OutputStreamWriter(outputStream, XmlToSql.CHARSET);) {
			while ((nbChars = reader.read(buffer)) != -1) {
				writer.write(XmlToSql.stripNonValidXMLCharacters(String.copyValueOf(buffer, 0, nbChars)));
			}
		}
		return xmlFileCorrected;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		String filenameStruct = null;
		String filenameData = null;

		if (args.length != 2) {
			XmlToSql.usage();
		} else {
			filenameStruct = args[0];
			filenameData = args[1];
		}
		final File sqlFileStruct = new File(filenameStruct + "Struct.sql");
		final File sqlFileData = new File(filenameData + "Data.sql");

		File xmlFileStructCorrected = null;
		File xmlFileDataCorrected = null;

		XmlToSql.LOG.info("Starting the copy of corrected file (deleting non-valid XML characters");
		try {
			xmlFileStructCorrected = XmlToSql.copyToCorrectedFile(filenameStruct);
			xmlFileDataCorrected = XmlToSql.copyToCorrectedFile(filenameData);

		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		XmlToSql.LOG.info("End of the copy of corrected file");

		final Xml2SqlStructEventHandler xml2sqlStruct = new Xml2SqlStructEventHandler();

		// Parse and construct the db structure, and write the sql file
		XmlToSql.LOG.info("Starting the parse of the XML dump (structure)");
		try (final InputStream xmlStream = new BufferedInputStream(new FileInputStream(xmlFileStructCorrected),
				XmlToSql.BUFFER_SIZE);
				final Reader xmlReader = new InputStreamReader(xmlStream, XmlToSql.CHARSET);
				final OutputStream outputStream = new FileOutputStream(sqlFileStruct);
				final Writer writer = new OutputStreamWriter(outputStream, XmlToSql.CHARSET);) {

			final InputSource is = new InputSource(xmlReader);
			is.setEncoding(XmlToSql.CHARSET);
			final SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setNamespaceAware(true);
			final SAXParser saxParser = spf.newSAXParser();
			saxParser.parse(is, xml2sqlStruct);
			xml2sqlStruct.writeSql(writer);
		} catch (final IOException | SAXException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		XmlToSql.LOG.info("End of the parsing of the XML dump (structure)");
		XmlToSql.LOG.info("Starting the parse of the XML dump (data)");

		// // Write the sql file from the XML data dump
		try (final InputStream xmlStream = new BufferedInputStream(new FileInputStream(xmlFileDataCorrected),
				XmlToSql.BUFFER_SIZE);
				final Reader xmlReader = new InputStreamReader(xmlStream, XmlToSql.CHARSET);
				final OutputStream outputStream = new FileOutputStream(sqlFileData);
				final Writer writer = new OutputStreamWriter(outputStream, XmlToSql.CHARSET);) {

			final InputSource is = new InputSource(xmlReader);
			is.setEncoding(XmlToSql.CHARSET);
			final SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setNamespaceAware(true);
			final SAXParser saxParser = spf.newSAXParser();
			saxParser.parse(is, new Xml2SqlDataEventHandler(writer, xml2sqlStruct.getTableMap()));
		} catch (final IOException | SAXException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		XmlToSql.LOG.info("End of the parse of the XML dump (data)");
	}

	/**
	 * This method ensures that the output String has only
	 * valid XML unicode characters as specified by the
	 * XML 1.0 standard. For reference, please see
	 * <a href="http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char">the
	 * standard</a>. This method will return an empty
	 * String if the input is null or empty.
	 *
	 * @param in
	 *            The String whose non-valid characters we want to remove.
	 * @return The in String, stripped of non-valid characters.
	 */
	private static String stripNonValidXMLCharacters(final String in) {
		final StringBuffer out = new StringBuffer(); // Used to hold the output.
		char current; // Used to reference the current character.

		if ((in == null) || ("".equals(in))) {
			return ""; // vacancy test.
		}
		for (int i = 0; i < in.length(); i++) {
			current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
			if ((current == 0x9) || (current == 0xA) || (current == 0xD) || ((current >= 0x20) && (current <= 0xD7FF))
					|| ((current >= 0xE000) && (current <= 0xFFFD))
					|| ((current >= 0x10000) && (current <= 0x10FFFF))) {
				out.append(current);
			}
		}
		return out.toString();
	}

	private static void usage() {
		System.err.println("Usage: XmlToSql <file.xml>");
		System.err.println("       -usage or -help = this message");
		System.exit(1);
	}
}
