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

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author Bazire
 *
 */
public class XmlToSql {
	private static final int BUFFER_SIZE = 1024 * 8;
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
		String filename = null;

		for (int i = 0; i < args.length; i++) {
			filename = args[i];
			if (i != (args.length - 1)) {
				XmlToSql.usage();
			}
		}

		if (filename == null) {
			XmlToSql.usage();
		}

		final File sqlFile = new File(filename + ".sql");

		File xmlFileCorrected = null;
		try {
			xmlFileCorrected = XmlToSql.copyToCorrectedFile(filename);
		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try (final InputStream xmlStream = new BufferedInputStream(new FileInputStream(xmlFileCorrected),
				XmlToSql.BUFFER_SIZE);
				final Reader xmlReader = new InputStreamReader(xmlStream, XmlToSql.CHARSET);
				final OutputStream outputStream = new FileOutputStream(sqlFile);
				final Writer writer = new OutputStreamWriter(outputStream, XmlToSql.CHARSET);) {

			final InputSource is = new InputSource(xmlReader);
			is.setEncoding(XmlToSql.CHARSET);
			final SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setNamespaceAware(true);
			final SAXParser saxParser = spf.newSAXParser();
			saxParser.parse(is, new Xml2SqlEventHandler(writer));

		} catch (final IOException | SAXException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
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
