package com.github.hartorn.mysql2pgsql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Main class to handle the main method : copying, parsing the XML and writing SQL files.
 *
 * @author Bazire
 *
 */
public class XmlToSql {
    private static final Logger LOG = LogManager.getLogger(XmlToSql.class);
    public static final String CHARSET = "ISO-8859-1";

    /**
     * Main method.
     *
     * @param args
     *            arguments
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

        XmlToSql.LOG.info("Starting the copy of corrected files (deleting non-valid XML characters)");
        try {
            xmlFileStructCorrected = FileUtils.copyToCorrectedFile(filenameStruct);
            xmlFileDataCorrected = FileUtils.copyToCorrectedFile(filenameData);

        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        XmlToSql.LOG.info("End of the copy of corrected files");

        Xml2SqlStructEventHandler xml2sqlStruct = new Xml2SqlStructEventHandler();

        // Parse and construct the db structure, and write the sql file
        XmlToSql.LOG.info("Starting the parse of the XML dump (structure)");

        try {
            xml2sqlStruct = XmlToSql.<Xml2SqlStructEventHandler> parseAndWriteToOutput(xmlFileStructCorrected, sqlFileStruct, XmlToSql.CHARSET, true,
                    null);
        } catch (IOException | SAXException | ParserConfigurationException e) {
            XmlToSql.LOG.info("Failure of the parsing of the XML dump (structure)");
            e.printStackTrace();
            System.exit(1);
        }
        XmlToSql.LOG.info("End of the parsing of the XML dump (structure)");

        XmlToSql.LOG.info("Starting the parse of the XML dump (data)");
        // Write the sql file from the XML data dump
        try {
            XmlToSql.parseAndWriteToOutput(xmlFileDataCorrected, sqlFileData, XmlToSql.CHARSET, false, xml2sqlStruct.getTableMap());
        } catch (IOException | SAXException | ParserConfigurationException e) {
            XmlToSql.LOG.info("Failure of the parsing of the XML dump (data)");
            e.printStackTrace();
            System.exit(1);
        }
        XmlToSql.LOG.info("End of the parse of the XML dump (data)");
    }

    private static DefaultHandler buildHandler(final boolean forStruct, final Writer writer, final Map<String, TableStruct> tableMap) {
        if (forStruct) {
            return new Xml2SqlStructEventHandler();
        }
        return new Xml2SqlDataEventHandler(writer, tableMap);
    }

    @SuppressWarnings("unchecked")
    private static <D extends DefaultHandler> D parseAndWriteToOutput(final File inputFile, final File outputFile, final String charset,
            final boolean forStruct, final Map<String, TableStruct> dbMapping) throws IOException, SAXException, ParserConfigurationException {
        try (final InputStream xmlStream = FileUtils.buildBufferedInputStream(inputFile);
                final Reader xmlReader = new InputStreamReader(xmlStream, charset);
                final OutputStream outputStream = new FileOutputStream(outputFile);
                final Writer writer = new OutputStreamWriter(outputStream, charset);) {

            final InputSource is = new InputSource(xmlReader);
            is.setEncoding(charset);
            final SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            final SAXParser saxParser = spf.newSAXParser();
            final DefaultHandler handler = XmlToSql.buildHandler(forStruct, writer, dbMapping);
            saxParser.parse(is, handler);
            return (D) handler;
        }
    }

    private static void usage() {
        System.err.println("Usage: XmlToSql <file.xml>");
        System.err.println("       -usage or -help = this message");
        System.exit(1);
    }
}
