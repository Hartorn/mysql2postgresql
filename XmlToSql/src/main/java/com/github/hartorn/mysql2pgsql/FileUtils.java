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

/**
 * Class to handle file manipulations.
 *
 * @author Hartorn
 *
 */
public enum FileUtils {
    ;
    private static final int BUFFER_SIZE = 1024 * 8;

    /**
     * This method ensures that the output String has only valid XML unicode characters as specified by the XML 1.0
     * standard. For reference, please see <a href="http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char">the standard</a>
     * . This method will return an empty String if the input is null or empty.
     *
     * @param in
     *            The String whose non-valid characters we want to remove.
     * @return The in String, stripped of non-valid characters.
     */
    private static String stripNonValidXmlCharacters(final String in) {
        final StringBuffer out = new StringBuffer(); // Used to hold the output.
        char current; // Used to reference the current character.

        if ((in == null) || ("".equals(in))) {
            return ""; // vacancy test.
        }
        for (int i = 0; i < in.length(); i++) {
            current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
            if ((current == 0x9) || (current == 0xA) || (current == 0xD) || ((current >= 0x20) && (current <= 0xD7FF))
                    || ((current >= 0xE000) && (current <= 0xFFFD)) || ((current >= 0x10000) && (current <= 0x10FFFF))) {
                out.append(current);
            }
        }
        return out.toString();
    }

    /**
     * Copy the source file to the destination, filtering the non-valid xml characters.
     *
     * @param filename
     *            the destination filename
     * @return the new file
     * @throws IOException
     *             Exception when reading or writing the file.
     */
    public static File copyToCorrectedFile(final String filename) throws IOException {
        final File xmlFile = new File(filename);
        final File xmlFileCorrected = new File(filename + ".corrected");
        if (xmlFileCorrected.exists()) {
            xmlFileCorrected.createNewFile();
        }
        final char[] buffer = new char[FileUtils.BUFFER_SIZE];
        int nbChars;

        try (final OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(xmlFileCorrected));
                final InputStream inputStream = new BufferedInputStream(new FileInputStream(xmlFile), FileUtils.BUFFER_SIZE);
                final Reader reader = new InputStreamReader(inputStream, XmlToSql.CHARSET);
                final Writer writer = new OutputStreamWriter(outputStream, XmlToSql.CHARSET);) {
            while ((nbChars = reader.read(buffer)) != -1) {
                writer.write(FileUtils.stripNonValidXmlCharacters(String.copyValueOf(buffer, 0, nbChars)));
                writer.flush();
            }
        }
        return xmlFileCorrected;
    }

    /**
     * Build a new file input stream, buffered.
     * 
     * @param inputFile
     *            the input file
     * @return the new input stream
     * @throws IOException
     *             Exception when creating the file input stream
     */
    public static InputStream buildBufferedInputStream(final File inputFile) throws IOException {
        return new BufferedInputStream(new FileInputStream(inputFile), FileUtils.BUFFER_SIZE);
    }

}
