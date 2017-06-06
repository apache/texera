package edu.uci.ics.textdb.exp.source.file;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.microsoft.OfficeParser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by junm5 on 5/3/17.
 */
public class TextExtractor {

    public static String extractCommonFile(Path path) {
        if (path == null) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * use pdfbox to extract data from pdf document.
     * This also can be done using Tika lib.
     *
     * @param path
     * @return
     */
    public static String extractPDFFile(Path path) {
        if (path == null) {
            return null;
        }
        PDDocument doc = null;
        try {
            doc = PDDocument.load(new File(path.toString()));
            return new PDFTextStripper().getText(doc);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (doc != null) {
                try {
                    doc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * use Tika to extract data from PPT file
     * attention: tika may conflict with poi
     * @param path
     * @return
     */
    public static String extractPPTFile(Path path) {
        if (path == null) {
            return null;
        }
        try {
            File file = new File(path.toString());
            OfficeParser autoDetectParser = new OfficeParser();
            ContentHandler handler = new WriteOutContentHandler(new StringWriter());
            autoDetectParser.parse(new FileInputStream(file), handler, new Metadata(), new ParseContext());
            return handler.toString();
        } catch (Exception exp) {
            exp.printStackTrace();
        }
        return null;

    }

}
