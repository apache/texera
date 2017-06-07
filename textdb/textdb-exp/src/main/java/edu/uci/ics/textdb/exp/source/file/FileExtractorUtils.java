package edu.uci.ics.textdb.exp.source.file;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import edu.uci.ics.textdb.api.exception.DataFlowException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by junm5 on 5/3/17.
 */
public class FileExtractorUtils {

    /**
     * Extracts data as plain text file.
     * 
     * @param path
     * @return
     * @throws DataFlowException
     */
    public static String extractPlainTextFile(Path path) throws DataFlowException {
        try {
            return new String(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new DataFlowException(e);
        }
    }

    /**
     * Extracts data from PDF document using pdfbox.
     *
     * @param path
     * @return
     * @throws DataFlowException
     */
    public static String extractPDFFile(Path path) throws DataFlowException {
        try (PDDocument doc = PDDocument.load(new File(path.toString()))) {
            return new PDFTextStripper().getText(doc);
        } catch (IOException e) {
            throw new DataFlowException(e);
        }
    }

    /**
     * Extracts data from PPT/PPTX from using poi.
     *
     * @param path
     * @return
     * @throws DataFlowException
     */
    public static String extractPPTFile(Path path) throws DataFlowException {
        try (FileInputStream inputStream = new FileInputStream(path.toString());
                XMLSlideShow ppt = new XMLSlideShow(inputStream)) {
            StringBuffer res = new StringBuffer();
            for (XSLFSlide slide : ppt.getSlides()) {
                List<XSLFShape> shapes = slide.getShapes();
                for (XSLFShape shape : shapes) {
                    if (shape instanceof XSLFTextShape) {
                        XSLFTextShape textShape = (XSLFTextShape) shape;
                        String text = textShape.getText();
                        res.append(text);
                    }
                }
            }
            return res.toString();
        } catch (IOException e) {
            throw new DataFlowException(e);
        }
    }

}
