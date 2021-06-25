package edu.uci.ics.texera.dataflow.source.file;

import com.itextpdf.text.*;
import com.itextpdf.text.pdf.PdfWriter;

import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.storage.utils.StorageUtils;
import junit.framework.Assert;

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by junm5 on 5/31/17.
 */
public class FileSourcePptPdfTest {

    private static Path specialFiles = Paths.get("./index/test_tables/filesource/specialfiles");
    private static Path pdfPath = specialFiles.resolve("test.pdf");
    private static Path pptPath = specialFiles.resolve("test.ppt");
    private static Path wordPath = Utils.getResourcePath("test.docx", TexeraProject.TEXERA_DATAFLOW);

    @Before
    public void setUp() throws Exception {
        Files.createDirectories(specialFiles);
        createPDF(pdfPath.toString());
        createPPT(pptPath.toString());
    }

    @AfterClass
    public static void cleanUp() throws StorageException {
        //delete temp folder
        if (Files.exists(specialFiles)) {
            StorageUtils.deleteDirectory(specialFiles.toString());
        }
    }

    @Test
    public void extractPDFFile() throws Exception {
        String file = FileExtractorUtils.extractPDFFile(pdfPath);
        assertThat(file, is("test\n"));
    }

    @Test
    public void extractPPTFile() throws Exception {
        Path pdfFile = Paths.get("./index/test_tables/filesource/specialfiles/test.ppt");
        String file = FileExtractorUtils.extractPPTFile(pdfFile);
        assertTrue(file.isEmpty());
    }
    
    @Test
    public void extractDocsFile() throws Exception {
        String content = FileExtractorUtils.extractWordFile(wordPath);
        Assert.assertEquals("Test Microsoft Word Documents", content.trim());
    }

    private static void createPDF(String path) throws Exception {
        Document pdfDoc = new Document(PageSize.A4);
        PdfWriter.getInstance(pdfDoc, new FileOutputStream(path)).setPdfVersion(PdfWriter.VERSION_1_7);
        pdfDoc.open();
        Font myfont = new Font();
        myfont.setStyle(Font.NORMAL);
        myfont.setSize(11);
        Paragraph para = new Paragraph("test", myfont);
        para.setAlignment(Element.ALIGN_JUSTIFIED);
        pdfDoc.add(para);
        pdfDoc.close();
    }

    private static void createPPT(String path) throws IOException {
        //creating a new empty slide show
        XMLSlideShow ppt = new XMLSlideShow();
        //creating an FileOutputStream object
        File file = new File(path);
        FileOutputStream out = new FileOutputStream(file);
        //saving the changes to a file
        ppt.write(out);
        out.close();
        ppt.close();
    }

}