package edu.uci.ics.texera.dataflow.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * FileSourceOperator reads a file or files under a directory and converts one file to one tuple.
 * 
 * The filePath in the predicate must be 1) a text file or 2) a directory
 * 
 * In case of a directory, FileSourceOperator supports recursively reading files 
 *   and specifying a max recursive depth.
 * 
 * The files must have one of the supported extensions: {@code supportedExtensions}
 * 
 * FileSourceOperator reads all content of one file and convert them to one tuple.
 *   The tuple will have one column, the attributeName as defined in {@code FileSourcePredicate},
 *   with the attributeType as TEXT.
 *   
 * In case of a directory, if the directory doesn't contain any file that 
 *   matches the allowed extensions, then an exception will be thrown.
 * 
 * @author Zuozhi Wang
 * @author Jun Ma
 */
public class FileSourceOperator implements ISourceOperator {
    
    private final FileSourcePredicate predicate;
    // output schema of this file source operator
    private final Schema outputSchema;
    
    // a list of files, each of which is a valid text file
    private List<Path> pathList;
    private Iterator<Path> pathIterator;
    
    // cursor indicating the current position
    private Integer cursor = CLOSED;

    
    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
        this.outputSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE,
                new Attribute(predicate.getAttributeName(), AttributeType.TEXT));

        this.pathList = new ArrayList<>();
        
        Path filePath = Paths.get(predicate.getFilePath());
        if (! Files.exists(filePath)) {
            throw new TexeraException(String.format("file %s doesn't exist", filePath));
        }
        
        if (Files.isDirectory(filePath)) {
            try {
                if (this.predicate.isRecursive()) {
                    pathList.addAll(Files.walk(filePath, this.predicate.getMaxDepth()).collect(Collectors.toList()));
                } else {
                    pathList.addAll(Files.list(filePath).collect(Collectors.toList()));
                }
                
            } catch (IOException e) {
                throw new TexeraException(String.format(
                        "opening directory %s failed: " + e.getMessage(), filePath));
            }
        } else {
            pathList.add(filePath);
        }
                
        // filter directories, files starting with ".", 
        //   and files that don't end with allowedExtensions
        this.pathList = pathList.stream()
            .filter(path -> ! Files.isDirectory(path))
            .filter(path -> ! path.getFileName().startsWith("."))
            .collect(Collectors.toList());
        
        // check if the path list is empty
        if (pathList.isEmpty()) {
            throw new TexeraException(String.format(
                    "the filePath: %s doesn't contain any files. ", filePath));
        } 
        pathIterator = pathList.iterator();
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED || cursor >= pathList.size()) {
            return null;
        }
        // keep iterating until 
        //   1) a file is converted to a tuple successfully
        //   2) the cursor reaches the end
        while (cursor < pathList.size()) {            
            try {
                Path path = pathIterator.next();
                String extension = com.google.common.io.Files.getFileExtension(path.toString());
                String content;
                if (extension.equalsIgnoreCase("pdf")) {
                    content = FileExtractorUtils.extractPDFFile(path);
                } else if (extension.equalsIgnoreCase("ppt") || extension.equalsIgnoreCase("pptx")) {
                    content = FileExtractorUtils.extractPPTFile(path);
                } else if(extension.equalsIgnoreCase("doc") || extension.equalsIgnoreCase("docx")) {
                    content = FileExtractorUtils.extractWordFile(path);
                } else {
                    content = FileExtractorUtils.extractPlainTextFile(path);
                }
                Tuple tuple = new Tuple(outputSchema, IDField.newRandomID(), new TextField(content));
                cursor++;
                return tuple;
            } catch (DataflowException e) {
                // ignore error and move on
                // TODO: use log4j
                System.out.println("FileSourceOperator: file read error, file is ignored. " + e.getMessage());
            }
        }    
        return null;
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public FileSourcePredicate getPredicate() {
        return this.predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema == null || inputSchema.length == 0)
            return getOutputSchema();
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
