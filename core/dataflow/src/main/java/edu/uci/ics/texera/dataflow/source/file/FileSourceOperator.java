package edu.uci.ics.texera.dataflow.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Verify;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.plangen.QueryContext;
import edu.uci.ics.texera.dataflow.resource.file.FileManager;

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
    private Schema outputSchema;
    
    // a list of files, each of which is a valid text file
    private List<Path> pathList;

    // cursor indicating the current position
    private Integer cursor = CLOSED;

    private List<Tuple> buffer = new ArrayList<>();

    
    public FileSourceOperator(FileSourcePredicate predicate, QueryContext ctx) {
        this.predicate = predicate;

        this.pathList = new ArrayList<>();

        Verify.verify((predicate.getFileName() != null && ctx != null) || predicate.getFilePath() != null);

        Path filePath;
        if (predicate.getFileName() == null) {
            filePath = Paths.get(predicate.getFilePath());
        } else {
            filePath = FileManager.getFilePath(ctx.getProjectOwnerID(), predicate.getFileName());
        }

        if (! Files.exists(filePath)) {
            throw new TexeraException(String.format("file %s doesn't exist", filePath));
        }

        Verify.verify(! Files.isDirectory(filePath));

        pathList.add(filePath);
                
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
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        cursor = OPENED;

        try {
            List<String> columnNames = null;

            if (predicate.getFileFormat() != null && predicate.getFileFormat() == FileSourcePredicate.FileFormat.CSV_WITH_HEADER) {
                Optional<String> header = Files.lines(pathList.get(0)).findFirst();
                if (header.isPresent()) {
                    columnNames = Arrays.stream(header.get().split(predicate.getColumnDelimiter())).collect(Collectors.toList());
                }
            } if (predicate.getColumnDelimiter() != null) {
                Optional<String> firstLine = Files.lines(pathList.get(0)).findFirst();
                if (firstLine.isPresent()) {
                    columnNames = IntStream.range(0, firstLine.get().split(predicate.getColumnDelimiter()).length)
                            .map(i -> i + 1).mapToObj(i -> "c" + i).collect(Collectors.toList());
                }
            }

            if (columnNames == null) {
                columnNames = Collections.singletonList("c1");
            }

            List<Attribute> attributes = columnNames.stream()
                    .map(name -> new Attribute(name, AttributeType.TEXT)).collect(Collectors.toList());
            this.outputSchema = new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE).add(attributes).build();

        } catch (IOException e) {
            throw new DataflowException(e);
        }


    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (! buffer.isEmpty()) {
            cursor++;
            return buffer.remove(0);
        }

        // keep iterating until 
        //   1) a file is converted to a tuple successfully
        //   2) the cursor reaches the end
        while (! pathList.isEmpty()) {
            try {
                Path path = pathList.remove(0);
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
                List<String> rows = new ArrayList<>();
                if (predicate.getRowDelimiter() != null) {
                    rows.addAll(Arrays.asList(content.split(predicate.getRowDelimiter())));
                } else {
                    rows.add(content);
                }

                List<Tuple> results = new ArrayList<>();
                for (String row: rows) {
                    List<IField> fields = new ArrayList<>();
                    if (predicate.getColumnDelimiter() != null && ! predicate.getColumnDelimiter().isEmpty()) {
                        fields.add(IDField.newRandomID());
                        List<String> columns = Arrays.asList(row.split(predicate.getColumnDelimiter()));
                        columns.forEach(c -> fields.add(new TextField(c)));
                        IntStream.range(0, outputSchema.getAttributes().size() - 1 - columns.size()).forEach(i -> fields.add(new TextField("")));
                    } else {
                        fields.add(IDField.newRandomID());
                        fields.add(new TextField(row));
                        IntStream.range(0, outputSchema.getAttributes().size() - 2).forEach(i -> fields.add(new TextField("")));
                    }
                    results.add(new Tuple(outputSchema, fields));
                }
                if (results.isEmpty()) {
                    continue;
                }

                buffer = results;
                cursor++;
                return buffer.remove(0);
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
