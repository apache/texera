package edu.uci.ics.textdb.exp.source.file;

import com.google.common.collect.Sets;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * FileSourceOperator reads a file or files under a directory and converts one file to one tuple.
 * <p>
 * The filePath in the predicate must be 1) a text file or 2) a directory
 * <p>
 * In case of a directory, FileSourceOperator supports recursively reading files
 * and specifying a max recursive depth.
 * <p>
 * The files must have one of the supported extensions: {@code supportedExtensions}
 * <p>
 * FileSourceOperator reads all content of one file and convert them to one tuple.
 * The tuple will have one column, the attributeName as defined in {@code FileSourcePredicate},
 * with the attributeType as TEXT.
 * <p>
 * In case of a directory, if the directory doesn't contain any file that
 * matches the allowed extensions, then an exception will be thrown.
 *
 * @author Zuozhi Wang
 * @author Jun Ma
 */
public class FileSourceOperator implements ISourceOperator {
    // cursor indicating the current position
    protected Integer cursor = CLOSED;
    protected final Schema outputSchema;
    protected FileSourcePredicate predicate;
    protected List<Path> pathList;
    protected Iterator<Path> pathIterator;

    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
        this.outputSchema = new Schema(SchemaConstants._ID_ATTRIBUTE, new Attribute(predicate.getAttributeName(), AttributeType.TEXT));
        this.pathList = new ArrayList<>();

        Path filePath = Paths.get(predicate.getFilePath());
        if (!Files.exists(filePath)) {
            throw new RuntimeException(String.format("file %s doesn't exist", filePath));
        }

        if (Files.isDirectory(filePath)) {
            try {
                if (this.predicate.isRecursive()) {
                    pathList.addAll(Files.walk(filePath, this.predicate.getMaxDepth()).collect(Collectors.toList()));
                } else {
                    pathList.addAll(Files.list(filePath).collect(Collectors.toList()));
                }

            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "opening directory %s failed: " + e.getMessage(), filePath));
            }
        } else {
            pathList.add(filePath);
        }

        this.pathList = pathList.stream()
                .filter(path -> !Files.isDirectory(path))
                .filter(path -> !path.getFileName().startsWith("."))
                .filter(path -> isExtensionAllowed(predicate.getAllowedExtensions(), path))
                .collect(Collectors.toList());

        // check if the path list is empty
        if (pathList.isEmpty()) {
            // TODO: change it to TextDB RuntimeException
            throw new RuntimeException(String.format(
                    "the filePath: %s doesn't contain any valid text files. " +
                            "File extension must be one of %s .",
                    filePath, predicate.getAllowedExtensions()));
        }
        pathIterator = pathList.iterator();
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED || cursor >= pathList.size()) {
            return null;
        }
        // keep iterating until
        //   1) a file is converted to a tuple successfully
        //   2) the cursor reaches the end
        while (pathIterator.hasNext()) {
            try {
                Path path = pathIterator.next();
                String extension = getExtension(path);
                String content;
                if (extension.equals("pdf")) {
                    content = TextExtractor.extractPDFFile(path);
                } else if (extension.equals("ppt") || extension.equals("pptx")) {
                    content = TextExtractor.extractPPTFile(path);
                } else {
                    //common file
                    content = TextExtractor.extractCommonFile(path);
                }
                // and assign a random ID to it
                Tuple tuple = null;
                if (content != null) {
                    tuple = new Tuple(outputSchema, IDField.newRandomID(), new TextField(content));
                }
                cursor++;
                return tuple;
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        }
        return null;
    }

    private String getExtension(Path path) {
        String filePath = path.toString();
        int indexOf = filePath.lastIndexOf(".");
        return filePath.substring(indexOf + 1);
    }

    /*
     * A helper function that returns if the file's extension is supported.
     * The extensions are expected to NOT have the dot "." in the string.
     * For example, extensions may contain "txt", but not ".txt"
     */
    protected static boolean isExtensionAllowed(List<String> allowedExtensions, Path path) {
        return allowedExtensions.stream()
                .map(ext -> "." + ext)
                .filter(ext -> path.getFileName().toString().toLowerCase().endsWith(ext))
                .findAny().isPresent();
    }

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        cursor = OPENED;
    }

    @Override
    public void close() throws TextDBException {
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
}
