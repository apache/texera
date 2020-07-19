package edu.uci.ics.texera.dataflow.arrow;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import static java.util.Arrays.asList;

public class ArrowNltkSentimentOperator implements IOperator {
    private final ArrowNltkSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;

    private List<Tuple> tupleBuffer;
    HashMap<String, Integer> idClassMap;

    private int cursor = CLOSED;

    private final static String PYTHON = "python3";
    private final static String PYTHONSCRIPT = Utils.getResourcePath("arrow_for_nltk_sentiment.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String BatchedFiles = Utils.getResourcePath("temp-to-python.arrow", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String resultPath = Utils.getResourcePath("temp-from-python.arrow", TexeraProject.TEXERA_DATAFLOW).toString();

    //Default nltk training model set to be "Senti.pickle"
    private String PicklePath = null;

    // For now it is fixed, but in the future should deal with arbitrary tuple and schema.
    // Related to Apache Arrow.
    private final static org.apache.arrow.vector.types.pojo.Schema tupleToPythonSchema =
            new org.apache.arrow.vector.types.pojo.Schema( asList (new Field("ID",
                            FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("text", FieldType.nullable(new ArrowType.Utf8()), null))
            );

    public ArrowNltkSentimentOperator(ArrowNltkSentimentPredicate predicate){
        this.predicate = predicate;

        String modelFileName = predicate.getInputAttributeModel();
        if (modelFileName == null) {
            modelFileName = "NltkSentiment.pickle";
        }
        this.PicklePath = Utils.getResourcePath(modelFileName, TexeraProject.TEXERA_DATAFLOW).toString();

    }

    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {
            throw new TexeraException("Cannot link this operator to another operator after the operator is opened");
        }
        this.inputOperator = operator;
    }

    /*
     * add a new field to the schema, with name resultAttributeName and type String
     */
    private Schema transformSchema(Schema inputSchema){
        Schema.checkAttributeExists(inputSchema, predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());
        return new Schema.Builder().add(inputSchema).add(predicate.getResultAttributeName(), AttributeType.INTEGER).build();
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();

        // generate output schema by transforming the input schema
        outputSchema = transformToOutputSchema(inputSchema);

        cursor = OPENED;
    }

    private boolean computeTupleBuffer() {
        tupleBuffer = new ArrayList<Tuple>();
        int i = 0;
        while (i < predicate.getBatchSize()){
            Tuple inputTuple;
            if ((inputTuple = inputOperator.getNextTuple()) != null) {
                tupleBuffer.add(inputTuple);
                i++;
            } else {
                break;
            }
        }
        if (tupleBuffer.isEmpty()) {
            return false;
        }
        try {
            if (Files.notExists(Paths.get(BatchedFiles))) {
                Files.createFile(Paths.get(BatchedFiles));
            }
           writeArrowFile(new File(BatchedFiles), tupleBuffer);
        } catch (IOException e) {
            throw new DataflowException(e.getMessage(), e);
        }
        return true;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (tupleBuffer == null){
            if (computeTupleBuffer()) {
                computeClassLabel(BatchedFiles);
            } else {
                return null;
            }
        }
        return popupOneTuple();
    }

    // Process the data file using NLTK
    private String computeClassLabel(String filePath) {
        try{
            /*
             *  In order to use the NLTK package to do classification, we start a
             *  new process to run the package, and wait for the result of running
             *  the process as the class label of this text field.
             *  Python call format:
             *      #python3 nltk_sentiment_classify picklePath dataPath resultPath
             * */
            List<String> args = new ArrayList<String>(
                    Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath, filePath, resultPath));
            ProcessBuilder processBuilder = new ProcessBuilder(args);

            Process p = processBuilder.start();
            p.waitFor();

            //Read label result from file generated by Python.

            idClassMap = new HashMap<String, Integer>();
            readArrowFile(new File(resultPath));
        }catch(Exception e){
            throw new DataflowException(e.getMessage(), e);
        }
        return null;
    }

    private Tuple popupOneTuple() {
        Tuple outputTuple = tupleBuffer.get(0);
        tupleBuffer.remove(0);
        if (tupleBuffer.isEmpty()) {
            tupleBuffer = null;
        }

        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(outputTuple.getFields());

        Integer className = idClassMap.get(outputTuple.getField(SchemaConstants._ID).getValue().toString());
        outputFields.add(new IntegerField( className ));
        return new Tuple(outputSchema, outputFields);
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {

        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        // check if the input schema is presented
        if (! inputSchema[0].containsAttribute(predicate.getInputAttributeName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema[0].getAttributeNames()));
        }

        // check if the attribute type is valid
        AttributeType inputAttributeType =
                inputSchema[0].getAttribute(predicate.getInputAttributeName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) ||
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new TexeraException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }

        return transformSchema(inputSchema[0]);
    }

    private void vectorizeTupleToPython(Tuple tuple, int index, VectorSchemaRoot schemaRoot) {
        ((VarCharVector) schemaRoot.getVector("ID")).setSafe(
                index, tuple.getField(SchemaConstants._ID).getValue().toString().getBytes(StandardCharsets.UTF_8)
        );
        ((VarCharVector) schemaRoot.getVector("text")).setSafe(
                index, tuple.getField(predicate.getInputAttributeName()).getValue().toString().getBytes(StandardCharsets.UTF_8)
        );
    }

    private void writeArrowFile(File file, List<Tuple> values) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(ArrowNltkSentimentOperator.tupleToPythonSchema, allocator);
             FileOutputStream fd = new FileOutputStream(file);
             ArrowFileWriter fileWriter = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {

            fileWriter.start();

            int index = 0;
            while (index < values.size()) {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < predicate.getChunkSize() && index + chunkIndex < values.size()) {
                    vectorizeTupleToPython(values.get(index + chunkIndex), chunkIndex, schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                fileWriter.writeBatch();

                index += chunkIndex;
                schemaRoot.clear();
            }

            fileWriter.end();
        }
    }

    private void readArrowFile(File arrowFile) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(arrowFile);
        SeekableReadChannel seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel());
        ArrowFileReader arrowFileReader = new ArrowFileReader(seekableReadChannel, new RootAllocator(Integer.MAX_VALUE));
        VectorSchemaRoot root  = arrowFileReader.getVectorSchemaRoot(); // get root
        List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
        //For every block(arrow batch / or called 'chunk' here)
        for (ArrowBlock rbBlock : arrowBlocks) {
            if (!arrowFileReader.loadRecordBatch(rbBlock)) { // load the batch
                throw new IOException("Expected to read record batch, but found none.");
            }
            List<FieldVector> fieldVector = root.getFieldVectors();
            VarCharVector idVector = ((VarCharVector) fieldVector.get(0));
            BigIntVector predVector = ((BigIntVector) fieldVector.get(1));
            for (int j = 0; j < idVector.getValueCount(); j++) {
                String id = new String(idVector.get(j), StandardCharsets.UTF_8);
                int label = (int) predVector.get(j);
                idClassMap.put(id, label);
            }
        }
    }
}
