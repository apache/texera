package edu.uci.ics.texera.dataflow.arrow;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import static java.util.Arrays.asList;

public class ArrowNltkSentiment implements IOperator {
    private final ArrowNltkSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private List<Tuple> batchedTupleBuffer;
    HashMap<String, Integer> idClassMap;
    private int cursor = CLOSED;
    private String picklePath = null;
    private final int batchSize;
    private final int chunkSize;
    private final String inputAttributeName;
    private final String outputAttributeName;

    private final static String PYTHON = "python";
    private final static String PYTHON_SCRIPT = Utils.getResourcePath(
            "arrow_for_nltk_sentiment.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String TO_PYTHON_FILE = Utils.getResourcePath(
            "temp-to-python.arrow", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String FROM_PYTHON_FILE = Utils.getResourcePath(
            "temp-from-python.arrow", TexeraProject.TEXERA_DATAFLOW).toString();

    private final static char SEPARATOR = ',';
    private final static char QUOTE_CHAR = '"';

    private final static org.apache.arrow.vector.types.pojo.Schema tupleToPythonSchema =
            new org.apache.arrow.vector.types.pojo.Schema( asList (new Field("ID",
                            FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("text", FieldType.nullable(new ArrowType.Utf8()), null))
            );

    public ArrowNltkSentiment(ArrowNltkSentimentPredicate predicate){
        this.predicate = predicate;

        String modelFileName = predicate.getInputAttributeModel();
        if (modelFileName == null) {
            modelFileName = "NltkSentiment.pickle";
        }

        this.picklePath = Utils.getResourcePath(modelFileName, TexeraProject.TEXERA_DATAFLOW).toString();
        this.inputAttributeName = predicate.getInputAttributeName();
        this.outputAttributeName = predicate.getResultAttributeName();
        this.batchSize = predicate.getBatchSize();
        this.chunkSize = predicate.getChunkSize();
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
        Schema.checkAttributeExists(inputSchema, inputAttributeName);
        Schema.checkAttributeNotExists(inputSchema,outputAttributeName);
        return new Schema.Builder().add(inputSchema).add(outputAttributeName, AttributeType.INTEGER).build();
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

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (batchedTupleBuffer == null){
            if (computeBatchedTupleBuffer()) {
                computeClassLabel();
            } else {
                return null;
            }
        }
        return popupOneTuple();
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
                index, tuple.getField(inputAttributeName).getValue().toString().getBytes(StandardCharsets.UTF_8)
        );
    }

    private boolean computeBatchedTupleBuffer() {
        batchedTupleBuffer = new ArrayList<Tuple>();
        int i = 0;
        while (i < batchSize){
            Tuple inputTuple;
            if ((inputTuple = inputOperator.getNextTuple()) != null) {
                batchedTupleBuffer.add(inputTuple);
                i++;
            } else {
                break;
            }
        }

        if (batchedTupleBuffer.isEmpty()) {
            return false;
        }

        try {
            if (Files.notExists(Paths.get(TO_PYTHON_FILE))) {
                Files.createFile(Paths.get(TO_PYTHON_FILE));
            }
            new ChunkedArrowWriter<Tuple>(chunkSize, this::vectorizeTupleToPython).write(
                    new File(TO_PYTHON_FILE), batchedTupleBuffer, tupleToPythonSchema);
        } catch (IOException e) {
            throw new DataflowException(e.getMessage(), e);
        }
        return true;
    }

    // Process the data file using NLTK
    private void computeClassLabel() {
        /*
         *  In order to use the NLTK package to do classification, we start a
         *  new process to run the package, and wait for the result of running
         *  the process as the class label of this text field.
         *  Python call format:
         *      #python3 nltk_sentiment_classify picklePath dataPath resultPath
         * */
        try{
            List<String> args = new ArrayList<String>(
                    asList(PYTHON, PYTHON_SCRIPT, picklePath, ArrowNltkSentiment.TO_PYTHON_FILE, FROM_PYTHON_FILE));
            ProcessBuilder processBuilder = new ProcessBuilder(args);

            Process p = processBuilder.start();
            p.waitFor();

            //Read label result from arrow file generated by Python.
            idClassMap = new HashMap<String, Integer>();

            //Prepare for read
            File arrowFile = new File(FROM_PYTHON_FILE);
            FileInputStream fileInputStream = new FileInputStream(arrowFile);
            SeekableReadChannel seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel());
            ArrowFileReader arrowFileReader = new ArrowFileReader(seekableReadChannel,
                    new RootAllocator(Integer.MAX_VALUE));
            VectorSchemaRoot root  = arrowFileReader.getVectorSchemaRoot(); // get root
            List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();

            //For every block(arrow batch / or called 'chunk' here)
            for (ArrowBlock rbBlock : arrowBlocks) {
                if (!arrowFileReader.loadRecordBatch(rbBlock)) { // load the batch
                    throw new IOException("Expected to read record batch");
                }
                List<FieldVector> fieldVector = root.getFieldVectors();
                VarCharVector idVector = ((VarCharVector) fieldVector.get(0));
                BigIntVector predVector = ((BigIntVector) fieldVector.get(1));
                for (int j = 0; j < idVector.getValueCount(); j++) {
                    String id = new String(idVector.get(j), StandardCharsets.UTF_8);
                    int label = (int) predVector.get(j);
                    try {
                        idClassMap.put(id, label);
                    } catch (NumberFormatException e) {
                        idClassMap.put(id, 0);
                    }
                }
            }
        }catch(Exception e){
            throw new DataflowException(e.getMessage(), e);
        }
    }

    private Tuple popupOneTuple() {
        Tuple outputTuple = batchedTupleBuffer.get(0);
        batchedTupleBuffer.remove(0);
        if (batchedTupleBuffer.isEmpty()) {
            batchedTupleBuffer = null;
        }

        List<IField> outputFields = new ArrayList<>(outputTuple.getFields());

        Integer className = idClassMap.get(outputTuple.getField(SchemaConstants._ID).getValue().toString());
        outputFields.add(new IntegerField( className ));
        return new Tuple(outputSchema, outputFields);
    }
}

