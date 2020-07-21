package edu.uci.ics.texera.dataflow.arrow;

import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.*;
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
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
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

    //Default nltk training model set to be "Senti.pickle"
    private String PicklePath = null;

    // For now it is fixed, but in the future should deal with arbitrary tuple and schema.
    // Related to Apache Arrow.
    private final static org.apache.arrow.vector.types.pojo.Schema tupleToPythonSchema =
            new org.apache.arrow.vector.types.pojo.Schema( asList (new Field("ID",
                            FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("text", FieldType.nullable(new ArrowType.Utf8()), null))
            );

    // Flight related
    private final static Location location = new Location(URI.create("grpc+tcp://localhost:5005"));
    private final static RootAllocator rootAllocator = new RootAllocator();
    private FlightClient flightClient = null;

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
        List<String> args = new ArrayList<>(Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath));
        ProcessBuilder processBuilder = new ProcessBuilder(args).inheritIO();
        try {
            // Start Flight server (Python process)
            processBuilder.start();
            // Connect to server
            flightClient = FlightClient.builder(rootAllocator, location).build();
//            System.out.println("Flight Client:\t" + new String(
                    flightClient.doAction(new Action("healthcheck")).next().getBody();
//                    , StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
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
        writeArrowStream(tupleBuffer);
        return true;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (tupleBuffer == null){
            if (computeTupleBuffer()) {
                computeClassLabel();
            } else {
                return null;
            }
        }
        return popupOneTuple();
    }

    // Process the data file using NLTK
    private void computeClassLabel() {
        try{
//            System.out.println("Flight Client:\t" + new String(
            flightClient.doAction(new Action("compute")).next().getBody();
//                    , StandardCharsets.UTF_8));
            idClassMap = new HashMap<>();
            readArrowStream();
        }catch(Exception e){
            throw new DataflowException(e.getMessage(), e);
        }
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
        try {
            flightClient.doAction(new Action("shutdown"));
            flightClient.close();
        } catch (InterruptedException e) {
            throw new DataflowException(e.getMessage(), e);
        }
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

    private void writeArrowStream(List<Tuple> values) {
//        System.out.print("Flight Client:\tSending data to Python...");
        SyncPutListener flightListener = new SyncPutListener();
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(tupleToPythonSchema, rootAllocator);
        FlightClient.ClientStreamListener streamWriter = flightClient.startPut(
                FlightDescriptor.path(Collections.singletonList("ToPython")), schemaRoot, flightListener);
        int index = 0;
        while (index < values.size()) {
            schemaRoot.allocateNew();
            int chunkIndex = 0;
            while (chunkIndex < predicate.getChunkSize() && index + chunkIndex < values.size()) {
                vectorizeTupleToPython(values.get(index + chunkIndex), chunkIndex, schemaRoot);
                chunkIndex++;
            }
            schemaRoot.setRowCount(chunkIndex);
            streamWriter.putNext();
            index += chunkIndex;
            schemaRoot.clear();
        }
        streamWriter.completed();
//        System.out.println(" Done.");
    }

    private void readArrowStream() {
//        System.out.print("Flight Client:\tReading data from Python...");
        FlightInfo info = flightClient.getInfo(FlightDescriptor.path(Collections.singletonList("FromPython")));
        Ticket ticket = info.getEndpoints().get(0).getTicket();
        FlightStream stream = flightClient.getStream(ticket);
        while (stream.next()) {
            VectorSchemaRoot root  = stream.getRoot(); // get root
            List<FieldVector> fieldVector = root.getFieldVectors();
            VarCharVector idVector = ((VarCharVector) fieldVector.get(0));
            BigIntVector predVector = ((BigIntVector) fieldVector.get(1));
            for (int j = 0; j < idVector.getValueCount(); j++) {
                String id = new String(idVector.get(j), StandardCharsets.UTF_8);
                int label = (int) predVector.get(j);
                idClassMap.put(id, label);
            }
        }
//        System.out.println(" Done.");
    }
}

