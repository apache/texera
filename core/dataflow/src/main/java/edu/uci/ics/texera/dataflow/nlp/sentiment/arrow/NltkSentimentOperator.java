package edu.uci.ics.texera.dataflow.nlp.sentiment.arrow;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import com.google.common.collect.ImmutableList;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.*;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringHashMap;

import static edu.uci.ics.texera.api.schema.AttributeType.*;

public class NltkSentimentOperator implements IOperator {
    private final NltkSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;

    private List<Tuple> tupleBuffer;
    Queue<Tuple> resultQueue;

    private int cursor = CLOSED;

    private final static String PYTHON = "python3";
    private final static String PYTHONSCRIPT = Utils.getResourcePath("nltk_sentiment_classify.py", TexeraProject.TEXERA_DATAFLOW).toString();

    //Default nltk training model set to be "Senti.pickle"
    private String PicklePath = null;

    // For now it is fixed, but in the future should deal with arbitrary tuple and schema.
    // Related to Apache Arrow.
    private org.apache.arrow.vector.types.pojo.Schema tupleToPythonSchema;

    private final static RootAllocator rootAllocator = new RootAllocator();
    private FlightClient flightClient = null;

    // This is temporary, used to vectorize LIST type data.
    private Map<String, Integer> innerIndexMap;

    public NltkSentimentOperator(NltkSentimentPredicate predicate){
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

    /**
     * When this operator is opened, it executes the python script, which constructs a {@code FlightServer}
     * object which is then up and running in the specified address. The operator calls
     * {@code flightClient.doAction(new Action("healthcheck"))} to check the status of the server, and then proceeds if
     * successful (otherwise there will be an exception).
     * @throws TexeraException
     */
    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }

        // Flight related
        try {
            int portNumber = getFreeLocalPort();
            Location location = new Location(URI.create("grpc+tcp://localhost:" + portNumber));
            List<String> args = new ArrayList<>(
                    Arrays.asList(
                            PYTHON, PYTHONSCRIPT,
                            Integer.toString(portNumber), PicklePath,
                            predicate.getInputAttributeName(), predicate.getResultAttributeName()
                    )
            );
            ProcessBuilder processBuilder = new ProcessBuilder(args).inheritIO();
            // Start Flight server (Python process)
            processBuilder.start();
            // Connect to server
            boolean connected = false;
            int tryCount = 0;
            while (!connected && tryCount < 5) {
                try {
                    flightClient = FlightClient.builder(rootAllocator, location).build();
                    String message = new String(
                            flightClient.doAction(new Action("healthcheck")).next().getBody(), StandardCharsets.UTF_8);
                    connected = message.equals("Flight Server is up and running!");
                } catch (Exception e) {
                    System.out.println("Flight Client:\tNot connected to the server in this try.");
                    flightClient.close();
                    tryCount++;
                }
            }
            if (tryCount == 5) throw new DataflowException("Exceeded try limit of 5 when connecting to Flight Server!");
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }

        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();

        // generate output schema by transforming the input schema
        outputSchema = transformToOutputSchema(inputSchema);

        cursor = OPENED;

        tupleToPythonSchema = convertToArrowSchema(inputSchema);
        innerIndexMap = new HashMap<>();
    }

    /**
     * For every batch, the operator calls {@code flightClient.doAction(new Action("compute"))} to tell the server to
     * compute sentiments of the specific table that was sent earlier. The server executes computation,
     * and returns back a success message when computation is finished.
     * @return Whether the buffer is empty
     */
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
            resultQueue = new LinkedList<>();
            readArrowStream();
        }catch(Exception e){
            throw new DataflowException(e.getMessage(), e);
        }
    }

    private Tuple popupOneTuple() {
        tupleBuffer.remove(0);
        if (tupleBuffer.isEmpty()) {
            tupleBuffer = null;
        }
        return resultQueue.remove();
    }

    /**
     * When all the batches are finished and the operator closes, it issues a
     * {@code flightClient.doAction(new Action("shutdown"))} call to shut down the server, and also closes the client.
     * @throws TexeraException
     */
    @Override
    public void close() throws TexeraException {
        try {
            flightClient.doAction(new Action("shutdown")).next();
            flightClient.close();
        } catch (InterruptedException e) {
            throw new DataflowException(e.getMessage(), e);
        }
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
        for (Attribute a : tuple.getSchema().getAttributes()) {
            String name = a.getName();
            // When it is null, skip it.
            if (tuple.getField(name).getValue() == null) continue;
            switch (a.getType()) {
                case INTEGER:
                    ((IntVector) schemaRoot.getVector(name)).setSafe(index, (int) tuple.getField(name).getValue());
                    break;
                case DOUBLE:
                    ((Float8Vector) schemaRoot.getVector(name)).setSafe(index, (double) tuple.getField(name).getValue());
                    break;
                case BOOLEAN:
//                    ((BitVector) schemaRoot.getVector(name)).setSafe(index, ((Integer) tuple.getField(name).getValue()));
//                    break;
                case TEXT:
                case STRING:
                case _ID_TYPE:
                    ((VarCharVector) schemaRoot.getVector(name)).setSafe(
                            index, tuple.getField(name).getValue().toString().getBytes(StandardCharsets.UTF_8));
                    break;
                case DATE:
                    ((DateDayVector) schemaRoot.getVector(name)).setSafe(index,
                            (int)((LocalDate) tuple.getField(name).getValue()).toEpochDay());
                    break;
                case DATETIME:
                    StructVector dateTimeStructs = ((StructVector) schemaRoot.getVector(name));
                    if (tuple.getField(name).getValue() != null) {
                        dateTimeStructs.setIndexDefined(index);
                        DateDayVector subVectorDay = (DateDayVector) dateTimeStructs.getVectorById(0);
                        TimeSecVector subVectorTime = (TimeSecVector) dateTimeStructs.getVectorById(1);
                        LocalDateTime value = (LocalDateTime) tuple.getField(name).getValue();
                        subVectorDay.setSafe(index, (int) value.toLocalDate().toEpochDay());
                        subVectorTime.setSafe(index, value.toLocalTime().toSecondOfDay());
                    }
                    else dateTimeStructs.setNull(index);
                    break;
                case LIST:
                    // For now only supporting span.
                    if (((ImmutableList) tuple.getField(name).getValue()).get(0).getClass() != Span.class) {
                        throw (new DataflowException("Unsupported Element Type for List Field!"));
                    }
                    else {
                        ListVector listVector = (ListVector) schemaRoot.getVector(name);
                        ImmutableList<Span> spansList = (ImmutableList<Span>) tuple.getField(name).getValue();
                        convertListOfSpans(spansList, listVector, index, name);
                    }

                    break;
                default: break;
            }
        }
    }

    /**
     * For every batch, the operator converts list of {@code Tuple}s into Arrow stream data in almost the exact same
     * way as it would when using Arrow file, except now it sends stream to the server with
     * {@link FlightClient#startPut(org.apache.arrow.flight.FlightDescriptor, org.apache.arrow.vector.VectorSchemaRoot,
     * org.apache.arrow.flight.FlightClient.PutListener, org.apache.arrow.flight.CallOption...)} and {@link
     * FlightClient.ClientStreamListener#putNext()}. The server uses {@code do_put()} to receive data stream
     * and convert it into a {@code pyarrow.Table} and store it in the server.
     * @param values The buffer of tuples to write.
     */
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
        flightListener.getResult();
        flightListener.close();
//        System.out.println(" Done.");
    }


    /**
     * For every batch, the operator gets the computed sentiment result by calling
     * {@link FlightClient#getStream(org.apache.arrow.flight.Ticket, org.apache.arrow.flight.CallOption...)}.
     * The reading and conversion process is the same as what it does when using Arrow file.
     */
    private void readArrowStream() {
//        System.out.print("Flight Client:\tReading data from Python...");
        FlightInfo info = flightClient.getInfo(FlightDescriptor.path(Collections.singletonList("FromPython")));
        Ticket ticket = info.getEndpoints().get(0).getTicket();
        FlightStream stream = flightClient.getStream(ticket);
        while (stream.next()) {
            VectorSchemaRoot root  = stream.getRoot(); // get root
            convertArrowVectorsToResults(root);
        }
//        System.out.println(" Done.");
    }

    private int getFreeLocalPort() throws IOException {
        ServerSocket s = null;
        try {
            // ServerSocket(0) results in availability of a free random port
            s = new ServerSocket(0);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            assert s != null;
            s.close();
        }
    }

    private org.apache.arrow.vector.types.pojo.Schema convertToArrowSchema(Schema texeraSchema) {
        List<Field> arrowFields = new ArrayList<>();
        for (Attribute a : texeraSchema.getAttributes()) {
            String name = a.getName();
            Field field = null;
            switch (a.getType()) {
                case INTEGER:
                    field = Field.nullablePrimitive(name, new ArrowType.Int(32, true));
                    break;
                case DOUBLE:
                    field = Field.nullablePrimitive(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
                    break;
                case BOOLEAN:
                    // Current BOOLEAN type is internally a string, so it will fall-through to the string case.
                    // We might change this in the future.
//                    field = Field.nullablePrimitive(name, new ArrowType.Bool());
//                    break;
                case TEXT:
                case STRING:
                case _ID_TYPE:
                    field = Field.nullablePrimitive(name, ArrowType.Utf8.INSTANCE);
                    break;
                case DATE:
                    field = Field.nullablePrimitive(name, new ArrowType.Date(DateUnit.DAY));
                    break;
                case DATETIME:
                    field = new Field(
                            name,
                            FieldType.nullable(ArrowType.Struct.INSTANCE),
                            Arrays.asList(
                                    Field.nullablePrimitive("date", new ArrowType.Date(DateUnit.DAY)),
                                    Field.nullablePrimitive("time", new ArrowType.Time(TimeUnit.SECOND, 32))
                            )
                    );
                    break;
                case LIST:
                    List<Field> children = Arrays.asList(
                            Field.nullablePrimitive("attributeName", ArrowType.Utf8.INSTANCE),
                            Field.nullablePrimitive("start", new ArrowType.Int(32, true)),
                            Field.nullablePrimitive("end", new ArrowType.Int(32, true)),
                            Field.nullablePrimitive("key", ArrowType.Utf8.INSTANCE),
                            Field.nullablePrimitive("value", ArrowType.Utf8.INSTANCE),
                            Field.nullablePrimitive("tokenOffset", new ArrowType.Int(32, true))
                    );
                    field = new Field(
                            name,
                            FieldType.nullable(new ArrowType.List()),
                            Collections.singletonList(
                                    new Field("Span", FieldType.nullable(ArrowType.Struct.INSTANCE), children))
                    );
                    break;
                default: break;
            }
            arrowFields.add(field);
        }
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    private void convertArrowVectorsToResults(VectorSchemaRoot schemaRoot) {
        List<FieldVector> fieldVectors = schemaRoot.getFieldVectors();
        Schema texeraSchema = convertToTexeraSchema(schemaRoot.getSchema());
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            Tuple tuple;
            List<IField> texeraFields = new ArrayList<>();

            for (FieldVector vector : fieldVectors) {
                IField texeraField = null;
                try {
                    switch (vector.getField().getFieldType().getType().getTypeID()) {
                        case Int:
                            // It's either IntVector or BigIntVector, but can't know because it depends on Python.
                            try {
                                texeraField = new IntegerField(((IntVector) vector).get(i));
                            } catch (ClassCastException e) {
                                texeraField = new IntegerField((int)((BigIntVector) vector).get(i));
                            }
                            break;
                        case FloatingPoint:
                            texeraField = new DoubleField((((Float8Vector) vector).get(i)));
                            break;
//                    case Bool:
//                        // FIXME: No BooleanField Class available.
//                        texeraField = new IntegerField(((IntVector) vector).get(i));
//                        break;
                        case Utf8:
                            texeraField = new TextField(new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8));
                            break;
                        case Date:
                            texeraField = new DateField(new Date(((DateDayVector) vector).get(i)));
                            break;
                        case Struct:
                            // For now, struct is only for DateTime
                            DateDayVector subVectorDay = (DateDayVector) ((StructVector) vector).getChildByOrdinal(0);
                            TimeSecVector subVectorTime = (TimeSecVector) ((StructVector) vector).getChildByOrdinal(1);
                            texeraField = new DateTimeField(
                                    LocalDateTime.of(
                                            LocalDate.ofEpochDay(subVectorDay.get(i)),
                                            LocalTime.ofSecondOfDay(subVectorTime.get(i))
                                    )
                            );
                            break;
                        case List:
                            texeraField = getSpanFromListVector((ListVector) vector, i);
                            break;
                        default:
                            throw (new DataflowException("Unsupported data type "+
                                    vector.getField().toString() +
                                    " when converting back to Texera table."));
                    }
                } catch (IllegalStateException e) {
                    if (!e.getMessage().contains("Value at index is null")) {
                        throw new DataflowException(e);
                    } else {
                        switch (vector.getField().getFieldType().getType().getTypeID()) {
                            case Int: texeraField = new IntegerField(null); break;
                            case FloatingPoint: texeraField = new DoubleField(null); break;
                            case Date: texeraField = new DateField((String) null); break;
                            case Struct: texeraField = new DateTimeField((String) null); break;
                            case List: texeraField = new ListField<Span>(null);
                            default: break;
                        }
                    }
                }
                texeraFields.add(texeraField);
            }
            tuple = new Tuple(texeraSchema, texeraFields);
            resultQueue.add(tuple);
        }
    }

    private Schema convertToTexeraSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        List<Attribute> texeraAttributes = new ArrayList<>();
        for (Field f : arrowSchema.getFields()) {
            String attributeName = f.getName();
            AttributeType attributeType;
            ArrowType arrowType = f.getFieldType().getType();
            switch (arrowType.getTypeID()) {
                case Int:
                    attributeType = INTEGER;
                    break;
                case FloatingPoint:
                    attributeType = DOUBLE;
                    break;
                case Bool:
                    attributeType = BOOLEAN;
                    break;
                case Utf8:
                case Null:
                    attributeType = TEXT;
                    break;
                case Date:
                    attributeType = DATE;
                    break;
                case Struct:
                    // For now only Struct of DateTime
                    attributeType = DATETIME;
                    break;
                case List:
                    attributeType = LIST;
                    break;
                default:
                    throw (new DataflowException("Unsupported data type "+
                            arrowType.getTypeID() +
                            " when converting back to Texera table."));
            }
            texeraAttributes.add(new Attribute(attributeName, attributeType));
        }
        return new Schema(texeraAttributes);
    }

    // For now we're only allowing List<Span>. This can (and should) be generalized in the future.
    private void convertListOfSpans(ImmutableList<Span> spansList, ListVector listVector, int index, String name) {
        if (index == 0) {
            if (innerIndexMap.containsKey(name)) innerIndexMap.replace(name, 0);
            else innerIndexMap.put(name, 0);
        }
        int innerIndex = innerIndexMap.get(name);
        int size = spansList.size();
        StructVector subElementsVector = (StructVector) listVector.getDataVector();
        listVector.startNewValue(index);
        VarCharVector attributeNameVector = (VarCharVector) subElementsVector.getVectorById(0);
        IntVector startVector = (IntVector) subElementsVector.getVectorById(1);
        IntVector endVector = (IntVector) subElementsVector.getVectorById(2);
        VarCharVector keyVector = (VarCharVector) subElementsVector.getVectorById(3);
        VarCharVector valueVector = (VarCharVector) subElementsVector.getVectorById(4);
        IntVector tokenOffsetVector = (IntVector) subElementsVector.getVectorById(5);

        for (int i = 0; i < size; i++) {
            if (spansList.get(i) == null) {
                subElementsVector.setNull(innerIndex);
            }
            else {
                subElementsVector.setIndexDefined(innerIndex);
                Span span = spansList.get(i);
                // For all the fields of the struct
                if (span.getAttributeName() != null) attributeNameVector.setSafe(innerIndex, span.getAttributeName().getBytes(StandardCharsets.UTF_8));
                startVector.setSafe(innerIndex, span.getStart());
                endVector.setSafe(innerIndex, span.getEnd());
                if (span.getKey() != null) keyVector.setSafe(innerIndex, span.getKey().getBytes(StandardCharsets.UTF_8));
                if (span.getValue() != null) valueVector.setSafe(innerIndex, span.getValue().getBytes(StandardCharsets.UTF_8));
                tokenOffsetVector.setSafe(innerIndex, span.getTokenOffset());
            }
            innerIndex++;
        }
        innerIndexMap.replace(name, innerIndex);
        listVector.endValue(index, size);
    }

    private ListField<Span> getSpanFromListVector(ListVector listVector, int index) {
       List<Span> resultList = new ArrayList<>();
       List<JsonStringHashMap> vals = (List<JsonStringHashMap>) listVector.getObject(index);
       for (JsonStringHashMap spanMap : vals) {
           resultList.add(
                   new Span(
                           spanMap.get("attributeName").toString(),
                           (int) spanMap.get("start"),
                           (int) spanMap.get("end"),
                           spanMap.get("key").toString(),
                           spanMap.get("value").toString(),
                           (int) spanMap.get("tokenOffset")
                   )
           );
       }
       return new ListField<>(resultList);
    }
}

