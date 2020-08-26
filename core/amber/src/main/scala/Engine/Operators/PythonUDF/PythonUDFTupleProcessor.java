package Engine.Operators.PythonUDF;

import Engine.Common.AmberException.AmberException;
import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import texera.common.TexeraUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PythonUDFTupleProcessor implements TupleProcessor {
    private String pythonScriptPath;
    private ArrayList<String> inputColumns;
    private ArrayList<String> outputColumns;
    private ArrayList<String> outerFilePaths;
    private int batchSize;

    private static final int MAX_TRY_COUNT = 20;
    private static final long WAIT_TIME_MS = 500;
    private static final String PYTHON = "python3";
    private static final String DAEMON_SCRIPT_PATH = getPythonResourcePath("texera_udf_server_main.py");

    private static final RootAllocator globalRootAllocator = new RootAllocator();
    private static final ObjectMapper globalObjectMapper = new ObjectMapper();
    private FlightClient flightClient;
    private Schema globalInputSchema;

    private Queue<Tuple> inputTupleBuffer;
    private Queue<Tuple> outputTupleBuffer;

    PythonUDFTupleProcessor(String pythonScriptFile, ArrayList<String> inputColumns, ArrayList<String> outputColumns,
                            ArrayList<String> outerFiles, int batchSize){
        setPredicate(pythonScriptFile, inputColumns, outputColumns, outerFiles, batchSize);
    }

    public void setPredicate(String pythonScriptFile, ArrayList<String> inputColumns, ArrayList<String> outputColumns,
                             ArrayList<String> outerFiles, int batchSize) {
        this.pythonScriptPath = pythonScriptFile;
        this.inputColumns = inputColumns;
        this.outputColumns = outputColumns;
        this.outerFilePaths = new ArrayList<>();
        for (String s : outerFiles) outerFilePaths.add(getPythonResourcePath(s));
        this.batchSize = batchSize;
    }

    @Override
    public void accept(Tuple tuple) {
        if (inputTupleBuffer == null) {
            // The first time, initialize this buffer.
            inputTupleBuffer = new LinkedList<>();
            // FIXME: Since there's no Schema implementation, here the names are hard-coded.
            try {
                globalInputSchema = convertAmber2ArrowSchema(tuple,
                        Arrays.asList(
                                "create_at",
                                "id",
                                "text",
                                "favorite_count",
                                "retweet_count",
                                "lang",
                                "is_retweet"
                        )
                );
            } catch (Exception e) {
                closeAndThrow(flightClient, e);
            }

        }
        inputTupleBuffer.add(tuple);
        if (inputTupleBuffer.size() == batchSize) {
            // This batch is full, execute the UDF.
            processOneBatch();
        }
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {
        if (inputTupleBuffer != null && !inputTupleBuffer.isEmpty()) {
            // There are some unprocessed tuples, finish them.
            processOneBatch();
        }
    }

    @Override
    public void initialize() {
        try {
            int portNumber = getFreeLocalPort();
            Location location = new Location(URI.create("grpc+tcp://localhost:" + portNumber));
            List<String> args = new ArrayList<>(
                    Arrays.asList(PYTHON, DAEMON_SCRIPT_PATH, Integer.toString(portNumber), getPythonResourcePath(pythonScriptPath))
            );

            ProcessBuilder processBuilder = new ProcessBuilder(args).inheritIO();
            // Start Flight server (Python process)
            processBuilder.start();
            // Connect to server
            boolean connected = false;
            int tryCount = 0;
            while (!connected && tryCount < MAX_TRY_COUNT) {
                try {
                    Thread.sleep(WAIT_TIME_MS);
                    flightClient = FlightClient.builder(globalRootAllocator, location).build();
                    String message = new String(
                            flightClient.doAction(new Action("healthcheck")).next().getBody(), StandardCharsets.UTF_8);
                    connected = message.equals("Flight Server is up and running!");
                } catch (Exception e) {
                    System.out.println("Flight Client:\tNot connected to the server in this try.");
                    flightClient.close();
                    tryCount++;
                }
            }
            if (tryCount == MAX_TRY_COUNT)
                throw new AmberException("Exceeded try limit of 5 when connecting to Flight Server!");
        } catch (Exception e) {
            throw new AmberException(e.getMessage());
        }

        // Send user args to Server.
        List<String> userArgs = new ArrayList<>();
        if (inputColumns != null) userArgs.addAll(inputColumns);
        if (outputColumns != null) userArgs.addAll(outputColumns);
        if (outerFilePaths != null) userArgs.addAll(outerFilePaths);

        Queue<Tuple> argsTuples = new LinkedList<>();
        for (String arg : userArgs) {
            argsTuples.add(new AmberTuple(new String[]{arg}));
        }
        Schema argsSchema = new Schema(
                Collections.singletonList(Field.nullablePrimitive("args", ArrowType.Utf8.INSTANCE))
        );

        try{
            writeArrowStream(flightClient, argsTuples, globalRootAllocator, argsSchema, "args", batchSize);
            flightClient.doAction(new Action("open")).next().getBody();
        }catch(Exception e){
            closeAndThrow(flightClient, e);
        }
    }

    @Override
    public boolean hasNext() {
        return !(outputTupleBuffer == null || outputTupleBuffer.isEmpty());
    }

    @Override
    public Tuple next() {
        return outputTupleBuffer.remove();
    }

    @Override
    public void dispose() {
        closeClientAndServer(flightClient);
    }

    private void processOneBatch() {
        writeArrowStream(flightClient, inputTupleBuffer, globalRootAllocator,
                globalInputSchema, "toPython", batchSize);
        if (outputTupleBuffer == null) {
            outputTupleBuffer = new LinkedList<>();
        }
        executeUDF(flightClient, globalObjectMapper, outputTupleBuffer);
    }

    private static String getPythonResourcePath(String fileName) {
        fileName = fileName.trim();
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return TexeraUtils.amberHomePath().resolve("src/main/resources/python_udf").resolve(fileName).toString();
    }

    private static int getFreeLocalPort() throws IOException {
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

    // FIXME: for now there's only hard-coded tuple metadata.
    private static void convertAmber2ArrowTuple(Tuple tuple, int index, VectorSchemaRoot vectorSchemaRoot) throws Exception {
        List<Field> preDefinedFields = vectorSchemaRoot.getSchema().getFields();
        if (tuple.length() != preDefinedFields.size()) throw new AmberException("Tuple does not match schema!");
        else {
            for (int i = 0; i < preDefinedFields.size(); i++) {
                FieldVector vector = vectorSchemaRoot.getVector(i);
                switch (preDefinedFields.get(i).getFieldType().getType().getTypeID()) {
                    case Int:
                        switch (((ArrowType.Int) (preDefinedFields.get(i).getType())).getBitWidth()) {
                            case 16:
                                ((SmallIntVector) vector).set(index, (short) tuple.get(i));
                                break;
                            case 32:
                                ((IntVector) vector).set(index, (int) tuple.get(i));
                                break;
                            default:
                                ((BigIntVector) vector).set(index, (long) tuple.get(i));
                        }
                        break;
                    case Bool:
                        ((BitVector) vector).set(index, (int) tuple.get(i));
                        break;
                    case Binary:
                        ((FixedSizeBinaryVector) vector).set(index, new byte[]{(byte) tuple.get(i)});
                        break;
                    case FloatingPoint:
                        if (((ArrowType.FloatingPoint) (preDefinedFields.get(i).getType())).getPrecision()
                                == FloatingPointPrecision.SINGLE) {
                            ((Float4Vector) vector).set(index, (float) tuple.get(i));
                        } else {
                            ((Float8Vector) vector).set(index, (double) tuple.get(i));
                        }
                        break;
                    case Utf8:
                        ((VarCharVector) vector).set(
                                index, tuple.get(i).toString().getBytes(StandardCharsets.UTF_8)
                        );
                        break;
                    default:
                        throw new Exception("Unsupported type when converting tuples from Amber to Arrow.");
                }
            }
        }
    }

    private static void convertArrow2AmberTableBuffer(VectorSchemaRoot vectorSchemaRoot, Queue<Tuple> resultQueue)
            throws Exception {
        List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
        List<FieldTypeInJava> amberSchema = convertArrow2AmberSchema(vectorSchemaRoot.getSchema());
        for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
            Tuple tuple;
            List<String> amberFields = new ArrayList<>();

            for (FieldVector vector : fieldVectors) {
                String amberField;
                try {
                    switch (vector.getField().getFieldType().getType().getTypeID()) {
                        case Int:
                            switch (((ArrowType.Int) (vector.getField().getFieldType().getType())).getBitWidth()) {
                                case 16:
                                    amberField = String.valueOf(((SmallIntVector) vector).get(i));
                                    break;
                                case 32:
                                    amberField = String.valueOf(((IntVector) vector).get(i));
                                    break;
                                default:
                                    amberField = String.valueOf(((BigIntVector) vector).get(i));
                            }
                            break;
                        case Bool:
                            amberField = String.valueOf(((BitVector) vector).get(i));
                            break;
                        case Binary:
                            amberField = Arrays.toString(((FixedSizeBinaryVector) vector).get(i));
                            break;
                        case FloatingPoint:
                            if (((ArrowType.FloatingPoint) (vector.getField().getFieldType().getType())).getPrecision()
                                    == FloatingPointPrecision.SINGLE) {
                                amberField = String.valueOf(((Float4Vector) vector).get(i));
                            } else {
                                amberField = String.valueOf(((Float8Vector) vector).get(i));
                            }
                            break;
                        case Utf8:
                            amberField = new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8);
                            break;
                        default:
                            throw new Exception("Unsupported type when converting tuples from Arrow to Amber.");
                    }
                } catch (IllegalStateException | IOException e) {
                    if (!e.getMessage().contains("Value at index is null")) {
                        throw new Exception(e.getMessage(), e);
                    } else {
                       amberField = "";
                    }
                }
                amberFields.add(amberField);
            }
            tuple = new AmberTuple(
                    amberFields.toArray(new String[0]),
                    FieldTypeConverter.convertList(amberSchema.toArray(new FieldTypeInJava[0]))
            );
            resultQueue.add(tuple);
        }
    }

    private static Schema convertAmber2ArrowSchema(Tuple exampleTuple, List<String> names) throws Exception {
        List<Field> arrowFields = new ArrayList<>();
        if (exampleTuple.length() != names.size()) throw new Exception("Number of tuple fields do not match names.");
        for (int i = 0; i < exampleTuple.length(); i++) {
            String name = names.get(i);
            Object fieldData = exampleTuple.get(i);
            Field field;
            if (fieldData.getClass() == Short.class) {
                field = Field.nullablePrimitive(name, new ArrowType.Int(16, true));
            } else if (fieldData.getClass() == Integer.class) {
                field = Field.nullablePrimitive(name, new ArrowType.Int(32, true));
            } else if (fieldData.getClass() == Boolean.class) {
                field = Field.nullablePrimitive(name, ArrowType.Bool.INSTANCE);
            } else if (fieldData.getClass() == Character.class || fieldData.getClass() == Byte.class) {
                field = Field.nullablePrimitive(name, new ArrowType.Binary());
            } else if (fieldData.getClass() == Double.class) {
                field = Field.nullablePrimitive(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            } else if (fieldData.getClass() == Float.class) {
                field = Field.nullablePrimitive(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            } else if (fieldData.getClass() == Long.class) {
                field = Field.nullablePrimitive(name, new ArrowType.Int(64, true));
            } else {
                // All treat as String
                field = Field.nullablePrimitive(name, ArrowType.Utf8.INSTANCE);
            }
            arrowFields.add(field);
        }
        return new Schema(arrowFields);
    }

    private static List<FieldTypeInJava> convertArrow2AmberSchema(Schema arrowSchema) throws Exception {
        List<FieldTypeInJava> amberSchema = new ArrayList<>();
        for(Field f : arrowSchema.getFields()) {
            switch (f.getFieldType().getType().getTypeID()) {
                case Int:
                    switch (((ArrowType.Int) (f.getType())).getBitWidth()) {
                        case 16:
                            amberSchema.add(FieldTypeInJava.SHORT);
                            break;
                        case 32:
                            amberSchema.add(FieldTypeInJava.INT);
                            break;
                        default:
                            amberSchema.add(FieldTypeInJava.LONG);
                    }
                    break;
                case Bool:
                    amberSchema.add(FieldTypeInJava.BOOLEAN);
                    break;
                case Binary:
                    amberSchema.add(FieldTypeInJava.BYTE);
                    break;
                case FloatingPoint:
                    if (((ArrowType.FloatingPoint) (f.getType())).getPrecision() == FloatingPointPrecision.SINGLE) {
                        amberSchema.add(FieldTypeInJava.FLOAT);
                    } else {
                        amberSchema.add(FieldTypeInJava.DOUBLE);
                    }
                    break;
                case Utf8:
                    amberSchema.add(FieldTypeInJava.STRING);
                    break;
                default:
                    throw new Exception("Unsupported type when converting tuples from Arrow to Amber.");
            }
        }
        return amberSchema;
    }

    private static void writeArrowStream(FlightClient client, Queue<Tuple> values, RootAllocator root,
                                         Schema arrowSchema, String descriptorPath, int chunkSize) {
        SyncPutListener flightListener = new SyncPutListener();
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, root);
        FlightClient.ClientStreamListener streamWriter = client.startPut(
                FlightDescriptor.path(Collections.singletonList(descriptorPath)), schemaRoot, flightListener);
        try {
            while (!values.isEmpty()) {
                schemaRoot.allocateNew();
                int indexThisChunk = 0;
                while (indexThisChunk < chunkSize && !values.isEmpty()) {
                    convertAmber2ArrowTuple(values.remove(), indexThisChunk, schemaRoot);
                    indexThisChunk++;
                }
                schemaRoot.setRowCount(indexThisChunk);
                streamWriter.putNext();
                schemaRoot.clear();
            }
            streamWriter.completed();
            flightListener.getResult();
            flightListener.close();
            schemaRoot.clear();
        } catch (Exception e) {
            closeAndThrow(client, e);
        }
    }
    private static void readArrowStream(FlightClient client, String descriptorPath, Queue<Tuple> resultQueue) {
        try {
            FlightInfo info = client.getInfo(FlightDescriptor.path(Collections.singletonList(descriptorPath)));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            FlightStream stream = client.getStream(ticket);
            while (stream.next()) {
                VectorSchemaRoot root  = stream.getRoot(); // get root
                convertArrow2AmberTableBuffer(root, resultQueue);
                root.clear();
            }
        } catch (Exception e) {
            closeAndThrow(client, e);
        }
    }

    // make sure the result queue is empty before being passed here.
    private static void executeUDF(FlightClient client, ObjectMapper mapper, Queue<Tuple> resultQueue) {
        try{
            byte[] resultBytes = client.doAction(new Action("compute")).next().getBody();
            Map<String, String> result = mapper.readValue(resultBytes, Map.class);
            if (result.get("status").equals("Fail")) {
                String errorMessage = result.get("errorMessage");
                throw new Exception(errorMessage);
            }
            readArrowStream(client, "FromPython", resultQueue);
        }catch(Exception e){
            closeAndThrow(client, e);
        }
    }

    private static void closeClientAndServer(FlightClient client) {
        try {
            client.doAction(new Action("close")).next().getBody();
            client.doAction(new Action("shutdown")).next();
            globalRootAllocator.close();
            client.close();
        } catch (Exception e) {
            throw new AmberException(e.getMessage());
        }
    }

    private static void closeAndThrow(FlightClient client, Exception e) {
        closeClientAndServer(client);
        e.printStackTrace();
        throw new AmberException(e.getMessage());
    }
}
