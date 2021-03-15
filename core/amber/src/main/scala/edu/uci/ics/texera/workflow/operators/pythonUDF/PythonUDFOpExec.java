package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.Utils;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PythonUDFOpExec implements OperatorExecutor {
    private String pythonScriptPath;
    private final String pythonScriptText;
    private final ArrayList<String> inputColumns;
    private final ArrayList<Attribute> outputColumns;
    private final ArrayList<String> outerFilePaths;
    private final int batchSize;
    private final boolean isDynamic;

    private static final int MAX_TRY_COUNT = 20;
    private static final long WAIT_TIME_MS = 500;
    private static final String PYTHON = "python3";
    private static final String DAEMON_SCRIPT_PATH = getPythonResourcePath("texera_udf_server_main.py");

    private static final RootAllocator globalRootAllocator = new RootAllocator();
    private static final ObjectMapper globalObjectMapper = Utils.objectMapper();
    private FlightClient flightClient;
    private org.apache.arrow.vector.types.pojo.Schema globalInputSchema;

    private Queue<Tuple> inputTupleBuffer;
    private Queue<Tuple> outputTupleBuffer;

    PythonUDFOpExec(String pythonScriptText, String pythonScriptFile, ArrayList<String> inputColumns,
                            ArrayList<Attribute> outputColumns, ArrayList<String> outerFiles, int batchSize) {
        this.pythonScriptText = pythonScriptText;
        this.pythonScriptPath = pythonScriptFile;
        this.inputColumns = inputColumns;
        this.outputColumns = outputColumns;
        this.outerFilePaths = new ArrayList<>();
        for (String s : outerFiles) outerFilePaths.add(getPythonResourcePath(s));
        this.batchSize = batchSize;
        isDynamic = pythonScriptFile == null || pythonScriptFile.isEmpty();
    }


    @Override
    public void open() {
        try {
            pythonScriptPath = isDynamic ?
                    getPythonResourcePath(String.valueOf(new Random().nextLong())) + ".py"
                    : getPythonResourcePath(pythonScriptPath);
            if (isDynamic) {
                // dynamic -> create a temp file and write the code into the file
                File tempScriptFile = new File(pythonScriptPath);

                if (tempScriptFile.createNewFile()) {
                    FileWriter fileWriter = new FileWriter(pythonScriptPath);
                    fileWriter.write(pythonScriptText);
                    fileWriter.close();
                }
            } else {
                // static -> check if the script file exists
                File scriptFile = new File(pythonScriptPath);
                if (!scriptFile.exists()) throw new Exception("Script file doest not exist!");
            }

            int portNumber = getFreeLocalPort();
            Location location = new Location(URI.create("grpc+tcp://localhost:" + portNumber));
            List<String> args = new ArrayList<>(
                    Arrays.asList(PYTHON, DAEMON_SCRIPT_PATH, Integer.toString(portNumber), pythonScriptPath)
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
                throw new RuntimeException("Exceeded try limit of " + MAX_TRY_COUNT +" when connecting to Flight Server!");
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (isDynamic) deleteTempFile(pythonScriptPath);
            } catch (Exception innerException) {
                innerException.printStackTrace();
            }
        }

        // Send user args to Server.
        List<String> userArgs = new ArrayList<>();
        if (inputColumns != null) userArgs.addAll(inputColumns);
        if (outputColumns != null) {
            for (Attribute a : outputColumns) userArgs.add(a.getName());
        }
        if (outerFilePaths != null) userArgs.addAll(outerFilePaths);

        Schema argsSchema = new Schema(Collections.singletonList(new Attribute("args", AttributeType.STRING)));
        Queue<Tuple> argsTuples = new LinkedList<>();
        for (String arg : userArgs) {
            argsTuples.add(new Tuple(argsSchema, Collections.singletonList(arg)));
        }

        try {
            writeArrowStream(flightClient, argsTuples, globalRootAllocator, convertAmber2ArrowSchema(argsSchema), "args", batchSize);
            flightClient.doAction(new Action("open")).next().getBody();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (isDynamic) deleteTempFile(pythonScriptPath);
            } catch (Exception innerException) {
                innerException.printStackTrace();
            }
            closeAndThrow(flightClient, e);
        }
        // Finally, delete the temp file because it has been loaded in Python.
        try {
            if (isDynamic) deleteTempFile(pythonScriptPath);
        } catch (Exception e) {
            closeAndThrow(flightClient, e);
        }
    }

    @Override
    public void close() {
        closeClientAndServer(flightClient, true);
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            Tuple inputTuple = tuple.left().get();
            if (inputTupleBuffer == null) {
                // The first time, initialize this buffer.
                inputTupleBuffer = new LinkedList<>();
                try {
                    globalInputSchema = convertAmber2ArrowSchema(inputTuple.getSchema());
                } catch (Exception e) {
                    closeAndThrow(flightClient, e);
                }
            }
            inputTupleBuffer.add(inputTuple);
            if (inputTupleBuffer.size() == batchSize) {
                // This batch is full, execute the UDF.
                outputTupleBuffer = new LinkedList<>();
                processOneBatch();
                return JavaConverters.asScalaIterator(outputTupleBuffer.iterator());
            }
        }
        else {
            if (inputTupleBuffer != null && !inputTupleBuffer.isEmpty()) {
                // There are some unprocessed tuples, finish them.
                outputTupleBuffer = new LinkedList<>();
                processOneBatch();
                return JavaConverters.asScalaIterator(outputTupleBuffer.iterator());
            }
        }
        return JavaConverters.asScalaIterator(Collections.emptyIterator());
    }

    private void processOneBatch() {
        writeArrowStream(flightClient, inputTupleBuffer, globalRootAllocator, globalInputSchema, "toPython", batchSize);
        executeUDF(flightClient, globalObjectMapper, outputTupleBuffer);
    }

    /**
     * Generate the absolute path in the Python UDF folder from a file name.
     *
     * @param fileName Input file name, not a path.
     * @return The absolute path in the Python UDF folder.
     */
    private static String getPythonResourcePath(String fileName) {
        fileName = fileName.trim();
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return Utils.amberHomePath().resolve("src/main/resources/python_udf").resolve(fileName).toString();
    }

    /**
     * Get a random free port.
     *
     * @return The port number.
     * @throws IOException Might happen when getting a free port.
     */
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

    /**
     * Does the actual conversion (serialization) of data tuples. This is a tuple-by-tuple method, because this method
     * will be used in different places.
     *
     * @param tuple            Input tuple.
     * @param index            Index of the input tuple in the table (buffer).
     * @param vectorSchemaRoot This should store the Arrow schema, which should already been converted from Amber.
     */
    private static void convertAmber2ArrowTuple(Tuple tuple, int index, VectorSchemaRoot vectorSchemaRoot) {

        List<Field> preDefinedFields = vectorSchemaRoot.getSchema().getFields();
        for (int i = 0; i < preDefinedFields.size(); i++) {
            FieldVector vector = vectorSchemaRoot.getVector(i);
            switch (preDefinedFields.get(i).getFieldType().getType().getTypeID()) {
                case Int: ((IntVector) vector).set(index, (int) tuple.get(i)); break;
                case Bool: ((BitVector) vector).set(index, (int) tuple.get(i)); break;
                case FloatingPoint: ((Float8Vector) vector).set(index, (double) tuple.get(i)); break;
                case Utf8:
                    ((VarCharVector) vector).set(index, tuple.get(i).toString().getBytes(StandardCharsets.UTF_8));
                    break;
            }
        }
    }

    /**
     * Does the actual conversion (deserialization) of data table. This is a table(buffer)-wise method.
     *
     * @param vectorSchemaRoot This should contain the data buffer.
     * @param resultQueue      This should be empty before input.
     * @throws Exception Whatever might happen during this conversion, but especially when tuples have unexpected type.
     */
    private static void convertArrow2AmberTableBuffer(VectorSchemaRoot vectorSchemaRoot, Queue<Tuple> resultQueue)
            throws Exception {
        List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
        Schema amberSchema = convertArrow2AmberSchema(vectorSchemaRoot.getSchema());
        for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
            List<Object> contents = new ArrayList<>();
            for (FieldVector vector : fieldVectors) {
                try {
                    Object content;
                    switch (vector.getField().getFieldType().getType().getTypeID()) {
                        case Int:
                            switch (((ArrowType.Int) (vector.getField().getFieldType().getType())).getBitWidth()) {
                                case 16:
                                    content = (int) ((SmallIntVector) vector).get(i);
                                    break;
                                case 32:
                                    content = ((IntVector) vector).get(i);
                                    break;
                                case 64:
                                default:
                                    content = (int) ((BigIntVector) vector).get(i);
                        }
                            break;
                        case Bool:
                            int bitValue =  ((BitVector) vector).get(i);
                            if (bitValue == 0) content = false;
                            else content = true;
                            break;
                        case FloatingPoint:
                            switch (((ArrowType.FloatingPoint) (vector.getField().getFieldType().getType())).getPrecision()) {
                                case HALF:
                                    throw new Exception("HALF floating point number is not supported.");
                                case SINGLE:
                                    content = (double) ((Float4Vector) vector).get(i);
                                break;
                                default: content = ((Float8Vector) vector).get(i);
                        }
                            break;
                        case Utf8:
                            content = new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8);
                            break;
                        default: throw new Exception("Unsupported type when converting tuples from Arrow to Amber.");
                    }
                    contents.add(content);
                } catch (Exception e) {
                    if (!e.getMessage().contains("Value at index is null")) {
                        throw new Exception(e.getMessage(), e);
                    } else {
                        contents.add(null);
                    }
                }
            }
            Tuple result = new Tuple(amberSchema, contents);
            resultQueue.add(result);
        }
    }

    /**
     * Converts an Amber schema into Arrow schema.
     *
     * @param amberSchema The Amber Tuple Schema.
     * @return An Arrow {@link org.apache.arrow.vector.types.pojo.Schema}.
     */
    private static org.apache.arrow.vector.types.pojo.Schema convertAmber2ArrowSchema(Schema amberSchema) {
        List<Field> arrowFields = new ArrayList<>();
        for (Attribute amberAttribute : amberSchema.getAttributes()) {
            String name = amberAttribute.getName();
            Field field;
            switch (amberAttribute.getType()) {
                case INTEGER:
                    field = Field.nullablePrimitive(name, new ArrowType.Int(32, true));
                    break;
                case DOUBLE:
                    field = Field.nullablePrimitive(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
                    break;
                case BOOLEAN:
                    field = Field.nullablePrimitive(name, ArrowType.Bool.INSTANCE);
                    break;
                case STRING:
                case ANY:
                    field = Field.nullablePrimitive(name, ArrowType.Utf8.INSTANCE);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + amberAttribute.getType());
            }
            arrowFields.add(field);

        }
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    /**
     * Converts an Arrow table schema into Amber schema.
     *
     * @param arrowSchema The arrow table schema to be converted.
     * @return The Amber Schema converted from Arrow Table Schema.
     */
    private static Schema convertArrow2AmberSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        List<Attribute> amberAttributes = new ArrayList<>();
        for (Field f : arrowSchema.getFields()) {
            AttributeType amberAttributeType;
            switch (f.getFieldType().getType().getTypeID()) {
                case Int: amberAttributeType = AttributeType.INTEGER; break;
                case Bool: amberAttributeType = AttributeType.BOOLEAN; break;
                case FloatingPoint: amberAttributeType = AttributeType.DOUBLE; break;
                case Utf8: amberAttributeType = AttributeType.STRING; break;
                default:
                    throw new IllegalStateException("Unexpected value: " + f.getFieldType().getType().getTypeID());
            }
            amberAttributes.add(new Attribute(f.getName(), amberAttributeType));
        }
        return new Schema(amberAttributes);
    }

    /**
     * For every batch, the operator converts list of {@code Tuple}s into Arrow stream data in almost the exact same
     * way as it would when using Arrow file, except now it sends stream to the server with
     * {@link FlightClient#startPut(FlightDescriptor, VectorSchemaRoot, FlightClient.PutListener, CallOption...)} and
     * {@link FlightClient.ClientStreamListener#putNext()}. The server uses {@code do_put()} to receive data stream
     * and convert it into a {@code pyarrow.Table} and store it in the server.
     * {@code startPut} is a non-blocking call, but this method in general is a blocking call, it waits until all the
     * data are sent.
     *
     * @param client         The FlightClient that manages this.
     * @param values         The input queue that holds tuples.
     * @param root           Root allocator that manages memory issues in Arrow.
     * @param arrowSchema    Input Arrow table schema. This should already have been defined (converted).
     * @param descriptorPath The predefined path that specifies where to store the data in Flight Serve.
     * @param chunkSize      The chunk size of the arrow stream. This is different than the batch size of the operator,
     *                       although they may seem similar. This doesn't actually affect serialization speed that much,
     *                       so in general it can be the same as {@code batchSize}.
     */
    private static void writeArrowStream(FlightClient client, Queue<Tuple> values, RootAllocator root,
                                         org.apache.arrow.vector.types.pojo.Schema arrowSchema,
                                         String descriptorPath, int chunkSize) {
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

    /**
     * For every batch, the operator gets the computed sentiment result by calling
     * {@link FlightClient#getStream(Ticket, CallOption...)}.
     * The reading and conversion process is the same as what it does when using Arrow file.
     * {@code getStream} is a non-blocking call, but this method is a blocking call because it waits until the stream
     * is finished.
     *
     * @param client         The FlightClient that manages this.
     * @param descriptorPath The predefined path that specifies where to read the data in Flight Serve.
     * @param resultQueue    resultQueue To store the results. Must be empty when it is passed here.
     */
    private static void readArrowStream(FlightClient client, String descriptorPath, Queue<Tuple> resultQueue) {
        try {
            FlightInfo info = client.getInfo(FlightDescriptor.path(Collections.singletonList(descriptorPath)));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            FlightStream stream = client.getStream(ticket);
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot(); // get root
                convertArrow2AmberTableBuffer(root, resultQueue);
                root.clear();
            }
        } catch (Exception e) {
            closeAndThrow(client, e);
        }
    }

    /**
     * Make the execution of the UDF in Python and read the results that are passed back. This should only be called
     * after input data is passed to Python. This is a blocking call.
     *
     * @param client      The FlightClient that manages this.
     * @param mapper      Used to decode the result status message (Json).
     * @param resultQueue To store the results. Must be empty when it is passed here.
     */
    private static void executeUDF(FlightClient client, ObjectMapper mapper, Queue<Tuple> resultQueue) {
        try {
            byte[] resultBytes = client.doAction(new Action("compute")).next().getBody();
            Map<String, String> result = mapper.readValue(resultBytes, Map.class);
            if (result.get("status").equals("Fail")) {
                String errorMessage = result.get("errorMessage");
                throw new Exception(errorMessage);
            }
            readArrowStream(client, "fromPython", resultQueue);
        } catch (Exception e) {
            closeAndThrow(client, e);
        }
    }

    /**
     * Manages the disposal of this operator. When all the batches are finished and the operator disposes, it issues a
     * {@code flightClient.doAction(new Action("shutdown"))} call to shut down the server, and also closes the root
     * allocator and the client. Since all the Flight RPC methods used here are intrinsically blocking calls, this is
     * also a blocking call.
     *
     * @param client The client to close that is still connected to the Arrow Flight server.
     */
    private static void closeClientAndServer(FlightClient client, boolean closeUDF) {
        try {
            if (closeUDF) client.doAction(new Action("close")).next().getBody();
            client.doAction(new Action("shutdown")).next();
            globalRootAllocator.close();
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close everything and throw an exception. This should only be called when an exception occurs and needs to be
     * thrown, but the Arrow Flight Client is still running.
     *
     * @param client FlightClient.
     * @param e      the exception to be wrapped into Amber Exception.
     */
    private static void closeAndThrow(FlightClient client, Exception e) {
        closeClientAndServer(client, false);
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
    }

    private static void deleteTempFile(String filePath) throws Exception {
        File tempFile = new File(filePath);
        if (tempFile.exists()) {
            if (!tempFile.delete()) throw new Exception("Could not delete temp file");
        }
    }
}
