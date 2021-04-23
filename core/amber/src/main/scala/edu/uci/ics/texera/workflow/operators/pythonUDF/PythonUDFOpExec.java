package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.web.WebUtils;
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
import org.jetbrains.annotations.NotNull;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFOpExec.Channel.FROM_PYTHON;
import static edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFOpExec.Channel.TO_PYTHON;
import static edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFOpExec.MSG.*;

public class PythonUDFOpExec implements OperatorExecutor {
    @NotNull
    private static byte[] communicate(@NotNull FlightClient client, @NotNull MSG message) {
        return client.doAction(new Action(message.content)).next().getBody();
    }

    private Process pythonServerProcess;

    private static final int MAX_TRY_COUNT = 20;
    private static final long WAIT_TIME_MS = 500;
    private static final String DAEMON_SCRIPT_PATH = getPythonResourcePath("texera_udf_server_main.py");
    private static final RootAllocator memoryAllocator = new RootAllocator();
    private static final ObjectMapper objectMapper = Utils.objectMapper();

    /**
     * For every batch, the operator converts list of {@code Tuple}s into Arrow stream data in almost the exact same
     * way as it would when using Arrow file, except now it sends stream to the server with
     * {@link FlightClient#startPut(FlightDescriptor, VectorSchemaRoot, FlightClient.PutListener, CallOption...)} and
     * {@link FlightClient.ClientStreamListener#putNext()}. The server uses {@code do_put()} to receive data stream
     * and convert it into a {@code pyarrow.Table} and store it in the server.
     * {@code startPut} is a non-blocking call, but this method in general is a blocking call, it waits until all the
     * data are sent.
     *
     * @param client      The FlightClient that manages this.
     * @param values      The input queue that holds tuples, its contents will be consumed in this method.
     * @param arrowSchema Input Arrow table schema. This should already have been defined (converted).
     * @param channel     The predefined path that specifies where to store the data in Flight Serve.
     * @param chunkSize   The chunk size of the arrow stream. This is different than the batch size of the operator,
     *                    although they may seem similar. This doesn't actually affect serialization speed that much,
     *                    so in general it can be the same as {@code batchSize}.
     */
    private void writeArrowStream(FlightClient client, Queue<Tuple> values,
                                  org.apache.arrow.vector.types.pojo.Schema arrowSchema,
                                  Channel channel, int chunkSize) throws RuntimeException {
        SyncPutListener flightListener = new SyncPutListener();
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, PythonUDFOpExec.memoryAllocator);
        FlightClient.ClientStreamListener streamWriter = client.startPut(FlightDescriptor.path(Collections.singletonList(channel.name)), schemaRoot, flightListener);
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
    private final String PYTHON = WebUtils.config().getString("python.path").trim();
    private String pythonScriptPath;
    private final String pythonScriptText;
    private final ArrayList<String> inputColumns;
    private final ArrayList<Attribute> outputColumns;

    private final ArrayList<String> arguments;
    private final ArrayList<String> outerFilePaths;
    private final int batchSize;
    private final boolean isDynamic;

    private FlightClient flightClient;
    private org.apache.arrow.vector.types.pojo.Schema globalInputSchema;
    private final Queue<Tuple> inputTupleBuffer = new LinkedList<>();

    PythonUDFOpExec(String pythonScriptText, String pythonScriptFile, ArrayList<String> inputColumns,
                    ArrayList<Attribute> outputColumns, ArrayList<String> arguments,
                    ArrayList<String> outerFiles, int batchSize) {
        this.pythonScriptText = pythonScriptText;
        this.pythonScriptPath = pythonScriptFile;
        this.inputColumns = inputColumns;
        this.outputColumns = outputColumns;
        this.arguments = arguments;
        this.outerFilePaths = new ArrayList<>();
        for (String s : outerFiles) outerFilePaths.add(getPythonResourcePath(s));
        this.batchSize = batchSize;
        isDynamic = pythonScriptFile == null || pythonScriptFile.isEmpty();

    }

    /**
     * Make the execution of the UDF in Python. This should only be called
     * after input data is passed to Python. This is a blocking call.
     *
     * @param client The FlightClient that manages this.
     */
    private void executeUDF(FlightClient client) {
        try {
            FlightResponseMap result = PythonUDFOpExec.objectMapper.readValue(communicate(client, MSG.COMPUTE),
                    FlightResponseMap.class);
            if (result.get("status").equals("Fail")) {
                String errorMessage = result.get("errorMessage");
                throw new Exception(errorMessage);
            }
        } catch (Exception e) {
            closeAndThrow(client, e);
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
    private static void convertAmber2ArrowTuple(Tuple tuple, int index, VectorSchemaRoot vectorSchemaRoot)
            throws ClassCastException {

        List<Field> preDefinedFields = vectorSchemaRoot.getSchema().getFields();
        for (int i = 0; i < preDefinedFields.size(); i++) {
            FieldVector vector = vectorSchemaRoot.getVector(i);
            switch (preDefinedFields.get(i).getFieldType().getType().getTypeID()) {
                case Int:
                    ((IntVector) vector).set(index, (int) tuple.get(i));
                    break;
                case Bool:
                    ((BitVector) vector).set(index, (Boolean) tuple.get(i) ? 1 : 0);
                    break;
                case FloatingPoint:
                    ((Float8Vector) vector).set(index, (double) tuple.get(i));
                    break;
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
     * @throws RuntimeException Whatever might happen during this conversion, but especially when tuples have unexpected type.
     */
    private static void convertArrow2AmberTableBuffer(VectorSchemaRoot vectorSchemaRoot, Queue<Tuple> resultQueue)
            throws RuntimeException {
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
                            int bitValue = ((BitVector) vector).get(i);
                            if (bitValue == 0) content = false;
                            else content = true;
                            break;
                        case FloatingPoint:
                            switch (((ArrowType.FloatingPoint) (vector.getField().getFieldType().getType())).getPrecision()) {
                                case HALF:
                                    throw new RuntimeException("HALF floating point number is not supported.");
                                case SINGLE:
                                    content = (double) ((Float4Vector) vector).get(i);
                                    break;
                                default:
                                    content = ((Float8Vector) vector).get(i);
                            }
                            break;
                        case Utf8:
                            content = new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8);
                            break;
                        default:
                            throw new RuntimeException("Unsupported type when converting tuples from Arrow to Amber.");
                    }
                    contents.add(content);
                } catch (Exception e) {
                    if (!e.getMessage().contains("Value at index is null")) {
                        throw new RuntimeException(e.getMessage(), e);
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
     * @throws IOException,RuntimeException Might happen when getting a free port.
     */
    private static int getFreeLocalPort() throws IOException, RuntimeException {
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
                case Int:
                    amberAttributeType = AttributeType.INTEGER;
                    break;
                case Bool:
                    amberAttributeType = AttributeType.BOOLEAN;
                    break;
                case FloatingPoint:
                    amberAttributeType = AttributeType.DOUBLE;
                    break;
                case Utf8:
                    amberAttributeType = AttributeType.STRING;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + f.getFieldType().getType().getTypeID());
            }
            amberAttributes.add(new Attribute(f.getName(), amberAttributeType));
        }
        return new Schema(amberAttributes);
    }

    @Override
    public void close() {
        closeClientAndServer(flightClient, true);
    }

    /**
     * For every batch, the operator gets the computed sentiment result by calling
     * {@link FlightClient#getStream(Ticket, CallOption...)}.
     * The reading and conversion process is the same as what it does when using Arrow file.
     * {@code getStream} is a non-blocking call, but this method is a blocking call because it waits until the stream
     * is finished.
     *
     * @param client      The FlightClient that manages this.
     * @param channel     The predefined path that specifies where to read the data in Flight Serve.
     * @param resultQueue resultQueue To store the results. Must be empty when it is passed here.
     */
    private void readArrowStream(FlightClient client, Channel channel, Queue<Tuple> resultQueue) {
        try {
            FlightInfo info = client.getInfo(FlightDescriptor.path(Collections.singletonList(channel.name)));
            Ticket ticket = info.getEndpoints().get(0).getTicket();
            FlightStream stream = client.getStream(ticket);
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot(); // get root
                convertArrow2AmberTableBuffer(root, resultQueue);
                root.clear();
            }
        } catch (RuntimeException e) {
            closeAndThrow(client, e);
        }
    }

    /**
     * Manages the disposal of this operator.
     * <p>
     * There are two possible scenarios this method will be invoked:
     * 1. An exception was caught, the connection needs to be terminated safely before throw the exception.
     * 2. All the batches are finished, the operator terminates peacefully.
     * <p>
     * When invoked, it first close the context on the Python end, then closes the memory
     * allocator and the client socket. Eventually, it will destroy the Python server process.
     * <p>
     * Since all the Flight RPC methods used here are intrinsically blocking calls, this is
     * also a blocking call.
     *
     * @param client The client to close that is still connected to the Arrow Flight server.
     */
    private void closeClientAndServer(FlightClient client, boolean sendClose) {
        try {
            // close context on the Python end.
            if (sendClose) communicate(client, CLOSE);

            // clean memory allocation.
            memoryAllocator.close();

            // terminate the python server.
            communicate(client, TERMINATE);

            // close client socket.
            client.close();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // python server should terminate by itself peacefully, this is to ensure it gets terminated
            // even in error cases.

            // destroy Python server process
            pythonServerProcess.destroy();
        }
    }

    /**
     * Close everything and throw an exception. This should only be called when an exception occurs and needs to be
     * thrown, but the Arrow Flight Client is still running.
     *
     * @param client    FlightClient.
     * @param exception the exception to be wrapped into Amber Exception.
     */
    private void closeAndThrow(FlightClient client, Exception exception) throws RuntimeException {
        closeClientAndServer(client, false);
        throw new RuntimeException(exception.getMessage());
    }

    private static void deleteTempFile(String filePath) throws IOException {
        File tempFile = new File(filePath);
        if (tempFile.exists()) {
            if (!tempFile.delete()) throw new IOException("Could not delete temp file");
        }
    }

    private void connectToServer(Location flightServerURI) throws InterruptedException {
        boolean connected = false;
        int tryCount = 0;
        while (!connected && tryCount < MAX_TRY_COUNT) {

            try {
                flightClient = FlightClient.builder(memoryAllocator, flightServerURI).build();
                connected = new String(communicate(flightClient, MSG.HEALTH_CHECK), StandardCharsets.UTF_8).equals("Flight Server is up and running!");
                if (!connected) Thread.sleep(WAIT_TIME_MS);
            } catch (FlightRuntimeException e) {
                System.out.println("Flight Client:\tNot connected to the server in this try.");
                flightClient.close();
                Thread.sleep(WAIT_TIME_MS);
                tryCount++;
            }
        }
        if (tryCount == MAX_TRY_COUNT)
            throw new RuntimeException("Exceeded try limit of " + MAX_TRY_COUNT + " when connecting to Flight Server!");
    }

    /**
     * Converts an Amber schema into Arrow schema.
     *
     * @param amberSchema The Amber Tuple Schema.
     * @return An Arrow {@link org.apache.arrow.vector.types.pojo.Schema}.
     */
    private static org.apache.arrow.vector.types.pojo.Schema convertAmber2ArrowSchema(Schema amberSchema)
            throws RuntimeException {
        List<Field> arrowFields = new ArrayList<>();
        for (Attribute amberAttribute : amberSchema.getAttributes()) {
            String name = amberAttribute.getName();
            Field field;
            switch (amberAttribute.getType()) {
                case INTEGER:
                    field = Field.nullablePrimitive(name, new ArrowType.Int(32, true));
                    break;
                case LONG:
                    field = Field.nullablePrimitive(name, new ArrowType.Int(64, true));
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
                    throw new RuntimeException("Unexpected value: " + amberAttribute.getType());
            }
            arrowFields.add(field);

        }
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    private static void safeDeleteTempFile(String fileName) {
        try {
            deleteTempFile(fileName);
        } catch (Exception innerException) {
            innerException.printStackTrace();
        }
    }

    private Iterator<Tuple> processOneBatch(boolean inputExhausted) throws RuntimeException {
        Queue<Tuple> outputTupleBuffer = new LinkedList<>();
        if (inputTupleBuffer.size() != 0) {
            writeArrowStream(flightClient, inputTupleBuffer, globalInputSchema, TO_PYTHON, batchSize);
            executeUDF(flightClient);
            readArrowStream(flightClient, FROM_PYTHON, outputTupleBuffer);
            inputTupleBuffer.clear();
        }

        if (inputExhausted) {
            Queue<Tuple> extraTupleBuffer = new LinkedList<>();
            communicate(flightClient, INPUT_EXHAUSTED);
            readArrowStream(flightClient, FROM_PYTHON, extraTupleBuffer);
            outputTupleBuffer.addAll(extraTupleBuffer);
        }
        return JavaConverters.asScalaIterator(outputTupleBuffer.iterator());
    }

    private void preparePythonScriptFile() throws IOException {
        pythonScriptPath = isDynamic ?
                getPythonResourcePath(String.valueOf(new Random().nextLong())) + ".py"
                : getPythonResourcePath(pythonScriptPath);
        if (isDynamic) {
            // dynamic -> create a temp file and write the code into the file
            writeTempPythonFile();
        } else {
            // static -> check if the script file exists
            File scriptFile = new File(pythonScriptPath);
            if (!scriptFile.exists()) throw new FileNotFoundException("Script file doest not exist!");
        }
    }

    @Override
    public void open() {

        try {
            preparePythonScriptFile();
            Location flightServerURL = startFlightServer();
            connectToServer(flightServerURL);

            // Send user args to Server.
            List<String> userArgs = new ArrayList<>();
            if (inputColumns != null) userArgs.addAll(inputColumns);
            if (arguments != null) userArgs.addAll(arguments);
            if (outputColumns != null) {
                for (Attribute a : outputColumns) userArgs.add(a.getName());
            }
            if (outerFilePaths != null) userArgs.addAll(outerFilePaths);

            Schema argsSchema = new Schema(Collections.singletonList(new Attribute("args", AttributeType.STRING)));
            Queue<Tuple> argsTuples = new LinkedList<>();
            for (String arg : userArgs) {
                argsTuples.add(new Tuple(argsSchema, Collections.singletonList(arg)));
            }

            writeArrowStream(flightClient, argsTuples, convertAmber2ArrowSchema(argsSchema), Channel.ARGS, batchSize);
            communicate(flightClient, MSG.OPEN);
        } catch (IOException | InterruptedException | RuntimeException e) {
            cleanTerminationWithThrow(e);
        }

        // Finally, delete the temp file because it has been loaded in Python.
        if (isDynamic) safeDeleteTempFile(pythonScriptPath);

    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            Tuple inputTuple = tuple.left().get();
            if (globalInputSchema == null) {
                try {
                    globalInputSchema = convertAmber2ArrowSchema(inputTuple.getSchema());
                } catch (RuntimeException exception) {
                    closeAndThrow(flightClient, exception);
                }
            }
            inputTupleBuffer.add(inputTuple);
            if (inputTupleBuffer.size() == batchSize) {
                // This batch is full, execute the UDF.
                return processOneBatch(false);
            } else {
                return JavaConverters.asScalaIterator(Collections.emptyIterator());
            }
        } else {
            // There might be some unprocessed tuples, finish them.
            return processOneBatch(true);
        }
    }

    enum Channel {
        TO_PYTHON("toPython"),
        FROM_PYTHON("fromPython"),
        ARGS("args");
        String name;

        Channel(String name) {
            this.name = name;
        }
    }

    enum MSG {
        OPEN("open"),
        HEALTH_CHECK("health_check"),
        COMPUTE("compute"),
        INPUT_EXHAUSTED("input_exhausted"),
        CLOSE("close"),
        TERMINATE("terminate");

        String content;

        MSG(String content) {
            this.content = content;
        }
    }

    private void writeTempPythonFile() throws IOException {
        File tempScriptFile = new File(pythonScriptPath);

        if (tempScriptFile.createNewFile()) {
            FileWriter fileWriter = new FileWriter(pythonScriptPath);
            fileWriter.write(pythonScriptText);
            fileWriter.close();
        }
    }

    @NotNull
    private Location startFlightServer() throws IOException {
        int portNumber = getFreeLocalPort();
        Location location = new Location(URI.create("grpc+tcp://localhost:" + portNumber));

        // Start Flight server (Python process)
        pythonServerProcess =
                new ProcessBuilder(PYTHON.isEmpty() ? "python3" : PYTHON, // add fall back in case empty
                        "-u", DAEMON_SCRIPT_PATH,
                        Integer.toString(portNumber), pythonScriptPath)
                        .inheritIO()
                        .start();
        return location;
    }

    private void cleanTerminationWithThrow(@NotNull Exception e) {
        e.printStackTrace();
        if (isDynamic) safeDeleteTempFile(pythonScriptPath);
        closeAndThrow(flightClient, e);
    }

    static class FlightResponseMap extends HashMap<String, String> {
    }
}
