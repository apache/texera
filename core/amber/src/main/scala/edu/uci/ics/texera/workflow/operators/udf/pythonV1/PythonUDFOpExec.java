package edu.uci.ics.texera.workflow.operators.udf.pythonV1;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.amber.engine.architecture.pythonworker.ArrowUtils;
import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.Utils;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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

import static edu.uci.ics.texera.workflow.operators.udf.pythonV1.PythonUDFOpExec.Channel.FROM_PYTHON;
import static edu.uci.ics.texera.workflow.operators.udf.pythonV1.PythonUDFOpExec.Channel.TO_PYTHON;
import static edu.uci.ics.texera.workflow.operators.udf.pythonV1.PythonUDFOpExec.MSG.*;

@Deprecated
public class PythonUDFOpExec implements OperatorExecutor {


    private static final int MAX_TRY_COUNT = 20;
    private static final long WAIT_TIME_MS = 500;
    private static final RootAllocator memoryAllocator = new RootAllocator();
    private final String pythonScriptText;
    private final List<String> inputColumns;
    private final List<Attribute> outputColumns;
    private final List<String> arguments;
    private final List<String> outerFilePaths;
    private final int batchSize;
    private final boolean isDynamic;
    private final Queue<Tuple> inputTupleBuffer = new LinkedList<>();
    private Process pythonServerProcess;
    private String pythonScriptPath;
    private FlightClient flightClient;
    private org.apache.arrow.vector.types.pojo.Schema globalInputSchema;

    PythonUDFOpExec(String pythonScriptText, String pythonScriptFile, List<String> inputColumns,
                    List<Attribute> outputColumns, List<String> arguments,
                    List<String> outerFiles, int batchSize) {
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

    private static byte[] communicate(FlightClient client, MSG message) {
        return client.doAction(new Action(message.content)).next().getBody();
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


    private static void deleteTempFile(String filePath) throws IOException {
        File tempFile = new File(filePath);
        if (tempFile.exists()) {
            if (!tempFile.delete()) throw new IOException("Could not delete temp file");
        }
    }


    private static void safeDeleteTempFile(String fileName) {
        try {
            deleteTempFile(fileName);
        } catch (Exception innerException) {
            innerException.printStackTrace();
        }
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
                while (schemaRoot.getRowCount() < chunkSize && !values.isEmpty()) {
                    ArrowUtils.appendTexeraTuple(values.remove(), schemaRoot);
                }
                streamWriter.putNext();
                schemaRoot.clear();
            }
            streamWriter.completed();
            flightListener.getResult();
            flightListener.close();
            schemaRoot.clear();
        } catch (Exception e) {
            e.printStackTrace();
            closeAndThrow(client, e);
        }
    }

    /**
     * Make the execution of the UDF in Python. This should only be called
     * after input data is passed to Python. This is a blocking call.
     *
     * @param client The FlightClient that manages this.
     */
    private void executeUDF(FlightClient client) {
        try {
            communicate(client, MSG.COMPUTE);
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
                for (int i = 0; i < root.getRowCount(); i++) {
                    resultQueue.add(ArrowUtils.getTexeraTuple(i, root));
                }
                root.clear();
            }
        } catch (RuntimeException e) {
            System.out.println("NO SUCH FLIGHT!!");
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

            // terminate the python server.
            communicate(client, TERMINATE);

            // clean memory allocation.
            memoryAllocator.close();

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
        exception.printStackTrace();
        closeClientAndServer(client, false);
        throw new RuntimeException(exception);
    }

    private void connectToServer(Location flightServerURI) throws InterruptedException {
        boolean connected = false;
        int tryCount = 0;
        while (!connected && tryCount < MAX_TRY_COUNT) {

            try {
                flightClient = FlightClient.builder(memoryAllocator, flightServerURI).build();
                connected = new String(communicate(flightClient, MSG.HEALTH_CHECK), StandardCharsets.UTF_8).equals("success");
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

            sendArgs();
            sendConf();

            communicate(flightClient, MSG.OPEN);
        } catch (IOException | InterruptedException | RuntimeException e) {
            cleanTerminationWithThrow(e);
        }

        // Finally, delete the temp file because it has been loaded in Python.
        if (isDynamic) safeDeleteTempFile(pythonScriptPath);

    }

    @Override
    public void close() {
        closeClientAndServer(flightClient, true);
    }

    private void sendArgs() {
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

        writeArrowStream(flightClient, argsTuples, ArrowUtils.fromTexeraSchema(argsSchema), Channel.ARGS, batchSize);
    }

    private void sendConf() {

        Schema confSchema = new Schema(Collections.singletonList(new Attribute("conf", AttributeType.STRING)));
        Queue<Tuple> confTuples = new LinkedList<>();

        // TODO: add configurations to be sent
        writeArrowStream(flightClient, confTuples, ArrowUtils.fromTexeraSchema(confSchema), Channel.CONF, batchSize);

    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            Tuple inputTuple = tuple.left().get();
            if (globalInputSchema == null) {
                try {
                    globalInputSchema = ArrowUtils.fromTexeraSchema(inputTuple.getSchema());
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

    private void writeTempPythonFile() throws IOException {
        File tempScriptFile = new File(pythonScriptPath);

        if (tempScriptFile.createNewFile()) {
            FileWriter fileWriter = new FileWriter(pythonScriptPath);
            fileWriter.write(pythonScriptText);
            fileWriter.close();
        }
    }


    private Location startFlightServer() throws IOException {
        int portNumber = getFreeLocalPort();
        Location location = new Location(URI.create("grpc+tcp://localhost:" + portNumber));

        // Start Flight server (Python process)
        String udfMainScriptPath = getPythonResourcePath("texera_udf_main.py");

        // TODO: find a better way to do default conf values.

        Config config = ConfigFactory.load("python_udf");
        String pythonPath = config.getString("python.path").trim();

        String logStreamHandlerLevel = config.getString("python.log.streamHandler.level").trim();
        String logStreamHandlerFormat = config.getString("python.log.streamHandler.format").trim();

        String logFileHandlerDir = config.getString("python.log.fileHandler.dir").trim();
        String logFileHandlerLevel = config.getString("python.log.fileHandler.level").trim();
        String logFileHandlerFormat = config.getString("python.log.fileHandler.format").trim();

        pythonServerProcess =
                new ProcessBuilder(pythonPath.isEmpty() ? "python3" : pythonPath, // add fall back in case of empty
                        "-u",
                        udfMainScriptPath,
                        Integer.toString(portNumber),

                        logStreamHandlerLevel.isEmpty() ? "INFO" : logStreamHandlerLevel,
                        logStreamHandlerFormat.isEmpty() ? "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | " +
                                "<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - " +
                                "<level>{message}</level>" : logStreamHandlerFormat,

                        logFileHandlerDir.isEmpty() ? "/tmp/" : logFileHandlerDir,
                        logFileHandlerLevel.isEmpty() ? "INFO" : logFileHandlerLevel,
                        logFileHandlerFormat.isEmpty() ? "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | " +
                                "<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - " +
                                "<level>{message}</level>" : logFileHandlerFormat,
                        pythonScriptPath)
                        .inheritIO()
                        .start();
        return location;
    }

    private void cleanTerminationWithThrow(Exception e) {
        if (isDynamic) safeDeleteTempFile(pythonScriptPath);
        closeAndThrow(flightClient, e);
    }

    enum Channel {
        TO_PYTHON("toPython"),
        FROM_PYTHON("fromPython"),
        ARGS("args"),
        CONF("conf");

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

}
