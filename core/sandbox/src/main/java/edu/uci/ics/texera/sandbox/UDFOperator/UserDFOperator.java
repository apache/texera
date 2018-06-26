package edu.uci.ics.texera.sandbox.UDFOperator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * @author Avinash Kumar (adaptation of Qinhua Huang's work)
 */

public class UserDFOperator extends AbstractSingleInputOperator implements SignalHandler, ISourceOperator{

    private UserDFOperatorPredicate predicate;
    private Schema outputSchema;
    
    private String inputFilePath = Utils.getResourcePath("input_java_python.txt", TexeraProject.TEXERA_DATAFLOW).toString();
    private String outputFilePath = Utils.getResourcePath("output_java_python.txt", TexeraProject.TEXERA_DATAFLOW).toString();
    private String pythonDebugOutputFilePath = Utils.getResourcePath("debug_output_python.txt", TexeraProject.TEXERA_DATAFLOW).toString();
    
    private String PYTHON = "python3";
    //private String PYTHONSCRIPT = Utils.getResourcePath("udf_operator.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private String PYTHONSCRIPT = Utils.getResourcePath("udf_operator_.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private String PYTHONSCRIPT_BASE = Utils.getResourcePath("udf_operator_base.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private String PYTHONSCRIPT_USER;// = Utils.getResourcePath("udf_operator_user.py", TexeraProject.TEXERA_DATAFLOW).toString();
    
    private boolean getPythonResult = false;
    public Process processPython;
    private String pythonPID;
    
    public Tuple outputTuple;
    
    private MappedByteBuffer inputBuffer;
    private MappedByteBuffer outputBuffer;
    
    private FileChannel inputFileChannel;
    private FileChannel outputFileChannel;
    
    public UserDFOperator(UserDFOperatorPredicate predicate) {
        this.predicate = predicate;
        this.pythonPID = null;
        this.inputBuffer = null;
        this.outputBuffer = null;
        this.PYTHONSCRIPT_USER = Utils.getResourcePath(predicate.getUserDefinedFunctionFile(), TexeraProject.TEXERA_DATAFLOW).toString();
    }
    
    private String getJavaPID() {
        String vmName = ManagementFactory.getRuntimeMXBean().getName();
        int p = vmName.indexOf("@");
        return vmName.substring(0, p);
    }
    
    @SuppressWarnings({ "restriction", "resource" })
    private boolean startAndHandShakeUDFScript() {
        // This function will initiate the communication between Java and the launched process.
        try {
            File inputFile = new File(inputFilePath);
            //Delete the file; we will create a new file
            inputFile.delete();
            // Get file channel in readwrite mode
            inputFileChannel = new RandomAccessFile(inputFile, "rw").getChannel();
            
            // Get direct byte buffer access using channel.map() operation,    320*K bytes
            inputBuffer = inputFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, predicate.mmapBufferSize);
            
            inputBuffer.position(0);
            inputBuffer.put((getJavaPID()+"\n").getBytes());
            
            //build output Buffer mmap
            File outputFile = new File(outputFilePath);
            outputFile.delete();
            outputFileChannel = new RandomAccessFile(outputFile, "rw").getChannel();
            
            outputBuffer = outputFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, predicate.mmapBufferSize);
            
            constructPythonScriptFile();
            startPythonProcess();
            
            while ( true ) {
                Thread.sleep(200);
                if (getPythonResult) {
                    getPythonResult = false;
                    break;
                }
            }
            pythonPID = getPythonPID();
            
            if (pythonPID == null || pythonPID.length() == 0) {
                return false;
            }
        } catch (Exception e) {
            throw new TexeraException("Hands shaking Failed!");
        }
        return true;
    }
    
    private int startPythonProcess() throws IOException {
        String pythonScriptPath = PYTHONSCRIPT;
        
        List<String> args = new ArrayList<String>(
                Arrays.asList(PYTHON, pythonScriptPath, inputFilePath, outputFilePath));
        
        ProcessBuilder processBuilder = new ProcessBuilder(args);
        processBuilder.redirectOutput(new File(pythonDebugOutputFilePath));
        processPython = processBuilder.start();
        return 1;
    }
    
    /***
     * The entire Python script is made from two files. 1. Base file which communicates with Texera's UDF operator. 2. The user function.
     * @throws IOException
     */
    private void constructPythonScriptFile() throws IOException {
        List<String> readSmallTextFile = new ArrayList<>();
        List<String> readSmallTextFile2 = new ArrayList<>();
        
        readSmallTextFile = Files.readAllLines( Paths.get( PYTHONSCRIPT_BASE ), StandardCharsets.UTF_8 );
        readSmallTextFile2 = Files.readAllLines( Paths.get( PYTHONSCRIPT_USER ), StandardCharsets.UTF_8 );
        
        Files.write( Paths.get( PYTHONSCRIPT ), readSmallTextFile, StandardCharsets.UTF_8 );
        Files.write( Paths.get( PYTHONSCRIPT ), readSmallTextFile2, StandardOpenOption.APPEND );
    }
    
    public static void notifyPython(String pythonPID) throws IOException {
        Runtime.getRuntime().exec("kill -SIGUSR2 " + pythonPID);
    }
    
    public String getPythonPID() {
        return readStringFromMMap(outputBuffer, predicate.POSITION_PID);
    }
    

    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {
            throw new TexeraException("Cannot link this operator to another operator after the operator is opened");
        }
        this.inputOperator = operator;
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
        return;
    }
    
    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @SuppressWarnings("restriction")
    @Override
    public void handle(Signal arg0) {
        if (arg0.getNumber() == predicate.IPC_SIG) {
            getPythonResult = true;
        }
    }
    
    /***
     * 1. The setup function registers the UDF operator class as signal handler of USR2 signal which the python process emits. 
     * 2. It then initialises two file channels (Memory-mapped files) - a. input (To give input to the python script) b. output (For python
     * process to write its output to). 
     * 3. Texera writes its Java PID into the input buffer and starts the python process. Texera then waits (polls on getPythonResult variable).
     * 4. Python process starts and reads Texera's PID and saves it. It then writes the Python PID to the output buffer and signals Texera.
     * 5. Upon receiving the signal, getPythonResult variable is set and Texera reads in the Python's PID.
     * 6. SetUp completes.
     */
    @SuppressWarnings("restriction")
    @Override
    protected void setUp() throws TexeraException {
        Schema inputSchema = inputOperator.getOutputSchema();
        Signal.handle(new Signal(predicate.IPC_SIG_STRING), this);
        startAndHandShakeUDFScript();
        
        outputSchema = inputSchema;
    }

    /***
     *    Input Buffer layout:
     *       ----------------------------------------
     *       |            |           |             |
     *       | Texera PID | Input Tag | JSON string |
     *       |            |           |             |
     *       ----------------------------------------
     *    Output Buffer layout:
     *       -----------------------------------------
     *       |            |            |             |
     *       | Python PID | Output Tag | JSON string |
     *       |            |            |             |
     *       -----------------------------------------
     *
     * There are tags which are put into the buffer between Process PID and main content.
     * Input tag (Put by Texera):
     *      TAG_NULL= "": input NULL. All input lines have been exhausted.
     *      TAG_LEN = length of text
     * Output tag (Put by Python script):
     *      TAG_NULL= "": result NULL
     *      TAG_WAIT= Character.unsigned:   need to wait for next
     *      TAG_LEN = length of text
     *      
     * 1. Whenever computeNextMatchingTuple is called, the UDF operator first puts one of the tags (Null/Length of text) into the buffer
     * 2. After that the actual input tuple to the UDF operator (if one exists) is input into the buffer.
     * 3. Python script is signalled and UDF operator begins to poll on 'getPythonResult' flag.
     * 4. The python process processes the tuple and puts its output into the output buffer. It then signals Texera.
     * 5. After being signalled, Texera fetches the contents of output buffer and checks its tag. 
     *    a. If the Tag is TAG_WAIT, it means that the python process needs more tuples to complete its job. Therefore, UDF operator
     *       fetches next tuple and puts it in the input buffer and notifies Python process again.
     *    b. If the Tag is TAG_NULL, it means that the Python process has output nothing. This happens typically when the Python process
     *       has gone through all the tuples and is fed TAG_NULL by Texera.
     *       Upon receiving TAG_NULL, Texera destroys Python process and returns NULL to the output operator.
     *    c. If the Tag is a length, then buffer is read till Unsigned Character is found. This read portion forms the output tuple.
     */
    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {        
        try {
            Tuple inputTuple = inputOperator.getNextTuple();
            //write attribute content to input mmap buffer
            if (inputTuple == null) {
                putTagIntoInputBuffer(predicate.TAG_NULL);
            } else {
                String inputTupleText = new ObjectMapper().writeValueAsString(inputTuple);
                putTagIntoInputBuffer(String.valueOf((new ObjectMapper().writeValueAsString(inputTuple)).length()));
                putJsonIntoInputBuffer(inputTupleText);
            }

            notifyPython( pythonPID.trim() );
            
            for( ; ; ) {
                Thread.sleep(200);
                if (getPythonResult) {
                    getPythonResult = false;
                    break;
                }
            }
            //Output from buffer
            String strLenTag = getTagFromOutputBuffer();
            
            if (strLenTag == predicate.TAG_WAIT) {
                this.getNextTuple();
            }
            
            if(strLenTag == predicate.TAG_NULL) {
                processPython.destroy();
                return null;
            }
            
            String outputTupleJsonStr = getJsonFromOutputBuffer();
            
            outputTuple = new ObjectMapper().readValue(outputTupleJsonStr.trim(), Tuple.class);
            outputSchema = outputTuple.getSchema();
        } catch (Exception e) {
            throw new TexeraException("MMap Operation Failed!");
        }
        return outputTuple;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if(inputSchema.length != 1){
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));
        }

        return getOutputSchema();
    }
    
    /* Return one of the indications: TAG-WAIT, TAG_NULL or TAG_LEN
     * */
    public String getTagFromOutputBuffer() {
        String strTag = "";
        outputBuffer.position(predicate.POSITION_TAG);
        while (true)
        {
            char ch;
            if ((ch = (char) outputBuffer.get()) == Character.UNASSIGNED) {
                break;
            } else if (ch == 'w') {
                strTag = predicate.TAG_WAIT;
                break;
            }
            strTag += ch;
        }
        if(strTag.startsWith("0")) {
            strTag =  predicate.TAG_NULL;
        }
        //return the length of Tuple Json string.
        return strTag;
    }
    
    public void putTagIntoInputBuffer(String tag) {
        inputBuffer.position(predicate.POSITION_TAG);
        inputBuffer.put((tag + "\n").getBytes());
        inputBuffer.putChar((char) Character.UNASSIGNED);
    }
    
    public void putJsonIntoInputBuffer(String stringJson) {
        inputBuffer.position(predicate.POSITION_JSON);
        inputBuffer.put((stringJson).getBytes());
        inputBuffer.putChar((char) Character.UNASSIGNED);
    }
    
    public String getJsonFromOutputBuffer() {
        return readStringFromMMap(outputBuffer, predicate.POSITION_JSON);
    }
    
    /***
     * read the portion of buffer starting at startPod and ending with unsigned character
     * @param outputBuffer
     * @param startPos
     * @return string
     */
    public String readStringFromMMap(MappedByteBuffer outputBuffer, int startPos) {
        String str = "";
        outputBuffer.position(startPos);
        while (true)
        {
            char ch;
            if ((ch = (char) outputBuffer.get()) == Character.UNASSIGNED) {
                break;
            }
            str += ch;
        }
        if (str.trim().length() == 0) {
            return null;
        }
        return str.trim();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        return null;
    }

    @Override
    protected void cleanUp() throws TexeraException {        
    }

}
