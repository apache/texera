package edu.uci.ics.texera.dataflow.udf;

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
 * @author Qinhua Huang
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
    private boolean handShake() {
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
    
    
    /* Return one of the indications: TAG-WAIT, TAG_NULL or TAG_LEN
     * */
    public String getPythonPID() {
        return readStringFromMMap(outputBuffer, predicate.POSITION_PID);
    }
    
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
 // read a piece of buffer ended with unsigend character
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
    
    private int startPythonProcess() throws IOException {
        String pythonScriptPath = PYTHONSCRIPT;
        
        List<String> args = new ArrayList<String>(
                Arrays.asList(PYTHON, pythonScriptPath, inputFilePath, outputFilePath));
        
        ProcessBuilder processBuilder = new ProcessBuilder(args);
        processBuilder.redirectOutput(new File(pythonDebugOutputFilePath));
        processPython = processBuilder.start();
        return 1;
    }
    
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
    
    @SuppressWarnings("restriction")
    @Override
    protected void setUp() throws TexeraException {
        Schema inputSchema = inputOperator.getOutputSchema();
        Signal.handle(new Signal(predicate.IPC_SIG_STRING), this);
        handShake();
        
        outputSchema = inputSchema;
    }

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

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        return null;
    }

    @Override
    protected void cleanUp() throws TexeraException {        
    }
}
