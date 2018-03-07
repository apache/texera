package edu.uci.ics.texera.dataflow.udf;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.sampler.SamplerPredicate.SampleType;

public class UserDFOperatorPredicate extends PredicateBase{
	/* Input Buffer file position definition:
	 * 0: javaPID      10: Tag to indicate read or write   20: start of Json string
	 * 
	 * Output Buffer file position definition:
	 * 0: javaPID      10: Tag to indicate read or write   20: start of Json string
	 */
	public final int POSITION_PID = 0;
	public final int POSITION_TAG = 10;
	public final int POSITION_JSON = 20;
	
	
	/* Input tag:
     *      TAG_NULL= "": result NULL
     *      TAG_LEN = length of text
     * Output tag:
     *      TAG_NULL= "": result NULL
     *      TAG_WAIT= Character.unsigned:   need to wait for next
     *      TAG_LEN = length of text
     */
	public final String TAG_NULL = "0\n";
	public final String TAG_WAIT = "w";
	public String TAG_LEN;
	
	// mmap size
	final int mmapBufferSize = 1024* 8;
    
    //Signal used between processes
	public final int IPC_SIG = 12;
	public final String IPC_SIG_STRING = "USR2";
    
	public String userDefinedFunctionFile;
    public UserDFOperatorPredicate(@JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
    String userDefinedFunctionFile) {
        this.userDefinedFunctionFile = userDefinedFunctionFile;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getUserDefinedFunctionFile() {
        return userDefinedFunctionFile;
    }
    
    @Override
    public IOperator newOperator() {
        return new UserDFOperator(this);
    }
    
}
