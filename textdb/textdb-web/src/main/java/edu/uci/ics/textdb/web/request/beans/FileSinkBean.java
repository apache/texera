package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the FileSink operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("FileSink")
public class FileSinkBean extends OperatorBean {
    @JsonProperty("file_path")
    private String filePath;

    public FileSinkBean() {
    }

    public FileSinkBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                        String filePath) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.filePath = filePath;
    }

    @JsonProperty("file_path")
    public String getFilePath() {
        return filePath;
    }

    @JsonProperty("file_path")
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(operatorProperties == null || this.getFilePath() == null)
            return null;
        operatorProperties.put(FileSinkBuilder.FILE_PATH, this.getFilePath());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof FileSinkBean)) return false;
        FileSinkBean fileSinkBean = (FileSinkBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(fileSinkBean))
                .append(filePath, fileSinkBean.getFilePath())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(filePath)
                .toHashCode();
    }
}