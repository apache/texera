package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the IndexSink operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("IndexSink")
public class IndexSinkBean extends OperatorBean {
    @JsonProperty("index_path")
    private String indexPath;
    @JsonProperty("index_name")
    private String indexName;

    public IndexSinkBean() {
    }

    public IndexSinkBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                         String indexPath, String indexName) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.indexPath = indexPath;
        this.indexName = indexName;
    }

    @JsonProperty("index_path")
    public String getIndexPath() {
        return indexPath;
    }

    @JsonProperty("index_path")
    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    @JsonProperty("index_name")
    public String getIndexName() {
        return indexName;
    }

    @JsonProperty("index_name")
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(operatorProperties == null)
            return null;
        //TODO - Check on properties for IndexSink, IndexSinkBuilder seems to be missing
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof IndexSinkBean)) return false;
        IndexSinkBean indexSinkBean = (IndexSinkBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(indexSinkBean))
                .append(indexPath, indexSinkBean.getIndexPath())
                .append(indexName, indexSinkBean.getIndexName())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(indexPath)
                .append(indexName)
                .toHashCode();
    }
}