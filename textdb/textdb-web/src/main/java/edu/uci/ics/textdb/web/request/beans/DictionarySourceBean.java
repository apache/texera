package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.plangen.operatorbuilder.DictionarySourceBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the DictionarySource operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/05/16.
 */
@JsonTypeName("DictionarySource")
public class DictionarySourceBean extends OperatorBean {
    @JsonProperty("dictionary")
    private String dictionary;
    @JsonProperty("matching_type")
    private String matchingType;
    @JsonProperty("data_source")
    private String dataSource;

    public DictionarySourceBean() {
    }

    public DictionarySourceBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                                String dictionary, String matchingType, String dataSource) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.dictionary = dictionary;
        this.matchingType = matchingType;
        this.dataSource = dataSource;
    }

    @JsonProperty("dictionary")
    public String getDictionary() {
        return dictionary;
    }

    @JsonProperty("dictionary")
    public void setDictionary(String dictionary) {
        this.dictionary = dictionary;
    }

    @JsonProperty("matching_type")
    public String getMatchingType() {
        return matchingType;
    }

    @JsonProperty("matching_type")
    public void setMatchingType(String matchingType) {
        this.matchingType = matchingType;
    }

    @JsonProperty("data_source")
    public String getDataSource() {
        return dataSource;
    }

    @JsonProperty("data_source")
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getDictionary() == null || this.getMatchingType() == null || this.getDataSource() == null ||
                operatorProperties == null)
            return null;
        operatorProperties.put(DictionarySourceBuilder.DICTIONARY, this.getDictionary());
        operatorProperties.put(DictionarySourceBuilder.MATCHING_TYPE, this.getMatchingType());
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, this.getDataSource());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof DictionarySourceBean)) return false;
        DictionarySourceBean dictionarySourceBean = (DictionarySourceBean) other;

        return new EqualsBuilder()
                .appendSuper(super.equals(dictionarySourceBean))
                .append(dictionary, dictionarySourceBean.getDictionary())
                .append(matchingType, dictionarySourceBean.getMatchingType())
                .append(dataSource, dictionarySourceBean.getDataSource())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(dictionary)
                .append(matchingType)
                .append(dataSource)
                .toHashCode();
    }
}