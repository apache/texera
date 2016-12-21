package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate.NlpTokenType;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the NlpExtractor operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("NlpExtractor")
public class NlpExtractorBean extends OperatorBean {
    @JsonProperty("nlp_type")
    private String nlpTokenType;

    public NlpExtractorBean() {
    }

    public NlpExtractorBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                            String nlpTokenType) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.nlpTokenType = nlpTokenType;
    }

    @JsonProperty("nlp_type")
    public String getNlpTokenType() {
        return nlpTokenType;
    }

    @JsonProperty("nlp_type")
    public void setNlpTokenType(String nlpTokenType) {
        this.nlpTokenType = nlpTokenType;
    }

    @Override
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getNlpTokenType() == null || operatorProperties == null)
            return null;
        operatorProperties.put(NlpExtractorBuilder.NLP_TYPE, this.getNlpTokenType());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof NlpExtractorBean)) return false;
        NlpExtractorBean nlpExtractorBean = (NlpExtractorBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(nlpExtractorBean))
                .append(nlpTokenType, nlpExtractorBean.getNlpTokenType())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(nlpTokenType)
                .toHashCode();
    }
}