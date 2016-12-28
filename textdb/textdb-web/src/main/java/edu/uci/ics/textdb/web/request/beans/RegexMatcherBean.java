package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the RegexMatcher operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 10/17/16.
 */
@JsonTypeName("RegexMatcher")
public class RegexMatcherBean extends OperatorBean {
    @JsonProperty("regex")
    private String regex;

    public RegexMatcherBean() {
    }

    public RegexMatcherBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                            String regex) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.regex = regex;
    }

    @JsonProperty("regex")
    public String getRegex() {
        return regex;
    }

    @JsonProperty("regex")
    public void setRegex(String regex) {
        this.regex = regex;
    }

    @Override
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getRegex() == null || operatorProperties == null)
            return null;
        operatorProperties.put(RegexMatcherBuilder.REGEX, this.getRegex());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof RegexMatcherBean)) return false;
        RegexMatcherBean regexMatcherBean = (RegexMatcherBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(regexMatcherBean))
                .append(regex, regexMatcherBean.getRegex())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(regex)
                .toHashCode();
    }
}