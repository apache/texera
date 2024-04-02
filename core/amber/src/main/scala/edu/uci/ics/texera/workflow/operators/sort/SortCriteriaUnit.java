package edu.uci.ics.texera.workflow.operators.sort;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;

public class SortCriteriaUnit {

    @JsonProperty(value = "attribute", required = true)
    @JsonPropertyDescription("Attribute name to sort by")
    @AutofillAttributeName
    public String attributeName;

    @JsonProperty(value = "sortPreference", required = true)
    @JsonPropertyDescription("Sort preference (ASC or DESC)")
    public SortPreference sortPreference;
}
