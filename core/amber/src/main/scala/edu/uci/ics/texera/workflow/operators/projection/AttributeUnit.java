package edu.uci.ics.texera.workflow.operators.projection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import static com.google.common.base.Preconditions.checkNotNull;

public class AttributeUnit{
    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute")
    @JsonPropertyDescription("Attribute name in the schema")
    @AutofillAttributeName
    private String originalAttribute;

    @JsonProperty
    @JsonSchemaTitle("Alias")
    @JsonPropertyDescription("Renamed attribute name")
    private scala.Option<String> alias;

    public AttributeUnit()
    {
        ;
    }

    AttributeUnit(String attributeName, String alias) {
        checkNotNull(attributeName);
        this.originalAttribute = attributeName;
        this.alias = scala.Some.apply(alias);
    }


    String getOriginalAttribute(){
        return originalAttribute;
    }


    String getAlias(){
        if(alias.get().length() == 0 || alias == null){
            return originalAttribute;
        }
        return alias.get();
    }


}
