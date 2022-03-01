package edu.uci.ics.texera.workflow.operators.projection;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;

import java.io.Serializable;
import java.util.Locale;

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
    private String alias;


    public AttributeUnit()
    {

    }

    @JsonIgnore
    public AttributeUnit(String attributeName,String alias)
    {
        checkNotNull(attributeName);
        this.originalAttribute = attributeName;
        this.alias = alias;
    }


    @JsonIgnore
    public String getOriginalAttributeAsString(){
        return originalAttribute;
    }

    @JsonIgnore
    public String getAliasAsString(){

        if(alias == null || alias.length() == 0){
            return originalAttribute;
        }
        return alias;
    }

    @JsonIgnore
    public Attribute getAlias(){
        if(alias == null || alias.length() == 0){
            return new Attribute(originalAttribute, AttributeType.STRING);
        }
        return new Attribute(alias, AttributeType.STRING);
    }

    @JsonIgnore
    public Attribute getAliasFromAttribute(String attribute){
        if(alias == null || alias.length() == 0){
            return new Attribute(originalAttribute, AttributeType.STRING);
        }
        return new Attribute(alias,AttributeType.STRING);

    }


}
