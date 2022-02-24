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
    public String originalAttribute;

    @JsonProperty
    @JsonSchemaTitle("Alias")
    @JsonPropertyDescription("Renamed attribute name")

    public String alias;


    public AttributeUnit()
    {

    }

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

        if(alias == null){
            return originalAttribute;
        }
        return alias;
    }
    @JsonIgnore
    public Attribute getAlias(){
        if(alias == null){
            return new Attribute(originalAttribute, AttributeType.STRING);
        }
        return new Attribute(alias, AttributeType.STRING);
    }
    @JsonIgnore
    public Attribute getAliasFromAttribute(String attribute) throws Exception {
        if(! attribute.equalsIgnoreCase(originalAttribute)){
            throw new Exception("original attribute is: " + originalAttribute + "and parameter attribute is" + attribute);
        }
        if(alias == null){
            return new Attribute(originalAttribute, AttributeType.STRING);
        }
        return new Attribute(alias,AttributeType.STRING);

    }


}
