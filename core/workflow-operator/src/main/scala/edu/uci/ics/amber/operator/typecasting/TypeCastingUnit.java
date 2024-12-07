package edu.uci.ics.amber.operator.typecasting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.core.tuple.AttributeType;
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName;

@JsonSchemaInject(json =
        "{" +
                "  \"attributeTypeRules\": {" +
                "    \"attribute\": {" +
                "      \"allOf\": [" +
                "        {" +
                "          \"if\": {" +
                "            \"resultType\": {" +
                "              \"valEnum\": [\"integer\"]" +
                "            }" +
                "          }," +
                "          \"then\": {" +
                "            \"enum\": [\"string\", \"long\", \"double\", \"boolean\"]" +
                "          }" +
                "        }," +
                "        {" +
                "          \"if\": {" +
                "            \"resultType\": {" +
                "              \"valEnum\": [\"double\"]" +
                "            }" +
                "          }," +
                "          \"then\": {" +
                "            \"enum\": [\"string\", \"integer\", \"long\", \"boolean\"]" +
                "          }" +
                "        }," +
                "        {" +
                "          \"if\": {" +
                "            \"resultType\": {" +
                "              \"valEnum\": [\"boolean\"]" +
                "            }" +
                "          }," +
                "          \"then\": {" +
                "            \"enum\": [\"string\", \"integer\", \"long\", \"double\"]" +
                "          }" +
                "        }," +
                "        {" +
                "          \"if\": {" +
                "            \"resultType\": {" +
                "              \"valEnum\": [\"long\"]" +
                "            }" +
                "          }," +
                "          \"then\": {" +
                "            \"enum\": [\"string\", \"integer\", \"double\", \"boolean\", \"timestamp\"]" +
                "          }" +
                "        }," +
                "        {" +
                "          \"if\": {" +
                "            \"resultType\": {" +
                "              \"valEnum\": [\"timestamp\"]" +
                "            }" +
                "          }," +
                "          \"then\": {" +
                "            \"enum\": [\"string\", \"long\"]" +
                "          }" +
                "        }" +
                "        " +
                "      ]" +
                "    }" +
                "  }" +
                "}"
)
public class TypeCastingUnit {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Attribute")
    @JsonPropertyDescription("Attribute for type casting")
    @AutofillAttributeName
    public String attribute;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Cast type")
    @JsonPropertyDescription("Result type after type casting")
    public AttributeType resultType;

    //TODO: override equals to pass equality check for typecasting operator during cache status update
}
