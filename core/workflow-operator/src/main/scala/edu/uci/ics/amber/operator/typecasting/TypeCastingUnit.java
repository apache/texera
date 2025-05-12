/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
