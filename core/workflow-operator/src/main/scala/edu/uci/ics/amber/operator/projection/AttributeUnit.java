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

package edu.uci.ics.amber.operator.projection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName;
import org.jooq.tools.StringUtils;
import java.util.Objects;

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

    // TODO: explore the reason why this JsonCreator annotation is required
    @JsonCreator
    public AttributeUnit(
        @JsonProperty("originalAttribute") String attributeName,
        @JsonProperty("alias") String alias) {
        this.originalAttribute = attributeName;
        this.alias = alias;
    }


    String getOriginalAttribute(){
        return originalAttribute;
    }


    String getAlias(){
        if(StringUtils.isBlank(alias)){
            return originalAttribute;
        }
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeUnit that = (AttributeUnit) o;
        return Objects.equals(originalAttribute, that.originalAttribute) && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalAttribute, alias);
    }
}