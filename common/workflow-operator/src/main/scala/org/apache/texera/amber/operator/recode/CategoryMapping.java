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

package org.apache.texera.amber.operator.recode;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;

import java.io.Serializable;
import java.util.Objects;

public class CategoryMapping implements Serializable {
    @JsonProperty(required = true)
    @JsonSchemaTitle("Source value")
    @JsonPropertyDescription("Exact value; strings preserve case and spaces. Numeric codes must fit the source type.")
    public String value;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Category")
    @JsonPropertyDescription("Text label for this value. Multiple values can share a category.")
    public String category;

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof CategoryMapping)) return false;
        CategoryMapping that = (CategoryMapping) other;
        return Objects.equals(value, that.value) && Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, category);
    }
}
