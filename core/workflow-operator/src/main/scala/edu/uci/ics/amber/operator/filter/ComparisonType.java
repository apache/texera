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

package edu.uci.ics.amber.operator.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ComparisonType {
    EQUAL_TO("="),

    GREATER_THAN(">"),

    GREATER_THAN_OR_EQUAL_TO(">="),

    LESS_THAN("<"),

    LESS_THAN_OR_EQUAL_TO("<="),

    NOT_EQUAL_TO("!="),

    IS_NULL("is null"),

    IS_NOT_NULL("is not null");

    private final String name;

    private ComparisonType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    // Handle custom deserialization for enum
    @JsonCreator
    public static ComparisonType fromString(String value) {
        for (ComparisonType type : ComparisonType.values()) {
            if (type.name.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown comparison type: " + value);
    }
}