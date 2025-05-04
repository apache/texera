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

package edu.uci.ics.amber.operator.source.scan;

import com.fasterxml.jackson.annotation.JsonValue;
import edu.uci.ics.amber.core.tuple.AttributeType;

public enum FileAttributeType {
    STRING("string", AttributeType.STRING),
    SINGLE_STRING("single string", AttributeType.STRING),
    INTEGER("integer", AttributeType.INTEGER),
    LONG("long", AttributeType.LONG),
    DOUBLE("double", AttributeType.DOUBLE),
    BOOLEAN("boolean", AttributeType.BOOLEAN),
    TIMESTAMP("timestamp", AttributeType.TIMESTAMP),
    BINARY("binary", AttributeType.BINARY);


    private final String name;
    private final AttributeType type;

    FileAttributeType(String name, AttributeType type) {
        this.name = name;
        this.type = type;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    public AttributeType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public boolean isSingle() {
        return this == SINGLE_STRING || this == BINARY;
    }
}
