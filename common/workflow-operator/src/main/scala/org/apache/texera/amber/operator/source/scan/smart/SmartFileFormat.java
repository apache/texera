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

package org.apache.texera.amber.operator.source.scan.smart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SmartFileFormat {
    AUTO("Auto-detect"),
    CSV("CSV"),
    TSV("TSV"),
    JSON("JSON"),
    JSONL("JSONL"),
    ARROW("Arrow"),
    PARQUET("Parquet"),
    EXCEL("Excel"),
    IMAGE("Image"),
    TEXT("Plain text");

    private final String label;

    SmartFileFormat(String label) {
        this.label = label;
    }

    @JsonValue
    public String getLabel() {
        return label;
    }

    /** Accept either the enum name (e.g. "CSV") or the label (e.g. "Plain text"). */
    @JsonCreator
    public static SmartFileFormat fromString(String value) {
        if (value == null) {
            return null;
        }
        for (SmartFileFormat format : values()) {
            if (format.name().equalsIgnoreCase(value) || format.label.equalsIgnoreCase(value)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown SmartFileFormat: " + value);
    }

    @Override
    public String toString() {
        return label;
    }
}
