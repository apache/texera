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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum UnmatchedCategoryPolicy {
    SET_NULL("Set null"),
    KEEP_ORIGINAL("Keep original"),
    USE_DEFAULT("Use default"),
    ERROR("Error");

    private final String label;

    UnmatchedCategoryPolicy(String label) {
        this.label = label;
    }

    @JsonValue
    public String getLabel() {
        return label;
    }

    @JsonCreator
    public static UnmatchedCategoryPolicy fromString(String label) {
        for (UnmatchedCategoryPolicy policy : values()) {
            if (policy.label.equals(label)) return policy;
        }
        throw new IllegalArgumentException("Unknown unmatched values policy.");
    }
}
