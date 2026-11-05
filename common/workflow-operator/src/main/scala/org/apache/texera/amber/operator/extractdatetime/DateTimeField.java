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

package org.apache.texera.amber.operator.extractdatetime;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A field of a timestamp that can be read out of it as a whole number.
 *
 * <p>Read as ISO-8601 states them, which is what lets the engine and the exported
 * Python agree: a Monday is 1, and pandas counts weekdays from 0.
 */
public enum DateTimeField {

    YEAR("year"),

    QUARTER("quarter"),

    MONTH("month"),

    DAY("day"),

    DAY_OF_WEEK("day of week"),

    DAY_OF_YEAR("day of year"),

    WEEK_OF_YEAR("week of year"),

    HOUR("hour"),

    MINUTE("minute"),

    SECOND("second");

    private final String name;

    DateTimeField(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
