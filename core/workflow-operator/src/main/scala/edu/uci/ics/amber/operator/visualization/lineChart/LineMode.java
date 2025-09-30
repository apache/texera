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

package edu.uci.ics.amber.operator.visualization.lineChart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum LineMode {
    LINE("line"),
    DOTS("dots"),
    LINE_WITH_DOTS("line with dots");
    private final String mode;

    LineMode(String mode) {
        this.mode = mode;
    }

    // Handle custom deserialization for enum
    @JsonCreator
    public static LineMode fromString(String value) {
        for (LineMode mode : LineMode.values()) {
            if (mode.mode.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown line mode: " + value);
    }

    @JsonValue
    public String getMode() {
        return mode;
    }

    public String getModeInPlotly() {
        // make the mode string compatible with plotly API.
        switch (this) {
            case DOTS:
                return "markers";
            case LINE:
                return "lines";
            case LINE_WITH_DOTS:
                return "lines+markers";
            default:
                throw new UnsupportedOperationException("line mode is not supported");
        }
    }
}