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

package org.apache.texera.amber.operator.audio;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

// The measurements the Audio Attribute Extractor can take; the names are what the
// generated Python and the output's series column use.
public enum AudioAttribute {
    WAVEFORM("waveform"),
    RMS("rms"),
    ZCR("zcr"),
    PITCH_HZ("pitch_hz"),
    ONSETS("onsets"),
    BEATS("beats");

    private final String name;

    AudioAttribute(String name) {
        this.name = name;
    }

    @JsonCreator
    public static AudioAttribute fromString(String value) {
        for (AudioAttribute a : AudioAttribute.values()) {
            if (a.name.equalsIgnoreCase(value)) {
                return a;
            }
        }
        throw new IllegalArgumentException("Unknown audio attribute: " + value);
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
