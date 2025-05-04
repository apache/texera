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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public enum FileDecodingMethod {
    UTF_8("UTF_8", StandardCharsets.UTF_8),
    UTF_16("UTF_16", StandardCharsets.UTF_16),
    ASCII("US_ASCII", StandardCharsets.US_ASCII);

    private final String name;
    private final Charset charset;

    FileDecodingMethod(String name, Charset charset) {
        this.name = name;
        this.charset = charset;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public Charset getCharset() {
        return this.charset;
    }
}
