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

package edu.uci.ics.amber.operator.dictionary;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * MatchingType: the type of matching to perform. <br>
 * Currently we have 3 types of matching: <br>
 * <p>
 * SCANBASED: <br>
 * Performs simple exact matching of the query. Matching is
 * case insensitive. <br>
 * <p>
 * SUBSTRING: <br>
 * Performs simple substring matching of the query. Matching is
 * case insensitive. <br>
 * <p>
 * CONJUNCTION_INDEXBASED: <br>
 * Performs search of conjunction of query tokens. The query is tokenized
 * into keywords, with each token treated as a separate keyword. The order
 * of tokens doesn't matter in the source tuple. <br>
 * <p>
 * For example: <br>
 * query "book appointment with the doctor" <br>
 * matches: "book appointment" <br>
 * also matches: "an appointment for a book" <br>
 * <br>
 *
 * @author Zuozhi Wang
 */

public enum MatchingType {
    SCANBASED("Scan"),

    SUBSTRING("Substring"),

    CONJUNCTION_INDEXBASED("Conjunction");

    private final String name;

    private MatchingType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
