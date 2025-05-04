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

package edu.uci.ics.amber.core.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An attribute describes the name and the type of a column.
 */
public class Attribute implements Serializable {

    private final String attributeName;
    private final AttributeType attributeType;

    @JsonCreator
    public Attribute(
            @JsonProperty(value = "attributeName", required = true) String attributeName,
            @JsonProperty(value = "attributeType", required = true) AttributeType attributeType
    ) {
        checkNotNull(attributeName);
        checkNotNull(attributeType);
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }

    @JsonProperty(value = "attributeName")
    public String getName() {
        return attributeName;
    }

    @JsonProperty(value = "attributeType")
    public AttributeType getType() {
        return attributeType;
    }

    @Override
    public String toString() {
        return "Attribute[name=" + attributeName + ", type=" + attributeType + "]";
    }

    @Override
    public boolean equals(Object toCompare) {
        if (this == toCompare) {
            return true;
        }
        if (toCompare == null) {
            return false;
        }
        if (this.getClass() != toCompare.getClass()) {
            return false;
        }

        Attribute that = (Attribute) toCompare;

        if (this.attributeName == null) {
            return that.attributeName == null;
        }
        if (this.attributeType == null) {
            return that.attributeType == null;
        }

        return this.attributeName.equalsIgnoreCase(that.attributeName) && this.attributeType.equals(that.attributeType);
    }

    @Override
    public int hashCode() {
        return this.attributeName.hashCode() + this.attributeType.toString().hashCode();
    }
}
