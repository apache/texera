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

package org.apache.texera.service.type.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.texera.service.type.LakeFSFileNode;
import scala.collection.immutable.List;
import scala.jdk.javaapi.CollectionConverters;

import java.io.IOException;

// this class is used to serialize the FileNode as JSON. So that FileNodes can be inspected by the frontend through JSON.
public class LakeFSFileNodeSerializer extends StdSerializer<LakeFSFileNode> {

    public LakeFSFileNodeSerializer() {
        this(null);
    }

    public LakeFSFileNodeSerializer(Class<LakeFSFileNode> t) {
        super(t);
    }

    @Override
    public void serialize(LakeFSFileNode value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("name", value.getName());
        gen.writeStringField("type", value.getNodeType());
        gen.writeStringField("parentDir", value.getParent().getFilePath());
        gen.writeStringField("ownerEmail", value.getOwnerEmail());
        if (value.getNodeType().equals("file")) {
            gen.writeObjectField("size", value.getSize());
        }
        if (value.getNodeType().equals("directory")) {
            gen.writeFieldName("children");
            gen.writeStartArray();
            List<LakeFSFileNode> children = value.getChildren();
            for (LakeFSFileNode child : CollectionConverters.asJava(children)) {
                serialize(child, gen, provider); // Recursively serialize children
            }
            gen.writeEndArray();
        }
        gen.writeEndObject();
    }
}
