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

package edu.uci.ics.texera.service.type.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.uci.ics.texera.service.type.DatasetFileNode;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;

import java.io.IOException;

// this class is used to serialize the FileNode as JSON. So that FileNodes can be inspected by the frontend through JSON.
public class DatasetFileNodeSerializer extends StdSerializer<DatasetFileNode> {

    public DatasetFileNodeSerializer() {
        this(null);
    }

    public DatasetFileNodeSerializer(Class<DatasetFileNode> t) {
        super(t);
    }

    @Override
    public void serialize(DatasetFileNode value, JsonGenerator gen, SerializerProvider provider) throws IOException {
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
            List<DatasetFileNode> children = value.getChildren();
            for (DatasetFileNode child : JavaConverters.seqAsJavaList(children)) {
                serialize(child, gen, provider); // Recursively serialize children
            }
            gen.writeEndArray();
        }
        gen.writeEndObject();
    }
}
