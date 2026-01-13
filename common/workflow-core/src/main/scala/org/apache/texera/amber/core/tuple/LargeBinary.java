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

package org.apache.texera.amber.core.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.texera.amber.core.executor.OperatorExecutor;
import org.apache.texera.service.util.LargeBinaryManager;

import java.net.URI;
import java.util.Objects;

/**
 * LargeBinary represents a reference to a large object stored in S3.
 * 
 * Each LargeBinary is identified by an S3 URI (s3://bucket/path/to/object).
 * LargeBinaries are automatically tracked and cleaned up when the workflow execution completes.
 */
public class LargeBinary {
    
    private final String uri;
    
    /**
     * Creates a LargeBinary from an existing S3 URI.
     * Used primarily for deserialization from JSON.
     * 
     * @param uri S3 URI in the format s3://bucket/path/to/object
     * @throws IllegalArgumentException if URI is null or doesn't start with "s3://"
     */
    @JsonCreator
    public LargeBinary(@JsonProperty("uri") String uri) {
        if (uri == null) {
            throw new IllegalArgumentException("LargeBinary URI cannot be null");
        }
        if (!uri.startsWith("s3://")) {
            throw new IllegalArgumentException(
                "LargeBinary URI must start with 's3://', got: " + uri
            );
        }
        this.uri = uri;
    }
    
    /**
     * Creates a new LargeBinary for writing data.
     * Generates a unique S3 URI.
     * 
     * Usage example:
     * 
     *   LargeBinary largeBinary = new LargeBinary();
     *   try (LargeBinaryOutputStream out = new LargeBinaryOutputStream(largeBinary)) {
     *     out.write(data);
     *   }
     *   // largeBinary is now ready to be added to tuples
     * 
     */
    public LargeBinary() {
        this(LargeBinaryManager.create());
    }
    
    @JsonValue
    public String getUri() {
        return uri;
    }
    
    public String getBucketName() {
        return URI.create(uri).getHost();
    }
    
    public String getObjectKey() {
        String path = URI.create(uri).getPath();
        return path.startsWith("/") ? path.substring(1) : path;
    }
    
    @Override
    public String toString() {
        return uri;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LargeBinary)) return false;
        LargeBinary that = (LargeBinary) obj;
        return Objects.equals(uri, that.uri);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }
}
