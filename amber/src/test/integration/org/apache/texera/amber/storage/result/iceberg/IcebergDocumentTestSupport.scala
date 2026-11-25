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

package org.apache.texera.amber.storage.result.iceberg

import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}

import java.net.URI
import java.sql.Timestamp
import java.util.UUID

/**
  * The all-types schema, tuple builder, port URIs and document opener that
  * `IcebergDocumentSpec` and `IcebergTableStatsSpec` share.
  */
object IcebergDocumentTestSupport {

  /** String, integer, boolean, long, double, timestamp and binary columns, in that order. */
  val allTypesSchema: Schema = Schema(
    List(
      new Attribute("col-string", AttributeType.STRING),
      new Attribute("col-int", AttributeType.INTEGER),
      new Attribute("col-bool", AttributeType.BOOLEAN),
      new Attribute("col-long", AttributeType.LONG),
      new Attribute("col-double", AttributeType.DOUBLE),
      new Attribute("col-timestamp", AttributeType.TIMESTAMP),
      new Attribute("col-binary", AttributeType.BINARY)
    )
  )

  /** A port base URI for an operator named `prefix` plus a random suffix, unique per call. */
  def uniquePortBaseURI(prefix: String = "test-table"): URI =
    VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(s"$prefix-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      )
    )

  /** Opens the document at `uri` as a document of tuples. */
  def openTupleDocument(uri: URI): VirtualDocument[Tuple] =
    DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]

  /** A tuple of `allTypesSchema` with the given values, in column order. */
  def buildAllTypesTuple(
      strVal: String,
      intVal: Any,
      boolVal: Any,
      longVal: Any,
      doubleVal: Any,
      timestampVal: Timestamp,
      binaryVal: Array[Byte]
  ): Tuple =
    Tuple
      .builder(allTypesSchema)
      .add("col-string", AttributeType.STRING, strVal)
      .add("col-int", AttributeType.INTEGER, intVal)
      .add("col-bool", AttributeType.BOOLEAN, boolVal)
      .add("col-long", AttributeType.LONG, longVal)
      .add("col-double", AttributeType.DOUBLE, doubleVal)
      .add("col-timestamp", AttributeType.TIMESTAMP, timestampVal)
      .add("col-binary", AttributeType.BINARY, binaryVal)
      .build()
}
