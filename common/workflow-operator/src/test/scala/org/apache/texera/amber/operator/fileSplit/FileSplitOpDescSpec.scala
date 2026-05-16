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

package org.apache.texera.amber.operator.fileSplit

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec

class FileSplitOpDescSpec extends AnyFlatSpec {

  "FileSplitOpDesc" should "propagate the input schema to every output port" in {
    val desc = new FileSplitOpDesc()
    val inputSchema = Schema(
      List(
        new Attribute("source_file", AttributeType.STRING),
        new Attribute("value", AttributeType.INTEGER)
      )
    )

    val outputSchemas = desc.getExternalOutputSchemas(Map(PortIdentity() -> inputSchema))

    assert(outputSchemas.keySet == Set(PortIdentity(), PortIdentity(1)))
    assert(outputSchemas.values.forall(_ == inputSchema))
  }

  it should "reject inputs without a file identity column" in {
    val desc = new FileSplitOpDesc()
    val inputSchema = Schema(List(new Attribute("value", AttributeType.INTEGER)))

    val err = intercept[IllegalArgumentException] {
      desc.getExternalOutputSchemas(Map(PortIdentity() -> inputSchema))
    }
    assert(err.getMessage.contains("source_file"))
    assert(err.getMessage.contains("filename"))
  }
}
