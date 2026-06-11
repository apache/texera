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

package org.apache.texera.amber.operator.source.dataset

import org.apache.texera.amber.core.tuple.AttributeType
import org.scalatest.flatspec.AnyFlatSpec

class FileListerSourceOpDescSpec extends AnyFlatSpec {

  "FileListerSourceOpDesc" should "expose a filename output column" in {
    val opDesc = new FileListerSourceOpDesc()

    val outputSchema = opDesc.getExternalOutputSchemas(Map.empty).values.head

    assert(outputSchema.getAttributes.length == 1)
    assert(outputSchema.getAttribute("filename").getType == AttributeType.STRING)
  }

  it should "use the expected operator metadata" in {
    val opDesc = new FileListerSourceOpDesc()

    assert(opDesc.operatorInfo.userFriendlyName == "File Lister")
    assert(opDesc.operatorInfo.inputPorts.isEmpty)
    assert(opDesc.operatorInfo.outputPorts.length == 1)
  }
}
