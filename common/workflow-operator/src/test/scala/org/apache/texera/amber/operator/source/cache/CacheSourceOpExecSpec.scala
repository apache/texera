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

package org.apache.texera.amber.operator.source.cache

import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

/**
  * Unit tests for the in-memory validation in [[CacheSourceOpExec]]'s constructor. The
  * result-storage read path requires a live Iceberg catalog and is covered by the amber
  * integration suite, not here.
  */
class CacheSourceOpExecSpec extends AnyFlatSpec with Matchers {

  private val globalPortId =
    GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opA"), "main"),
      PortIdentity(0),
      input = true
    )

  "CacheSourceOpExec" should "reject a storage URI whose resource type is not RESULT" in {
    val stateUri =
      VFSURIFactory.stateURI(
        VFSURIFactory.createPortBaseURI(WorkflowIdentity(7L), ExecutionIdentity(11L), globalPortId)
      )
    val ex = intercept[RuntimeException](new CacheSourceOpExec(stateUri))
    ex.getMessage shouldBe "The storage URI must point to a result storage"
  }

  it should "reject a non-vfs storage URI" in {
    intercept[IllegalArgumentException](
      new CacheSourceOpExec(new URI("http:///wid/1/eid/1/result"))
    )
  }
}
