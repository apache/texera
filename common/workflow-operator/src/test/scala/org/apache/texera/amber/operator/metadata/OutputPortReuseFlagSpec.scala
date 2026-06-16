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

package org.apache.texera.amber.operator.metadata

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Guard for the `OutputPort.reuseStorage` flag.
  *
  * The flag tells the region scheduler to reuse (append to) a port's storage
  * across region re-executions instead of recreating it. Only an operator whose
  * output accumulates across re-executions should set it -- today that is no
  * operator on `main` (the only one that will, Loop End, is not yet merged).
  *
  * This pins the flag off for every registered operator so it can't be turned
  * on unexpectedly. When the loop operators land, update this to allow Loop
  * End's output port (and only it).
  */
class OutputPortReuseFlagSpec extends AnyFlatSpec with Matchers {

  "No registered operator" should "enable OutputPort.reuseStorage on any of its output ports" in {
    OperatorMetadataGenerator.operatorTypeMap.keys.foreach { opClass =>
      opClass.getConstructor().newInstance().operatorInfo.outputPorts.foreach { port =>
        withClue(s"${opClass.getSimpleName} / output port ${port.id}: ") {
          port.reuseStorage shouldBe false
        }
      }
    }
  }
}
