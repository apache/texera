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

import org.apache.texera.amber.operator.loop.LoopEndOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Guard for the `OutputPort.reuseStorage` flag.
  *
  * The flag tells the region scheduler to reuse (append to) a port's storage
  * across region re-executions instead of recreating it. The only operator that
  * needs it is Loop End, whose output accumulates across the iterations of its
  * own loop. This pins that nothing else turns the flag on -- if a new operator
  * (or a change to an existing one) enables it, this fails.
  */
class OutputPortReuseFlagSpec extends AnyFlatSpec with Matchers {

  "Only Loop End" should "enable OutputPort.reuseStorage on its output ports" in {
    OperatorMetadataGenerator.operatorTypeMap.keys.foreach { opClass =>
      val mayReuse = opClass == classOf[LoopEndOpDesc]
      opClass.getConstructor().newInstance().operatorInfo.outputPorts.foreach { port =>
        withClue(s"${opClass.getSimpleName} / output port ${port.id}: ") {
          port.reuseStorage shouldBe mayReuse
        }
      }
    }
  }
}
