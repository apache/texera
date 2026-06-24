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

package org.apache.texera.amber.operator.source.sql.asterixdb

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AsterixDBSourceOpDescSpec extends AnyFlatSpec with Matchers {

  "AsterixDBSourceOpDesc.operatorInfo" should
    "advertise the AsterixDB source in the Database Connector group with no input and one output" in {
    val info = (new AsterixDBSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "AsterixDB Source"
    info.operatorDescription shouldBe "Read data from an AsterixDB instance"
    info.operatorGroupName shouldBe OperatorGroupConstants.DATABASE_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "AsterixDBSourceOpDesc" should "default its geo/regex/filter and connection fields" in {
    val d = new AsterixDBSourceOpDesc
    d.geoSearch shouldBe Some(false)
    d.geoSearchByColumns shouldBe empty
    d.geoSearchBoundingBox shouldBe empty
    d.regexSearch shouldBe Some(false)
    d.regexSearchByColumn shouldBe None
    d.regex shouldBe None
    d.filterCondition shouldBe Some(false)
    d.filterPredicates shouldBe empty
    d.host shouldBe null
    d.interval shouldBe 0L
  }

  "AsterixDBSourceOpDesc.sourceSchema" should "be null before a connection is configured" in {
    (new AsterixDBSourceOpDesc).sourceSchema() shouldBe null
  }

  "AsterixDBSourceOpDesc.getPhysicalOp" should
    "wire the AsterixDB exec as a source op with no input port and one output port" in {
    val d = new AsterixDBSourceOpDesc
    val physical = d.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.sql.asterixdb.AsterixDBSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "AsterixDBSourceOpDesc" should
    "round-trip its config fields and omit the ignored credentials" in {
    val d = new AsterixDBSourceOpDesc
    d.host = "localhost"
    d.database = "db"
    d.table = "t"
    d.username = "secret-user"
    d.password = "secret-pass"
    d.regex = Some("a.*")
    d.geoSearchByColumns = List("lonlat")
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"AsterixDBSource\"")
    // username/password are dropped via @JsonIgnoreProperties on this subclass.
    json should not include "secret-user"
    json should not include "secret-pass"
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[AsterixDBSourceOpDesc]
    val r = restored.asInstanceOf[AsterixDBSourceOpDesc]
    r.host shouldBe "localhost"
    r.database shouldBe "db"
    r.table shouldBe "t"
    r.regex shouldBe Some("a.*")
    r.geoSearchByColumns shouldBe List("lonlat")
    r.username shouldBe null
    r.password shouldBe null
  }
}
