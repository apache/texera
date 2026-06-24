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

package org.apache.texera.amber.operator.source.sql.mysql

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

import scala.annotation.nowarn

// MySQLSourceOpDesc is @deprecated (no longer executable) but retained so legacy
// workflows still deserialize; the coverage below pins that backward-compatible contract.
@nowarn("cat=deprecation")
class MySQLSourceOpDescSpec extends AnyFlatSpec with Matchers {

  "MySQLSourceOpDesc.operatorInfo" should
    "advertise the MySQL source in the Database Connector group with no input and one output" in {
    val info = (new MySQLSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "MySQL Source"
    info.operatorDescription shouldBe "Read data from a MySQL instance"
    info.operatorGroupName shouldBe OperatorGroupConstants.DATABASE_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "MySQLSourceOpDesc" should "default its connection and query fields" in {
    val d = new MySQLSourceOpDesc
    d.host shouldBe null
    d.port shouldBe null
    d.database shouldBe null
    d.table shouldBe null
    d.limit shouldBe None
    d.offset shouldBe None
    d.keywordSearch shouldBe Some(false)
    d.progressive shouldBe Some(false)
    d.interval shouldBe 0L
  }

  "MySQLSourceOpDesc.sourceSchema" should "be null before a connection is configured" in {
    (new MySQLSourceOpDesc).sourceSchema() shouldBe null
  }

  "MySQLSourceOpDesc.getPhysicalOp" should
    "wire the MySQL exec as a source op with no input port and one output port" in {
    val d = new MySQLSourceOpDesc
    val physical = d.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.sql.mysql.MySQLSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "MySQLSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new MySQLSourceOpDesc
    d.host = "localhost"
    d.database = "db"
    d.table = "t"
    d.username = "secret-user"
    d.password = "secret-pass"
    d.limit = Some(5L)
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"MySQLSource\"")
    // Unlike AsterixDBSourceOpDesc (which drops credentials via @JsonIgnoreProperties), the SQL
    // base persists username/password in plaintext; pin that behavior so any future change is visible.
    json should include("secret-user")
    json should include("secret-pass")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[MySQLSourceOpDesc]
    val r = restored.asInstanceOf[MySQLSourceOpDesc]
    r.host shouldBe "localhost"
    r.database shouldBe "db"
    r.table shouldBe "t"
    r.username shouldBe "secret-user"
    r.password shouldBe "secret-pass"
    r.limit shouldBe Some(5L)
  }
}
