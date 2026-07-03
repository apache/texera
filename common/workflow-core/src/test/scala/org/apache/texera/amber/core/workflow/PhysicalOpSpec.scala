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

package org.apache.texera.amber.core.workflow

import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.scalatest.flatspec.AnyFlatSpec

class PhysicalOpSpec extends AnyFlatSpec {

  private val workflowId = WorkflowIdentity(0L)
  private val executionId = ExecutionIdentity(0L)
  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")
  private val intSchema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
  private def newOp(name: String): PhysicalOp =
    PhysicalOp.oneToOnePhysicalOp(opId(name), workflowId, executionId, OpExecInitInfo.Empty)

  // ----- SchemaPropagationFunc -----

  "SchemaPropagationFunc" should "wrap a serializable Java function" in {
    val javaFunc =
      new java.util.function.Function[Map[PortIdentity, Schema], Map[PortIdentity, Schema]]
        with java.io.Serializable {
        override def apply(m: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] = m
      }
    val wrapped = SchemaPropagationFunc(javaFunc)
    val schemas = Map(PortIdentity(0) -> intSchema)
    assert(wrapped.func(schemas) == schemas)
    val scalaFunc = SchemaPropagationFunc(identity[Map[PortIdentity, Schema]] _)
    assert(scalaFunc == scalaFunc.copy())
  }

  // ----- factory methods -----

  "PhysicalOp factories" should "build source ops preferring the controller" in {
    val op = PhysicalOp.sourcePhysicalOp(
      workflowId,
      executionId,
      OperatorIdentity("src"),
      OpExecInitInfo.Empty
    )
    assert(op.id == opId("src"))
    assert(!op.parallelizable)
    assert(op.locationPreference.contains(PreferController))
    assert(op.isSourceOperator)
    val op2 = PhysicalOp.sourcePhysicalOp(opId("s2"), workflowId, executionId, OpExecInitInfo.Empty)
    assert(op2.id == opId("s2"))
    assert(op2.locationPreference.contains(PreferController))
  }

  it should "build one-to-one ops as parallelizable with no location preference" in {
    val op = PhysicalOp.oneToOnePhysicalOp(
      workflowId,
      executionId,
      OperatorIdentity("o"),
      OpExecInitInfo.Empty
    )
    assert(op.id == opId("o"))
    assert(op.parallelizable)
    assert(op.locationPreference.isEmpty)
  }

  it should "build many-to-one ops requiring a single partition" in {
    val op = PhysicalOp.manyToOnePhysicalOp(
      workflowId,
      executionId,
      OperatorIdentity("m"),
      OpExecInitInfo.Empty
    )
    assert(!op.parallelizable)
    assert(op.partitionRequirement == List(Some(SinglePartition())))
    assert(op.derivePartition(List(HashPartition(List("k")))) == SinglePartition())
    val op2 =
      PhysicalOp.manyToOnePhysicalOp(opId("m2"), workflowId, executionId, OpExecInitInfo.Empty)
    assert(op2.partitionRequirement == List(Some(SinglePartition())))
  }

  it should "build local ops on the controller requiring a single partition" in {
    val op = PhysicalOp.localPhysicalOp(
      workflowId,
      executionId,
      OperatorIdentity("l"),
      OpExecInitInfo.Empty
    )
    assert(!op.parallelizable)
    assert(op.partitionRequirement == List(Some(SinglePartition())))
    assert(op.locationPreference.contains(PreferController))
    val op2 = PhysicalOp.localPhysicalOp(opId("l2"), workflowId, executionId, OpExecInitInfo.Empty)
    assert(op2.locationPreference.contains(PreferController))
  }

  // ----- dependee inputs -----

  "PhysicalOp.dependeeInputs" should "list distinct dependee ports" in {
    val op = newOp("j").withInputPorts(
      List(
        InputPort(PortIdentity(0)),
        InputPort(PortIdentity(1), dependencies = Seq(PortIdentity(0))),
        InputPort(PortIdentity(2), dependencies = Seq(PortIdentity(0)))
      )
    )
    assert(op.dependeeInputs == List(PortIdentity(0)))
    val dependeeLink = PhysicalLink(opId("u"), PortIdentity(0), opId("j"), PortIdentity(0))
    val dependerLink = PhysicalLink(opId("u"), PortIdentity(0), opId("j"), PortIdentity(1))
    assert(op.isInputLinkDependee(dependeeLink))
    assert(!op.isInputLinkDependee(dependerLink))
  }

  // ----- exec-code accessors -----

  "PhysicalOp.isPythonBased" should "hold for python and R code executors only" in {
    def withLanguage(language: String): PhysicalOp =
      newOp("p").copy(opExecInitInfo = OpExecWithCode("code", language))
    assert(withLanguage("python").isPythonBased)
    assert(withLanguage("r-tuple").isPythonBased)
    assert(withLanguage("r-table").isPythonBased)
    assert(!withLanguage("java").isPythonBased)
    assert(!newOp("p").isPythonBased)
  }

  "PhysicalOp.getCode" should "return the code or reject executors without code" in {
    val op = newOp("c").copy(opExecInitInfo = OpExecWithCode("print(1)", "python"))
    assert(op.getCode == "print(1)")
    val ex = intercept[IllegalAccessError](newOp("c").getCode)
    assert(ex.getMessage == "No code information in this physical operator")
  }

  // ----- with-builders -----

  "PhysicalOp with-builders" should
    "copy the partition requirement, derive function, one-to-many flag, and pve name" in {
    val required = List(Some(HashPartition(List("k"))), None)
    assert(newOp("a").withPartitionRequirement(required).partitionRequirement == required)
    val op = newOp("a").withDerivePartition(_ => HashPartition(List("x")))
    assert(op.derivePartition(List(SinglePartition())) == HashPartition(List("x")))
    assert(newOp("a").withIsOneToManyOp(true).isOneToManyOp)
    assert(!newOp("a").isOneToManyOp)
    assert(newOp("a").pveName == "")
    assert(newOp("a").withPveName("my-pve").pveName == "my-pve")
  }

  // ----- propagateSchema -----

  "PhysicalOp.propagateSchema" should "wait for all input schemas before running propagation" in {
    val op = newOp("a")
      .withInputPorts(List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
    val updated = op.propagateSchema(Some((PortIdentity(0), intSchema)))
    assert(updated.inputPorts(PortIdentity(0))._3 == Right(intSchema))
    assert(updated.inputPorts(PortIdentity(1))._3.isLeft)
    assert(updated.outputPorts(PortIdentity(0))._3.isLeft)
  }

  it should "run propagation with no new schema when all inputs are already known" in {
    val src = newOp("s")
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> intSchema)))
    val out = src.propagateSchema()
    assert(out.outputPorts(PortIdentity(0))._3 == Right(intSchema))
  }

  // ----- input-port dependency ordering -----

  "PhysicalOp.getInputPortDependencyPairs" should "order ports along the dependency chain" in {
    val op = newOp("j").withInputPorts(
      List(
        InputPort(PortIdentity(0)),
        InputPort(PortIdentity(1), dependencies = Seq(PortIdentity(0))),
        InputPort(PortIdentity(2), dependencies = Seq(PortIdentity(1)))
      )
    )
    assert(
      op.getInputPortDependencyPairs == List(PortIdentity(0), PortIdentity(1), PortIdentity(2))
    )
  }

  it should "be empty when no port declares dependencies" in {
    val op = newOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    assert(op.getInputPortDependencyPairs == Nil)
  }

  // ----- addOutputLink guards -----

  "PhysicalOp.addOutputLink" should "reject links from other operators or undeclared ports" in {
    val op = newOp("a").withOutputPorts(List(OutputPort(PortIdentity(0))))
    assertThrows[AssertionError] {
      op.addOutputLink(PhysicalLink(opId("other"), PortIdentity(0), opId("dn"), PortIdentity(0)))
    }
    assertThrows[AssertionError] {
      op.addOutputLink(PhysicalLink(opId("a"), PortIdentity(99), opId("dn"), PortIdentity(0)))
    }
  }

  // ----- primary-constructor defaults -----

  "PhysicalOp" should "provide sensible defaults from the primary constructor" in {
    val op = PhysicalOp(opId("raw"), workflowId, executionId, OpExecInitInfo.Empty)
    assert(op == op.copy())
    assert(
      op.derivePartition(List(HashPartition(List("x")), SinglePartition())) == HashPartition(
        List("x")
      )
    )
    val schemas = Map(PortIdentity(0) -> intSchema)
    assert(op.propagateSchema.func(schemas) == schemas)
  }
}
