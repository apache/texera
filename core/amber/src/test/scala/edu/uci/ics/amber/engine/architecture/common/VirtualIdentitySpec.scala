package edu.uci.ics.amber.engine.architecture.common

import com.google.protobuf.any.Any
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy2.{
  Add,
  Literal,
  ProtoExprWrapper
}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.worker.WorkerState
import edu.uci.ics.amber.engine.common.worker.WorkerState.Ready
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import scalapb.json4s.{Parser, Printer, TypeRegistry}

class VirtualIdentitySpec extends AnyFlatSpec {
  it should "convert virtualidentities to json with jackson" in {
    val workerID = ActorVirtualIdentity("worker")
    val workerID2 = ActorVirtualIdentity("worker2")

    val operatorID = OperatorIdentity("workflow", "op1")
    val from = LayerIdentity("workflow", "op1", "layer1")
    val to = LayerIdentity("workflow", "op2", "layer1")
    val linkIdentity = LinkIdentity(Option(from), Option(to))

    val jsonString = objectMapper.writeValueAsString(linkIdentity)
    println(jsonString)

    assert(linkIdentity == objectMapper.readValue(jsonString, classOf[LinkIdentity]))
  }

  it should "convert Enum" in {

    val jsonString = objectMapper.writeValueAsString(Ready)
    println(jsonString)
    assert(Ready == objectMapper.readValue(jsonString, classOf[WorkerState]))
  }

//  it should "convert oneof" in {
//    val workerID = ActorVirtualIdentity("worker")
//    val workerID2 = ActorVirtualIdentity("worker2")
//
//    val from = LayerIdentity("workflow", "op1", "layer1")
//    val to = LayerIdentity("workflow", "op2", "layer1")
//    val linkIdentity = LinkIdentity(Option(from),Option(to))
//    val policy = OneToOnePolicy(Option(linkIdentity), 10, Seq(workerID, workerID2))
//    val json4sString = new scalapb.json4s.Printer(
//      includingDefaultValueFields = true,
//      preservingProtoFieldNames = true,
//      formattingLongAsNumber = true
//    ).print(policy)
//    println(json4sString)
//    println(new  Parser().fromJsonString[DataSendingPolicy](json4sString))
//    val jsonString = objectMapper.writeValueAsString(policy)
//
//    println(jsonString)
//    assert(policy == objectMapper.readValue(jsonString, classOf[OneToOnePolicy]))
//  }

  it should "convert oneof using explicit wrapper" in {
    val m = ProtoExprWrapper(Seq(Add(Literal(2), Literal(3))))

    val json4sString =
      new Printer().includingDefaultValueFields.preservingProtoFieldNames.formattingLongAsNumber
        .print(m)

    println(s"scalapb-json4s $json4sString")
    println(
      s"Parse back with json4s ${new Parser().fromJsonString[ProtoExprWrapper](json4sString)}"
    )
    println(s"raw jackson ${objectMapper.writeValueAsString(m)}")
    // println(s"Parse back with jackson ${objectMapper.readValue(json4sString, classOf[ProtoExprWrapper])}")
    // does not work

  }

  it should "convert oneof using any wrapper" in {
    val m = Any.pack(Add(Literal(2), Literal(3)))
    val t = TypeRegistry().addMessage(Add)
    val json4sString =
      new Printer()
        .withTypeRegistry(t)
        .includingDefaultValueFields
        .preservingProtoFieldNames
        .formattingLongAsNumber
        .print(m)

    println(s"scalapb-json4s $json4sString")

    println(
      s"Parse back with json4s ${new Parser().withTypeRegistry(t).fromJsonString[Any](json4sString)}"
    )
    println(s"raw jackson ${objectMapper.writeValueAsString(m)}")
//     println(s"Parse back with jackson ${objectMapper.readValue(json4sString, classOf[Any])}")
//     does not work

  }
}
