package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HashJoinOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val build: Int = 0
  val probe: Int = 1

  var opExec: HashJoinOpExec[String] = _
  var opDesc: HashJoinOpDesc[String] = _
  var counter: Int = 0

  def linkID(): LinkIdentity = LinkIdentity(layerID(), fromPort = 0, layerID(), toPort = 0)

  def layerID(): LayerIdentity = {
    counter += 1
    LayerIdentity("" + counter, "" + counter, "" + counter)
  }

  def tuple(name: String, n: Int = 1, i: Option[Int]): Tuple = {

    Tuple
      .newBuilder(schema(name, n))
      .addSequentially(Array[Object](i.map(_.toString).orNull, i.map(_.toString).orNull))
      .build()
  }

  def schema(name: String, n: Int = 1): Schema = {
    Schema
      .newBuilder()
      .add(
        new Attribute(name, AttributeType.STRING),
        new Attribute(name + "_" + n, AttributeType.STRING)
      )
      .build()
  }

  it should "work with basic two input streams with different buildAttributeName and probeAttributeName" in {

    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "build_1"
    opDesc.probeAttributeName = "probe_1"
    val inputSchemas = Array(schema("build"), schema("probe"))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)

    opExec = new HashJoinOpExec[String](
      "build_1",
      "probe_1",
      JoinType.INNER,
      OperatorSchemaInfo(inputSchemas, Array(outputSchema))
    )
    opExec.open()
    counter = 0
    (0 to 7).map(i => {
      assert(opExec.processTexeraTuple(Left(tuple("build", 1, Some(i))), build, null, null).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    val outputTuples = (5 to 9)
      .map(i => opExec.processTexeraTuple(Left(tuple("probe", 1, Some(i))), probe, null, null))
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList

    assert(opExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 3)

    opExec.close()
  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName" in {
    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    val inputSchemas = Array(schema("same", 1), schema("same", 2))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    opExec = new HashJoinOpExec[String](
      "same",
      "same",
      JoinType.INNER,
      OperatorSchemaInfo(inputSchemas, Array(outputSchema))
    )
    opExec.open()
    counter = 0
    (0 to 7).map(i => {
      assert(
        opExec.processTexeraTuple(Left(tuple("same", n = 1, Some(i))), build, null, null).isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    val outputTuples = (5 to 9)
      .map(i => opExec.processTexeraTuple(Left(tuple("same", n = 2, Some(i))), probe, null, null))
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList

    assert(opExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 3)

    opExec.close()
  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName with Full Outer Join" in {
    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    val inputSchemas = Array(schema("same", 1), schema("same", 2))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    opExec = new HashJoinOpExec[String](
      "same",
      "same",
      JoinType.FULL_OUTER,
      OperatorSchemaInfo(inputSchemas, Array(outputSchema))
    )
    opExec.open()
    counter = 0
    (0 to 7).map(i => {
      assert(
        opExec.processTexeraTuple(Left(tuple("same", n = 1, Some(i))), build, null, null).isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    assert(
      (5 to 9)
        .map(_ => {
          opExec.processTexeraTuple(Left(tuple("same", n = 2, None)), probe, null, null)
        })
        .foldLeft(Iterator[Tuple]())(_ ++ _)
        .size == 5
    )

    assert(opExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).size == 8)

    opExec.close()
  }
}
