package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HashJoinOpSpec extends AnyFlatSpec with BeforeAndAfter {
  val build: Int = 0
  val probe: Int = 1

  var buildOpExec: HashJoinBuildOpExec[String] = _
  var probeOpExec: HashJoinProbeOpExec[String] = _
  var opDesc: HashJoinOpDesc[String] = _

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

    buildOpExec = new HashJoinBuildOpExec[String]("build_1")
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTexeraTuple(Left(tuple("build", 1, Some(i))), build, null, null).isEmpty
      )
    })

    val buildOpOutputIterator =
      buildOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "build_1",
      "probe_1",
      JoinType.INNER,
      inputSchemas(0),
      inputSchemas(1),
      outputSchema
    )

    probeOpExec.open()
    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTexeraTuple(Left(buildOpOutputIterator.next()), build, null, null)
          .isEmpty
      )
    }
    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    buildOpExec.close()

    val outputTuples = (5 to 9)
      .map(i => probeOpExec.processTexeraTuple(Left(tuple("probe", 1, Some(i))), probe, null, null))
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList

    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 3)

    probeOpExec.close()

  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName" in {
    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    val inputSchemas = Array(schema("same", 1), schema("same", 2))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)

    buildOpExec = new HashJoinBuildOpExec[String]("same")
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTexeraTuple(Left(tuple("same", 1, Some(i))), build, null, null).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "same",
      "same",
      JoinType.INNER,
      inputSchemas(0),
      inputSchemas(1),
      outputSchema
    )

    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTexeraTuple(Left(buildOpOutputIterator.next()), build, null, null)
          .isEmpty
      )
    }
    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    buildOpExec.close()

    val outputTuples = (5 to 9)
      .map(i =>
        probeOpExec.processTexeraTuple(Left(tuple("same", n = 2, Some(i))), probe, null, null)
      )
      .foldLeft(Iterator[Tuple]())(_ ++ _)
      .toList

    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 3)

    probeOpExec.close()
  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName with Full Outer Join" in {
    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    val inputSchemas = Array(schema("same", 1), schema("same", 2))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)

    buildOpExec = new HashJoinBuildOpExec[String]("same")
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTexeraTuple(Left(tuple("same", 1, Some(i))), build, null, null).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "same",
      "same",
      JoinType.FULL_OUTER,
      inputSchemas(0),
      inputSchemas(1),
      outputSchema
    )

    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTexeraTuple(Left(buildOpOutputIterator.next()), build, null, null)
          .isEmpty
      )
    }
    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), build, null, null).isEmpty)

    buildOpExec.close()

    assert(
      (5 to 9)
        .map(_ => {
          probeOpExec.processTexeraTuple(Left(tuple("same", n = 2, None)), probe, null, null)
        })
        .foldLeft(Iterator[Tuple]())(_ ++ _)
        .size == 5
    )

    assert(probeOpExec.processTexeraTuple(Right(InputExhausted()), probe, null, null).size == 8)

    probeOpExec.close()
  }
}
