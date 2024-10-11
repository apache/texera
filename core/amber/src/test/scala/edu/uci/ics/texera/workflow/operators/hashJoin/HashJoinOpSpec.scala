package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.model.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple,
  TupleLike
}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HashJoinOpSpec extends AnyFlatSpec with BeforeAndAfter {
  val build: Int = 0
  val probe: Int = 1

  var buildOpExec: HashJoinBuildOpExec[String] = _
  var probeOpExec: HashJoinProbeOpExec[String] = _
  var opDesc: HashJoinOpDesc[String] = _

  def getInternalHashTableSchema(buildInputSchema: Schema): Schema = {
    Schema
      .builder()
      .add(HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.ANY)
      .add(buildInputSchema)
      .build()
  }
  def tuple(name: String, n: Int = 1, i: Option[Int]): Tuple = {

    Tuple
      .builder(schema(name, n))
      .addSequentially(Array[Any](i.map(_.toString).orNull, i.map(_.toString).orNull))
      .build()
  }

  def schema(name: String, n: Int = 1): Schema = {
    Schema
      .builder()
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
        buildOpExec.processTuple(tuple("build", 1, Some(i)), build).isEmpty
      )
    })

    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "probe_1",
      JoinType.INNER
    )

    probeOpExec.open()
    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head)),
            build
          )
          .isEmpty
      )
    }
    assert(probeOpExec.onFinish(build).isEmpty)

    buildOpExec.close()

    val outputTuples = (5 to 9)
      .map(i => probeOpExec.processTuple(tuple("probe", 1, Some(i)), probe))
      .foldLeft(Iterator[TupleLike]())(_ ++ _)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
      .toList

    assert(probeOpExec.onFinish(probe).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getFields.length == 3)

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
        buildOpExec.processTuple(tuple("same", 1, Some(i)), build).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "same",
      JoinType.INNER
    )

    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head)),
            build
          )
          .isEmpty
      )
    }
    assert(probeOpExec.onFinish(build).isEmpty)

    buildOpExec.close()

    val outputTuples = (5 to 9)
      .map(i => probeOpExec.processTuple(tuple("same", n = 2, Some(i)), probe))
      .foldLeft(Iterator[TupleLike]())(_ ++ _)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
      .toList

    assert(probeOpExec.onFinish(probe).isEmpty)

    assert(outputTuples.size == 3)
    assert(outputTuples.head.getFields.length == 3)

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
        buildOpExec.processTuple(tuple("same", 1, Some(i)), build).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](
      "same",
      JoinType.FULL_OUTER
    )

    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head)),
            build
          )
          .isEmpty
      )
    }
    assert(probeOpExec.onFinish(build).isEmpty)

    buildOpExec.close()

    assert(
      (5 to 9)
        .map(_ => {
          probeOpExec.processTuple(tuple("same", n = 2, None), probe)
        })
        .foldLeft(Iterator[TupleLike]())(_ ++ _)
        .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
        .size == 5
    )

    assert(probeOpExec.onFinish(probe).size == 8)

    probeOpExec.close()
  }
}
