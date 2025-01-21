package edu.uci.ics.amber.operator.hashJoin

import edu.uci.ics.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple,
  TupleLike
}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.operator.hashJoin.HashJoinBuildOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper
class HashJoinOpSpec extends AnyFlatSpec with BeforeAndAfter {
  val build: Int = 0
  val probe: Int = 1

  var buildOpExec: HashJoinBuildOpExec[String] = _
  var probeOpExec: HashJoinProbeOpExec[String] = _
  var opDesc: HashJoinOpDesc[String] = _

  def getInternalHashTableSchema(buildInputSchema: Schema): Schema = {
    Schema()
      .add(HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.ANY)
      .add(buildInputSchema)

  }

  def tuple(name: String, n: Int = 1, i: Option[Int]): Tuple = {

    Tuple
      .builder(schema(name, n))
      .addSequentially(Array[Any](i.map(_.toString).orNull, i.map(_.toString).orNull))
      .build()
  }

  def schema(name: String, n: Int = 1): Schema = {
    Schema()
      .add(new Attribute(name, AttributeType.STRING))
      .add(new Attribute(name + "_" + n, AttributeType.STRING))

  }

  it should "work with basic two input streams with different buildAttributeName and probeAttributeName" in {

    opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "build_1"
    opDesc.probeAttributeName = "probe_1"
    opDesc.joinType = JoinType.INNER
    val inputSchemas = Map(PortIdentity() -> schema("build"), PortIdentity(1) -> schema("probe"))
    val outputSchema = opDesc.getExternalOutputSchemas(inputSchemas).values.head

    buildOpExec = new HashJoinBuildOpExec[String](objectMapper.writeValueAsString(opDesc))
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTuple(tuple("build", 1, Some(i)), build).isEmpty
      )
    })

    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](objectMapper.writeValueAsString(opDesc))

    probeOpExec.open()
    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head._2)),
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
    opDesc.joinType = JoinType.INNER
    val inputSchemas =
      Map(PortIdentity() -> schema("same", 1), PortIdentity(1) -> schema("same", 2))
    val outputSchema = opDesc.getExternalOutputSchemas(inputSchemas).values.head

    buildOpExec = new HashJoinBuildOpExec[String](objectMapper.writeValueAsString(opDesc))
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTuple(tuple("same", 1, Some(i)), build).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](objectMapper.writeValueAsString(opDesc))
    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head._2)),
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
    opDesc.joinType = JoinType.FULL_OUTER
    val inputSchemas =
      Map(PortIdentity() -> schema("same", 1), PortIdentity(1) -> schema("same", 2))
    val outputSchema = opDesc.getExternalOutputSchemas(inputSchemas).values.head

    buildOpExec = new HashJoinBuildOpExec[String](objectMapper.writeValueAsString(opDesc))
    buildOpExec.open()

    (0 to 7).map(i => {
      assert(
        buildOpExec.processTuple(tuple("same", 1, Some(i)), build).isEmpty
      )
    })
    val buildOpOutputIterator =
      buildOpExec.onFinish(build)
    assert(buildOpOutputIterator.hasNext)

    probeOpExec = new HashJoinProbeOpExec[String](objectMapper.writeValueAsString(opDesc))
    probeOpExec.open()

    while (buildOpOutputIterator.hasNext) {
      assert(
        probeOpExec
          .processTuple(
            buildOpOutputIterator
              .next()
              .asInstanceOf[SchemaEnforceable]
              .enforceSchema(getInternalHashTableSchema(inputSchemas.head._2)),
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
