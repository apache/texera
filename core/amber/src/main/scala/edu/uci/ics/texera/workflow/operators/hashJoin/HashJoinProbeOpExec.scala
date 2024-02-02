package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple.BuilderV2
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinProbeOpExec[K](
    val buildAttributeName: String,
    val probeAttributeName: String,
    val joinType: JoinType,
    val buildSchema: Schema,
    val probeSchema: Schema,
    val outputSchema: Schema
) extends OperatorExecutor {
  var currentTuple: Tuple = _

  var buildTableHashMap: mutable.HashMap[K, (ListBuffer[Tuple], Boolean)] = _

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(tuple) =>
        if (input == 0) {
          buildTableHashMap.update(tuple.getField("key"), tuple.getField("value"))
          Iterator()
        } else {
          // probing phase
          val key = tuple.getField(probeAttributeName).asInstanceOf[K]
          val (matchedTuples, _) =
            buildTableHashMap.getOrElse(key, (new ListBuffer[Tuple](), false))

          if (matchedTuples.isEmpty) {
            // does not have a match with the probe tuple
            if (joinType != JoinType.RIGHT_OUTER && joinType != JoinType.FULL_OUTER) {
              return Iterator()
            }
            performRightAntiJoin(tuple)
          } else {
            // found a join match group
            buildTableHashMap.put(key, (matchedTuples, true))
            performJoin(tuple, matchedTuples)
          }
        }
      case Right(_) => {
        if (input == 0) {
          Iterator()
        } else {
          if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) {
            performLeftAntiJoin
          } else {
            Iterator()
          }
        }
      }
    }
  }

  private def performLeftAntiJoin: Iterator[Tuple] = {
    buildTableHashMap.valuesIterator
      .filter({ case (_: ListBuffer[Tuple], joined: Boolean) => !joined })
      .flatMap {
        case (tuples: ListBuffer[Tuple], _: Boolean) =>
          tuples
            .map((tuple: Tuple) => {
              // creates a builder
              val builder = Tuple.newBuilder(outputSchema)

              // fill the probe tuple attributes as null, since no match
              fillNonJoinFields(
                builder,
                probeSchema,
                Array.fill(probeSchema.getAttributesScala.length)(null),
                resolveDuplicateName = true
              )

              // fill the build tuple
              fillNonJoinFields(builder, buildSchema, tuple.getFields.toArray())

              // fill the join attribute (align with build)
              builder.add(
                buildSchema.getAttribute(buildAttributeName),
                tuple.getField(buildAttributeName)
              )

              // build the new tuple
              builder.build()
            })
            .toIterator
      }
  }

  def fillNonJoinFields(
      builder: BuilderV2,
      schema: Schema,
      fields: Array[Object],
      resolveDuplicateName: Boolean = false
  ): Unit = {
    schema.getAttributesScala.filter(attribute => attribute.getName != probeAttributeName) map {
      (attribute: Attribute) =>
        {
          val field = fields.apply(schema.getIndex(attribute.getName))
          if (resolveDuplicateName) {
            val attributeName = attribute.getName
            builder.add(
              new Attribute(
                if (buildSchema.getAttributeNames.contains(attributeName))
                  attributeName + "#@1"
                else attributeName,
                attribute.getType
              ),
              field
            )
          } else {
            builder.add(attribute, field)
          }
        }
    }
  }

  private def performJoin(probeTuple: Tuple, matchedTuples: ListBuffer[Tuple]): Iterator[Tuple] = {

    matchedTuples
      .map(buildTuple => {
        // creates a builder with the build tuple filled
        val builder = Tuple
          .newBuilder(outputSchema)
          .add(buildTuple)

        // append the probe tuple
        fillNonJoinFields(
          builder,
          probeSchema,
          probeTuple.getFields.toArray(),
          resolveDuplicateName = true
        )

        // build the new tuple
        builder.build()
      })
      .toIterator
  }

  private def performRightAntiJoin(tuple: Tuple): Iterator[Tuple] = {
    // creates a builder
    val builder = Tuple.newBuilder(outputSchema)

    // fill the build tuple attributes as null, since no match
    fillNonJoinFields(
      builder,
      buildSchema,
      Array.fill(buildSchema.getAttributesScala.length)(null)
    )

    // fill the probe tuple
    fillNonJoinFields(
      builder,
      probeSchema,
      tuple.getFields.toArray(),
      resolveDuplicateName = true
    )

    // fill the join attribute (align with probe)
    builder.add(
      buildSchema.getAttribute(buildAttributeName),
      tuple.getField(probeAttributeName)
    )

    // build the new tuple
    Iterator(builder.build())
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ListBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

}
