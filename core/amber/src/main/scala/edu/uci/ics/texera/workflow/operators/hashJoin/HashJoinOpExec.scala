package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple.BuilderV2
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinOpExec[K](
    val buildTable: LinkIdentity,
    val buildAttributeName: String,
    val probeAttributeName: String,
    val joinType: JoinType,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  val buildSchema: Schema = operatorSchemaInfo.inputSchemas(0)
  val probeSchema: Schema = operatorSchemaInfo.inputSchemas(1)
  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, (ArrayBuffer[Tuple], Boolean)] = _
  var outputProbeSchema: Schema = operatorSchemaInfo.outputSchema

  var currentEntry: Iterator[Tuple] = _
  var currentTuple: Tuple = _

  val buildTableTransferBatchSize = 4000

  def getBuildHashTableBatches(): ArrayBuffer[mutable.HashMap[K, ArrayBuffer[Tuple]]] = {
    val sendingMap = new ArrayBuffer[mutable.HashMap[K, ArrayBuffer[Tuple]]]
    var count = 1
    var curr = new mutable.HashMap[K, ArrayBuffer[Tuple]]
    for ((key, tuples) <- buildTableHashMap) {
      curr.put(key, tuples._1)
      if (count % buildTableTransferBatchSize == 0) {
        sendingMap.append(curr)
        curr = new mutable.HashMap[K, ArrayBuffer[Tuple]]
      }
      count += 1
    }
    if (!curr.isEmpty) sendingMap.append(curr)
    sendingMap
  }

  /**
    * This function does not handle duplicates. It merges whatever it is given. It will treat
    * duplicate tuples of the key as new tuples and will append it. The responsibility to not send
    * duplicates is with the senders.
    */
  def mergeIntoHashTable(additionalTable: mutable.HashMap[Any, ArrayBuffer[Tuple]]): Boolean = {
    try {
      for ((key, tuples) <- additionalTable) {
        val (storedTuples, _) =
          buildTableHashMap.getOrElseUpdate(key.asInstanceOf[K], (new ArrayBuffer[Tuple](), false))
        storedTuples.appendAll(tuples)
      }
      true
    } catch {
      case exception: Exception =>
        false
    }
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(tuple) =>
        // The operatorInfo() in HashJoinOpDesc has a inputPorts list. In that the
        // small input port comes first. So, it is assigned the inputNum 0. Similarly
        // the large input is assigned the inputNum 1.

        if (input == buildTable) {
          // building phase
          building(tuple)
          Iterator()
        } else if (!isBuildTableFinished) {
          // should never happen, building phase has to finish before first probe
          throw new WorkflowRuntimeException("Probe table came before build table ended")
        } else {
          // probing phase
          val key = tuple.getField(probeAttributeName).asInstanceOf[K]
          val (matchedTuples, _) =
            buildTableHashMap.getOrElse(key, (new ArrayBuffer[Tuple](), false))

          if (matchedTuples.isEmpty) {
            // do not have a match with the probe tuple
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
      case Right(_) =>
        if (input == buildTable && !isBuildTableFinished) {
          // the first input is exhausted, building phase finished
          isBuildTableFinished = true
          Iterator()
        } else {
          // the second input is exhausted, probing phase finished
          if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) {
            performLeftAntiJoin
          } else {
            Iterator()
          }
        }
    }
  }

  private def performLeftAntiJoin: Iterator[Tuple] = {
    buildTableHashMap.valuesIterator
      .filter({ case (_: ArrayBuffer[Tuple], joined: Boolean) => !joined })
      .flatMap {
        case (tuples: ArrayBuffer[Tuple], _: Boolean) =>
          tuples
            .map((tuple: Tuple) => {
              // creates a builder
              val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema)

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

  private def performJoin(probeTuple: Tuple, matchedTuples: ArrayBuffer[Tuple]): Iterator[Tuple] = {

    matchedTuples
      .map(buildTuple => {
        // creates a builder with the build tuple filled
        val builder = Tuple
          .newBuilder(operatorSchemaInfo.outputSchema)
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

  private def performRightAntiJoin(tuple: Tuple): Iterator[Tuple] = {
    // creates a builder
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema)

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
      probeSchema.getAttribute(probeAttributeName),
      tuple.getField(probeAttributeName)
    )

    // build the new tuple
    Iterator(builder.build())
  }

  private def building(tuple: Tuple): Unit = {
    val key = tuple.getField(buildAttributeName).asInstanceOf[K]
    val (storedTuples, _) =
      buildTableHashMap.getOrElseUpdate(key, (new ArrayBuffer[Tuple](), false))
    storedTuples += tuple
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ArrayBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

}
