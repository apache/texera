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
    val leftOuterJoin: Boolean,
    val rightOuterJoin: Boolean,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  val buildSchema: Schema = operatorSchemaInfo.inputSchemas(0)
  val probeSchema: Schema = operatorSchemaInfo.inputSchemas(1)
  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, (ArrayBuffer[Tuple], Boolean)] = _
  var outputProbeSchema: Schema = operatorSchemaInfo.outputSchema

  var currentEntry: Iterator[Tuple] = _
  var currentTuple: Tuple = _

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
          building(tuple)
          Iterator()
        } else if (!isBuildTableFinished) {

          throw new WorkflowRuntimeException("Probe table came before build table ended")
        } else {

          val key = tuple.getField(probeAttributeName).asInstanceOf[K]
          val (matchedTuples, _) =
            buildTableHashMap.getOrElse(key, (new ArrayBuffer[Tuple](), false))
          if (matchedTuples.isEmpty) {
            if (!rightOuterJoin) {
              return Iterator()
            }
            performRightOuterJoin(tuple)
          } else {
            buildTableHashMap.put(key, (matchedTuples, true))
            performJoin(tuple, matchedTuples)
          }

        }
      case Right(_) =>
        if (input == buildTable && !isBuildTableFinished) {

          // the first input is exhausted, build phase finished
          isBuildTableFinished = true
          Iterator()
        } else {
          // the second input is exhausted, probe phase finished
          if (leftOuterJoin) {
            performLeftOuterJoin
          } else {
            Iterator()
          }
        }
    }
  }

  private def performLeftOuterJoin: Iterator[Tuple] = {
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

  private def performRightOuterJoin(tuple: Tuple): Iterator[Tuple] = {
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
