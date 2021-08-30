package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinOpExec[K](
    val buildTable: LinkIdentity,
    val buildAttributeName: String,
    val probeAttributeName: String,
    val rightOuterJoin: Boolean,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  val buildSchema: Schema = operatorSchemaInfo.inputSchemas(0)
  val probeSchema: Schema = operatorSchemaInfo.inputSchemas(1)
  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, ArrayBuffer[Tuple]] = _
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
          val key = tuple.getField(buildAttributeName).asInstanceOf[K]
          val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += tuple
          buildTableHashMap.put(key, storedTuples)
          Iterator()
        } else if (!isBuildTableFinished) {

          throw new WorkflowRuntimeException("Probe table came before build table ended")
        } else {
          val key = tuple.getField(probeAttributeName).asInstanceOf[K]
          val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          if (storedTuples.isEmpty) {
            if (!rightOuterJoin) {
              return Iterator()
            }

            // creates a builder
            val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema)

            // fill the build tuple attributes as null, since no match
            buildSchema.getAttributesScala map { attribute => builder.add(attribute, null) }

            // append the probe tuple
            appendPartialTuple(builder, tuple)

            // build the new tuple
            Iterator(builder.build())
          } else {
            storedTuples
              .map(buildTuple => {
                // creates a builder with the build tuple filled
                val builder = Tuple
                  .newBuilder(operatorSchemaInfo.outputSchema)
                  .add(buildTuple)

                // append the probe tuple
                appendPartialTuple(builder, tuple)

                // build the new tuple
                builder.build()
              })
              .toIterator
          }

        }
      case Right(_) =>
        if (input == buildTable) {
          isBuildTableFinished = true
        }
        Iterator()

    }
  }

  def appendPartialTuple(builder: Tuple.BuilderV2, tuple: Tuple): Unit = {
    // outputProbeSchema doesnt have "probeAttribute" but t does. The following code
    //  takes that into consideration while creating a tuple.
    for (i <- 0 until tuple.getFields.size()) {
      val attributeName = tuple.getSchema.getAttributeNames.get(i)
      val attribute = tuple.getSchema.getAttribute(attributeName)

      if (attributeName != probeAttributeName) {
        builder.add(
          new Attribute(
            if (buildSchema.getAttributeNames.contains(attributeName))
              attributeName + "#@1"
            else attributeName,
            attribute.getType
          ),
          tuple.getFields.get(i)
        )
      }
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

}
