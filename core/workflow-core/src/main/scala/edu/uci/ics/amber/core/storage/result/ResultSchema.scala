package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}

object ResultSchema {
  val runtimeStatisticsSchema: Schema = new Schema(
    new Attribute("operatorId", AttributeType.STRING),
    new Attribute("time", AttributeType.TIMESTAMP),
    new Attribute("inputTupleCnt", AttributeType.LONG),
    new Attribute("inputTupleSize", AttributeType.LONG),
    new Attribute("outputTupleCnt", AttributeType.LONG),
    new Attribute("outputTupleSize", AttributeType.LONG),
    new Attribute("dataProcessingTime", AttributeType.LONG),
    new Attribute("controlProcessingTime", AttributeType.LONG),
    new Attribute("idleTime", AttributeType.LONG),
    new Attribute("numWorkers", AttributeType.INTEGER),
    new Attribute("status", AttributeType.INTEGER)
  )

  val consoleMessagesSchema: Schema = new Schema(
    new Attribute("message", AttributeType.STRING)
  )
}
