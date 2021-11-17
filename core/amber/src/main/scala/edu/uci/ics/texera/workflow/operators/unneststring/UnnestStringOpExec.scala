package edu.uci.ics.texera.workflow.operators.unneststring

import edu.uci.ics.texera.workflow.common.operators.flatmap.FlatMapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo
}

class UnnestStringOpExec(opDesc: UnnestStringOpDesc, operatorSchemaInfo: OperatorSchemaInfo)
    extends FlatMapOpExec {

  def splitByDelimiter(tuple: Tuple): Iterator[Tuple] = {

    val tupleValue = tuple.getField(this.opDesc.attribute).toString
    val dataIterator = this.opDesc.delimiter.r.split(tupleValue).filter(!_.equals("")).toIterator

    dataIterator.map(split => {
      Tuple
        .newBuilder(operatorSchemaInfo.outputSchema)
        .add(tuple)
        .add(opDesc.resultAttribute, AttributeType.STRING, split)
        .build()
    })
  }
  setFlatMapFunc(splitByDelimiter)
}
