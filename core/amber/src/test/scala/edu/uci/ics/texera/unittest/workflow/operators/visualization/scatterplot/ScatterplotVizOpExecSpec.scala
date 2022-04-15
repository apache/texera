package edu.uci.ics.texera.unittest.workflow.operators.visualization.scatterplot

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.scatterplot.{
  ScatterplotOpDesc,
  ScatterplotOpExec
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class ScatterplotVizOpExecSpec extends AnyFlatSpec with BeforeAndAfter {

  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("field1", AttributeType.DOUBLE))
    .add(new Attribute("field2", AttributeType.DOUBLE))
    .build()

  val desc: ScatterplotOpDesc = new ScatterplotOpDesc()
  val tuple: Tuple = Tuple
    .newBuilder(tupleSchema)
    .add(new Attribute("field1", AttributeType.DOUBLE), 73.142)
    .add(new Attribute("field2", AttributeType.DOUBLE), 32.33)
    .build()

  //another tuple with slightly different latitude value to test pixel based visualization
  val extratuple: Tuple = Tuple
    .newBuilder(tupleSchema)
    .add(new Attribute("field1", AttributeType.DOUBLE), 73.142)
    .add(new Attribute("field2", AttributeType.DOUBLE), 32.22)
    .build()
  var scatterplotOpExec: ScatterplotOpExec = _

  before {
    desc.isGeometric = true
    desc.xColumn = "field1"
    desc.yColumn = "field2"
    val outputSchema: Schema = desc.getOutputSchema(Array(tupleSchema))
    val operatorSchemaInfo: OperatorSchemaInfo =
      OperatorSchemaInfo(Array(tupleSchema), Array(outputSchema))
    scatterplotOpExec = new ScatterplotOpExec(desc, operatorSchemaInfo)
    scatterplotOpExec.open()
  }

  it should "output the correct schema for geometric for the frontend to render it and process only one tuple because the other falls on the same pixel" in {
    val processedTuple: Tuple = scatterplotOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("xColumn").asInstanceOf[Double] == 73.142)
    assert(processedTuple.getField("yColumn").asInstanceOf[Double] == 32.33)
    val outputTuples: List[Tuple] =
      scatterplotOpExec.processTexeraTuple(Left(extratuple), null).toList
    assert(outputTuples.size == 0)
  }

  it should "process the both points with the correct schema when the type is not geometric" in {
    desc.isGeometric = false
    val outputSchema: Schema = desc.getOutputSchema(Array(tupleSchema))
    val operatorSchemaInfo: OperatorSchemaInfo =
      OperatorSchemaInfo(Array(tupleSchema), Array(outputSchema))
    scatterplotOpExec = new ScatterplotOpExec(desc, operatorSchemaInfo)
    scatterplotOpExec.open()
    val processedTuple: Tuple = scatterplotOpExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("field1").asInstanceOf[Double] == 73.142)
    assert(processedTuple.getField("field2").asInstanceOf[Double] == 32.33)
    val processedAnotherTuple: Tuple =
      scatterplotOpExec.processTexeraTuple(Left(extratuple), null).next()
    assert(processedAnotherTuple.getField("field1").asInstanceOf[Double] == 73.142)
    assert(processedAnotherTuple.getField("field2").asInstanceOf[Double] == 32.22)
  }

  it should "handle last tuple correctly" in {
    val outputTuples: List[Tuple] =
      scatterplotOpExec.processTexeraTuple(Right(InputExhausted()), null).toList
    assert(outputTuples.size == 0)
  }

  after {
    scatterplotOpExec.close()
  }
}
