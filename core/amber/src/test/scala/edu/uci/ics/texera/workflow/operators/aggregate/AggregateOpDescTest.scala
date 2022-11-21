package edu.uci.ics.texera.workflow.operators.aggregate

import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class AggregateOpDescTest extends AnyFlatSpec with BeforeAndAfter  {

  val testIntInputSchema = Schema.newBuilder()
    .add("group", AttributeType.STRING)
    .add("num", AttributeType.INTEGER).build()

  it should "infer correct SUM output schema" in {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.attribute = "num"
    aggOp.resultAttribute = "res"
    aggOp.groupByKeys = List("group")
    aggOp.aggFunction = AggregationFunction.SUM

    val outSchema = aggOp.getOutputSchema(Array(testIntInputSchema))
    assert(outSchema.containsAttribute("res"))
    assert(outSchema.getAttribute("res").getType == AttributeType.INTEGER)
  }

  it should "infer correct COUNT output schema" in {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.attribute = "num"
    aggOp.resultAttribute = "res"
    aggOp.groupByKeys = List("group")
    aggOp.aggFunction = AggregationFunction.COUNT

    val outSchema = aggOp.getOutputSchema(Array(testIntInputSchema))
    assert(outSchema.containsAttribute("res"))
    assert(outSchema.getAttribute("res").getType == AttributeType.INTEGER)
  }

  it should "infer correct AVERAGE output schema" in {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.attribute = "num"
    aggOp.resultAttribute = "res"
    aggOp.groupByKeys = List("group")
    aggOp.aggFunction = AggregationFunction.AVERAGE

    val outSchema = aggOp.getOutputSchema(Array(testIntInputSchema))
    assert(outSchema.containsAttribute("res"))
    assert(outSchema.getAttribute("res").getType == AttributeType.DOUBLE)
  }

  it should "infer correct MIN output schema" in {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.attribute = "num"
    aggOp.resultAttribute = "res"
    aggOp.groupByKeys = List("group")
    aggOp.aggFunction = AggregationFunction.MIN

    val outSchema = aggOp.getOutputSchema(Array(testIntInputSchema))
    assert(outSchema.containsAttribute("res"))
    assert(outSchema.getAttribute("res").getType == AttributeType.INTEGER)
  }

  it should "infer correct MAX output schema" in {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.attribute = "num"
    aggOp.resultAttribute = "res"
    aggOp.groupByKeys = List("group")
    aggOp.aggFunction = AggregationFunction.MAX

    val outSchema = aggOp.getOutputSchema(Array(testIntInputSchema))
    assert(outSchema.containsAttribute("res"))
    assert(outSchema.getAttribute("res").getType == AttributeType.INTEGER)
  }

}
