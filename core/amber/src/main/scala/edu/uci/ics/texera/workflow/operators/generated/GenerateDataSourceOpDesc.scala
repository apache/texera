package edu.uci.ics.texera.workflow.operators.generated

import java.util.Collections.singletonList

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.immutable.List

class GenerateDataSourceOpDesc extends SourceOperatorDescriptor{

  @JsonProperty(value = "generate limit", required = true)
  @JsonPropertyDescription("for each worker, how many tuple to generate")
  val limit:Int = 0

  @JsonProperty(value = "key range", required = true)
  @JsonPropertyDescription("give a range of the key. e.g. 0,10 means the key of a tuple will be choose from [0,10]")
  val keyRange:String = "0,10"

  @JsonProperty(value = "distributions")
  @JsonPropertyDescription("give the distribution over the generating process." +
    "e.g. 100, 2:60, 3:10 means after generating 100 tuple, key 2's ratio will be 60%, key 3 will be 10%, other key's are evenly divided in the remaining 30%")
  val distributions:List[String] = List.empty

  @JsonProperty(value = "delay")
  @JsonPropertyDescription("delay for generating every x tuples. e.g. 2000,1000 means after generating 2000 tuples, sleep 1000ms")
  val delay:String = "0,0"

  override def operatorExecutor: OpExecConfig ={
    val keyRangeArr = keyRange.split(",").map(_.toInt)
    val delayArr = delay.split(",").map(_.toInt)
    val distributionsArr = distributions.map{
      line =>
        val arr = line.split(",")
        val ratioArr = arr.drop(1).map(x => {
          val ratio = x.split(":")
          (ratio(0).toInt,ratio(1).toInt)
        })
        (arr.head.toInt, ratioArr)
    }

    new GenerateDataSourceOpExecConfig(operatorIdentifier, Constants.defaultNumWorkers, sourceSchema(),
      limit,
      (keyRangeArr(0), keyRangeArr(1)),
      distributionsArr.toArray,
      (delayArr(0), delayArr(1))
      )
  }

  override def sourceSchema(): Schema = {
    val builder = Schema.newBuilder()
    builder.add(new Attribute("key",AttributeType.INTEGER))
    builder.add(new Attribute("value", AttributeType.INTEGER))
    builder.build()
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo("Generate Data", "generate data", OperatorGroupConstants.SOURCE_GROUP, List.empty, asScalaBuffer(singletonList(new OutputPort(""))).toList)
  }
}
