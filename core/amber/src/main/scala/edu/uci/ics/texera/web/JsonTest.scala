package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.architecture.logreplay._
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.texera.ExpUtils._

import scala.collection.mutable

object JsonTest {

  def getDistribution(iter:Iterator[ReplayLogRecord]): (Int, Int, Int, Double) = {
    var input = 0
    var output = 0
    var dest = 0
    val avg_step = mutable.ArrayBuffer[Long]()
    var end_step = 0L
    var start_step = 0L
    iter.foreach {
      case MessageContent(message) =>
        dest +=1
        avg_step.append(end_step-start_step)
        start_step = 0
        end_step = 0
      case OutputRecord(portIdOpt) =>
        output +=1
      case ProcessingStep(channelId, step) =>
        input += 1
        if(start_step == 0){
          start_step = step
        }
        end_step = step
      case ReplayDestination(id) =>
        dest += 1
      case TerminateSignal => ???
    }

    var avg_step_double = 0.0
    if(avg_step.nonEmpty){
      avg_step_double = avg_step.toArray.sum/avg_step.size
    }
    (input, output, dest, avg_step_double)
  }

  import scala.io.StdIn


  def doExperiment(): Unit = {
    val result = mutable.ArrayBuffer[String]()
    List(20).foreach{
      udf =>
        List(1,10,100,1000).foreach {
          slow =>
            List(true).foreach {
              scatter =>
                List("flink-async").foreach {
                  method =>
                    (0 until 3).foreach {
                      repeat =>
                        result.append(runExp5(AmberRuntime.actorSystem, udf, scatter, method, 1, slow))
                    }
                }
            }
        }
    }
    List(20).foreach{
      udf =>
        List(1,10,100,1000).foreach {
          slow =>
            List(true).foreach {
              scatter =>
                List("flink-async").foreach {
                  method =>
                    (0 until 3).foreach {
                      repeat =>
                        result.append(runExp6(AmberRuntime.actorSystem, udf, scatter, method, 1, slow))
                    }
                }
            }
        }
    }
    result.foreach{
      s =>
        println(s)
    }
  }

  def waitForS(): Unit = {
    var input = ""
    while (input.trim.toLowerCase != "s") {
      println("Press 's' to start...")
      input = StdIn.readLine()
    }
    println(runExp8(AmberRuntime.actorSystem, 2, isScattered = false, "flink-sync", 1000))
  }

  def main(args: Array[String]): Unit = {

//    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
//      Some(new URI("file:///Users/shengquanni/Documents/GitHub/IcedTea/core/log/recovery-logs/WorkflowIdentity(0)/ExecutionIdentity(5)"))
//    )
//    logStorage.listFiles.foreach{
//      file =>
//        if(file != "CONTROLLER"){
//          val a = getDistribution(logStorage.getReader(file).mkRecordIterator())
//          println(s"$file, ${a._1}, ${a._2}, ${a._3}, ${a._4}")
//        }
//    }

    AmberRuntime.startActorMaster(false)
    // Call the function
    waitForS()

  }
}

class JsonTest {}
