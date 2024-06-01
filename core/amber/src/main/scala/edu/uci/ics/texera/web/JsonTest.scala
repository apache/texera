package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.architecture.logreplay.{MessageContent, OutputRecord, ProcessingStep, ReplayDestination, ReplayLogRecord, TerminateSignal}
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage

import java.net.URI
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

  def main(args: Array[String]): Unit = {

    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
      Some(new URI("file:///Users/shengquanni/Documents/GitHub/texera/core/log/recovery-logs/WorkflowIdentity(25)/w3"))
    )
    logStorage.listFiles.foreach{
      file =>
        if(file != "CONTROLLER"){
          val a = getDistribution(logStorage.getReader(file).mkRecordIterator())
          println(s"$file, ${a._1}, ${a._2}, ${a._3}, ${a._4}")
        }
    }
  }
}

class JsonTest {}
