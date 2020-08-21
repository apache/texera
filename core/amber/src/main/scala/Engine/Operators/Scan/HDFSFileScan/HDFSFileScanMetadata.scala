package Engine.Operators.Scan.HDFSFileScan

import java.io.File
import java.net.URI

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.{RandomDeployment, RoundRobinDeployment}
import Engine.Architecture.DeploySemantics.DeploymentFilter.UseAll
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, GeneratorWorkerLayer}
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Common.TableMetadata
import Engine.Operators.OperatorMetadata
import Engine.Operators.Scan.FileScanMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import org.apache.hadoop.fs.{FileSystem, Path}

class HDFSFileScanMetadata (tag:OperatorTag, numWorkers:Int, val host:String, filePath:String, delimiter: Char, indicesToKeep:Array[Int], tableMetadata: TableMetadata) extends FileScanMetadata(tag,numWorkers,filePath,delimiter,indicesToKeep,tableMetadata)  {
  override val totalBytes: Long = FileSystem.get(new URI(host),new Configuration()).getFileStatus(new Path(filePath)).getLen
  println("Read from HDFS: "+filePath)
  override lazy val topology: Topology = {
    new Topology(Array(new GeneratorWorkerLayer(
      LayerTag(tag,"main"),
      i =>{
        val endOffset = if(i != numWorkers-1) totalBytes/numWorkers*(i+1) else totalBytes
        new HDFSFileScanTupleProducer(host,filePath,totalBytes/numWorkers*i,endOffset,delimiter,indicesToKeep,tableMetadata)
      },
      numWorkers,
      UseAll(), // it's source operator
      RoundRobinDeployment())),
      Array(),
      Map())
  }
  override def assignBreakpoint(topology: Array[ActorLayer], states: mutable.AnyRefMap[ActorRef, WorkerState.Value], breakpoint: GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_)!= WorkerState.Completed))
  }
}