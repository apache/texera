package Engine.FaultTolerance.Materializer

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.{FileWriter, BufferedWriter}
import java.net.URI

class HashBasedMaterializer(
    val outputPath: String,
    val index: Int,
    val hashFunc: Tuple => Int,
    val numBuckets: Int,
    val remoteHDFS: String = null
) extends TupleProcessor {

  var writer: Array[BufferedWriter] = _

  override def accept(tuple: Tuple): Unit = {
    val index = (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
    writer(index).write(tuple.mkString("|"))
  }

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def noMore(): Unit = {
    for (i <- 0 until numBuckets) {
      writer(i).close()
    }
    if (remoteHDFS != null) {
      val fs = FileSystem.get(new URI(remoteHDFS), new Configuration())
      for (i <- 0 until numBuckets) {
        fs.copyFromLocalFile(
          new Path(outputPath + "/" + index + "/" + i + ".tmp"),
          new Path(outputPath + "/" + i + "/" + index + ".tmp")
        )
      }
      fs.close()
    }
  }

  override def initialize(): Unit = {
    writer = new Array[BufferedWriter](numBuckets)
    for (i <- 0 until numBuckets) {
      writer(i) = new BufferedWriter(new FileWriter(outputPath + "/" + index + "/" + i + ".tmp"))
    }
  }

  override def hasNext: Boolean = false

  override def next(): Tuple = ???

  override def dispose(): Unit = {
    writer.foreach(_.close())
  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
