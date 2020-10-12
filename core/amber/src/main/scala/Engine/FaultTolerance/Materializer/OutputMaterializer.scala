package Engine.FaultTolerance.Materializer

import Engine.Common.tuple.Tuple
import Engine.Common.OperatorExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.{FileWriter, BufferedWriter}
import java.net.URI

class OutputMaterializer(val outputPath: String, val remoteHDFS: String = null)
    extends OperatorExecutor {

  var writer: BufferedWriter = _

  override def open(): Unit = {
    writer = new BufferedWriter(new FileWriter(outputPath))
  }

  override def close(): Unit = {
    writer.close()
  }

  override def processTuple(tuple: Tuple, input: Int): scala.Iterator[Tuple] = {
    writer.write(tuple.mkString("|"))
    Iterator()
  }

  override def inputExhausted(input: Int): Iterator[Tuple] = {
    writer.close()
    if (remoteHDFS != null) {
      val fs = FileSystem.get(new URI(remoteHDFS), new Configuration())
      fs.copyFromLocalFile(new Path(outputPath), new Path(outputPath))
      fs.close()
    }
    Iterator()
  }
}
