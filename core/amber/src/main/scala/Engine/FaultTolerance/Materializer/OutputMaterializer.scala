package Engine.FaultTolerance.Materializer

import Engine.Common.tuple.Tuple
import Engine.Common.{InputExhausted, OperatorExecutor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.{BufferedWriter, FileWriter}
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

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        writer.write(t.mkString("|"))
        Iterator()
      case Right(_) =>
        writer.close()
        if (remoteHDFS != null) {
          val fs = FileSystem.get(new URI(remoteHDFS), new Configuration())
          fs.copyFromLocalFile(new Path(outputPath), new Path(outputPath))
          fs.close()
        }
        Iterator()
    }
  }

}
