package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource
import org.jooq.types.UInteger

import java.io.{File, FileInputStream, FileNotFoundException, InputStream, OutputStream, FileOutputStream}
import java.net.URI
import java.nio.file.{Files, Path, Paths}

class DatasetFileDocument(uid: UInteger, fileFullPath: Path) extends VirtualDocument[Nothing] {

  // Extract did, dvid, datasetName, versionName, and fileRelativePath from the file path
  // The format is /did:123_dvid:456/datasetName/versionName/a/b/c/d
  private val pattern = """/did:(\d+)_dvid:(\d+)/([^/]+)/([^/]+)/(.+)""".r
  private val (did, dvid, datasetName, versionName, fileRelativePath) = fileFullPath.toString match {
    case pattern(didStr, dvidStr, datasetStr, versionStr, pathStr) =>
      (Some(didStr.toInt), Some(dvidStr.toInt), datasetStr, versionStr, Paths.get(pathStr))
    case _ => (None, None, "", "", fileFullPath)
  }

  private var tempFile: Option[File] = None

  require(did.isDefined && dvid.isDefined, "given file path is not valid")

  override def getURI: URI = throw new UnsupportedOperationException("The URI cannot be acquired because the file is not physically located")

  override def asInputStream(): InputStream = DatasetResource.getDatasetFile(UInteger.valueOf(did.get), UInteger.valueOf(dvid.get), uid, fileRelativePath)

  override def asFile(): File = {
    tempFile match {
      case Some(file) => file
      case None =>
        val tempFilePath = Files.createTempFile("versionedFile", ".tmp")
        val tempFileStream = new FileOutputStream(tempFilePath.toFile)
        val inputStream = asInputStream()

        val buffer = new Array[Byte](1024)

        // Create an iterator to repeatedly call inputStream.read, and direct buffered data to file
        Iterator
          .continually(inputStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(tempFileStream.write(buffer, 0, _))

        inputStream.close()
        tempFileStream.close()

        val file = tempFilePath.toFile
        tempFile = Some(file)
        file
    }
  }

  override def remove(): Unit = {
    tempFile match {
      case Some(file) => Files.delete(file.toPath)
      case None => // Do nothing
    }
  }
}
