package edu.uci.ics.amber.engine.common.storage
import org.apache.commons.vfs2.{FileObject, VFS}
import java.io.{File, InputStream, FileOutputStream, IOException}
import java.net.URI

class FileDocument(val uri: URI) extends VirtualDocument[AnyRef] {
  val file: FileObject = VFS.getManager.resolveFile(uri.toString)
  // Ensure the file and its parent directories exist
  if (!file.exists()) {
    val parentDir = file.getParent
    if (parentDir != null && !parentDir.exists()) {
      parentDir.createFolder() // Create all necessary parent directories
    }
    file.createFile()
  }

  // append content in the inputStream to the FileDocument
  override def writeWithStream(inputStream: InputStream): Unit = {
    // append mode
    val outStream = file.getContent.getOutputStream(true)
    try {
      val buffer = new Array[Byte](1024)
      var len = inputStream.read(buffer)
      while (len != -1) {
        outStream.write(buffer, 0, len)
        len = inputStream.read(buffer)
      }
    } finally {
      outStream.close()
      inputStream.close()
    }
  }

  override def asInputStream(): InputStream = {
    if (!file.exists()) {
      throw new RuntimeException(f"File $uri doesn't exist")
    }
    file.getContent.getInputStream
  }

  override def getURI: URI = uri

  override def remove(): Unit = {
    if (!file.exists()) {
      throw new RuntimeException(f"File $uri doesn't exist")
    }
    file.delete()
  }
}
