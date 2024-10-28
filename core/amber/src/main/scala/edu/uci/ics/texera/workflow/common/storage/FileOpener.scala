package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.storage.{
  DatasetFileDocument,
  ReadonlyLocalFileDocument,
  ReadonlyVirtualDocument
}
import edu.uci.ics.texera.workflow.common.storage.FileResolver.DATASET_FILE_URI_SCHEME

import java.net.URI

object FileOpener {
  type FileHandle = ReadonlyVirtualDocument[_]
  def openFile(fileUri: URI): FileHandle = {
    fileUri.getScheme match {
      case DATASET_FILE_URI_SCHEME =>
        new DatasetFileDocument(fileUri)

      case "file" =>
        // For local files, create a ReadonlyLocalFileDocument
        new ReadonlyLocalFileDocument(fileUri)

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported URI scheme: ${fileUri.getScheme}")
    }
  }
}
