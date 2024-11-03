package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.storage.model.{
  DatasetFileDocument,
  ReadonlyLocalFileDocument,
  ReadonlyVirtualDocument
}
import FileResolver.DATASET_FILE_URI_SCHEME

import java.net.URI

object DocumentFactory {
  def newReadonlyDocument(fileUri: URI): ReadonlyVirtualDocument[_] = {
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
