package edu.uci.ics.texera.workflow.common.storage

import java.nio.file.{Files, Path, Paths}
import edu.uci.ics.amber.engine.common.storage.DatasetFileDocument
import scala.util.{Failure, Success, Try}

object ScanSourceFileResolver {

  case class SourceFileNotFound(message: String) extends Exception(message)

  /**
    * Attempts to resolve the given fileName to either a local file path or a DatasetFileDocument.
    *
    * @param fileName the name of the file to resolve
    * @return a tuple of (Option[Path], Option[DatasetFileDocument])
    */
  def resolve(fileName: String): (Option[String], Option[DatasetFileDocument]) = {
    val filePath = Paths.get(fileName)

    // Check if the file exists in the local filesystem
    if (Files.exists(filePath)) {
      // File exists locally, return Some(path) and None for the DatasetFileDocument
      (Some(fileName), None)
    } else {
      // File does not exist locally, try to create a DatasetFileDocument
      Try(new DatasetFileDocument(filePath)) match {
        case Success(document) =>
          (None, Some(document)) // Return None for path and Some(document) if successful
        case Failure(_) =>
          // If creating the document fails, throw SourceFileNotFound error
          throw SourceFileNotFound(
            s"Source file '$fileName' could not be resolved as a DatasetFileDocument"
          )
      }
    }
  }
}
