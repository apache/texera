package edu.uci.ics.texera.workflow.operators.util

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.storage.{DatasetDirectoryDocument, DatasetFileDocument}

import java.nio.file.Paths

object OperatorFilePathUtils {
  // resolve the file path based on whether the user system is enabled
  // it will check for the presence of the given filePath/Desc based on fileName
  def determineFilePathOrDatasetFile(
      fileName: Option[String],
      isFile: Boolean = true
  ): (String, DatasetDirectoryDocument) = {
    if (AmberConfig.isUserSystemEnabled) {
      // if user system is defined, a datasetFileDesc will be initialized, which is the handle of reading file from the dataset
      val datasetFile = Some(new DatasetDirectoryDocument(Paths.get(fileName.get), isFile))
      val file = datasetFile.getOrElse(
        throw new RuntimeException("Dataset file descriptor is not provided.")
      )
      (null, file)
    } else {
      // otherwise, the fileName will be inputted by user, which is the filePath.
      val filepath = fileName.getOrElse(throw new RuntimeException("File path is not provided."))
      (filepath, null)
    }
  }
}
