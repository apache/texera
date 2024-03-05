package edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import java.nio.file.Path

// This file
class DatasetFileDesc(val fileName: Path, val datasetPath: Path, val versionHash: String) {
  def tempFilePath(): Path = {
    GitVersionControlLocalFileStorage.writeVersionedFileToTempFile(
      datasetPath,
      versionHash,
      datasetPath.resolve(fileName)
    )
  }

  override def toString: String =
    s"DatasetFileDesc(fileName=$fileName, datasetPath=$datasetPath, versionHash=$versionHash)"
}
