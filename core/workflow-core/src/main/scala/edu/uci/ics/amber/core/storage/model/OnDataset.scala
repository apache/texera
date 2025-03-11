package edu.uci.ics.amber.core.storage.model

trait OnDataset {
  def getDatasetName(): String

  def getVersionHash(): String

  def getFileRelativePath(): String
}
