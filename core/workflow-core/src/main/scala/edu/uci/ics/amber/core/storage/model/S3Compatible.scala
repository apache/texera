package edu.uci.ics.amber.core.storage.model

trait S3Compatible {
  def getRepoName(): String

  def getBucketName(): String

  def getVersionHash(): String

  def getObjectRelativePath(): String
}
