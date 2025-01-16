package edu.uci.ics.amber.core.storage.model

import edu.uci.ics.amber.core.storage.LakeFSFileStorage
import org.apache.commons.vfs2.FileNotFoundException

import java.io.{File, InputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * LakeFSFileDocument manages file operations on LakeFS.
 *
 * @param uri The LakeFS file URI in the format: lakefs://repoName/commitHash/objectPath
 */
private[storage] class LakeFSFileDocument(uri: URI) extends VirtualDocument[Nothing] {

  // Utility function to parse and decode URI into components
  private def parseUri(uri: URI): (String, String, String) = {
    val segments = Paths.get(uri.getPath).iterator().asScala.map(_.toString).toArray

    if (segments.length < 3)
      throw new IllegalArgumentException(s"Invalid LakeFS URI format: $uri")

    val repoName = URLDecoder.decode(segments(0), StandardCharsets.UTF_8)
    val commitHash = URLDecoder.decode(segments(1), StandardCharsets.UTF_8)
    val decodedPathSegments = segments.drop(2).map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))
    val objectPath = Paths.get(decodedPathSegments.head, decodedPathSegments.tail: _*).toString

    (repoName, commitHash, objectPath)
  }

  // Extract repoName, commitHash, and objectPath from the URI
  private val (repoName, commitHash, objectPath) = parseUri(uri)

  // Cache for the temporary file
  private var tempFile: Option[File] = None

  /**
   * Returns the URI of the LakeFS file.
   */
  override def getURI: URI = uri

  /**
   * Provides an InputStream of the LakeFS file content.
   */
  override def asInputStream(): InputStream = {
    try {
      Files.newInputStream(LakeFSFileStorage.retrieveFileContent(repoName, commitHash, objectPath).toPath)
    } catch {
      case _: Exception =>
        throw new FileNotFoundException(s"Failed to retrieve file from LakeFS: $uri")
    }
  }

  /**
   * Provides a local File object of the LakeFS file by downloading it temporarily.
   */
  override def asFile(): File = {
    tempFile match {
      case Some(file) => file
      case None =>
        tempFile = Some(LakeFSFileStorage.retrieveFileContent(repoName, commitHash, objectPath))
        tempFile.get
    }
  }

  /**
   * Deletes the temporary file and the object from LakeFS.
   */
  override def clear(): Unit = {
    // Delete temporary local file
    tempFile.foreach(file => Files.deleteIfExists(file.toPath))

    // Delete the object from LakeFS
    try {
      LakeFSFileStorage.removeFileFromRepo(repoName, commitHash, objectPath)
    } catch {
      case _: Exception =>
        throw new FileNotFoundException(s"Failed to delete file from LakeFS: $uri")
    }
  }
}