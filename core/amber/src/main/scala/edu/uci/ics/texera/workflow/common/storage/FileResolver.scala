package edu.uci.ics.texera.workflow.common.storage

import java.nio.file.{Files, Path, Paths}
import edu.uci.ics.amber.engine.common.storage.DatasetFileDocument
import org.apache.commons.vfs2.FileNotFoundException

import scala.util.{Failure, Success, Try}

object FileResolver {

  type ResolverOutput = Either[String, DatasetFileDocument]

  /**
    * Attempts to resolve the given fileName using a list of resolver functions.
    *
    * @param fileName the name of the file to resolve
    * @return Either[String, DatasetFileDocument] - the resolved path as a String or a DatasetFileDocument
    */
  def resolve(fileName: String): ResolverOutput = {
    val resolvers: List[String => ResolverOutput] = List(localResolveFunc, remoteResolveFunc)

    // Try each resolver function in sequence
    resolvers.iterator
      .map(resolver => Try(resolver(fileName)))
      .collectFirst {
        case Success(output) => output
      }
      .getOrElse(throw new FileNotFoundException(s"Source file '$fileName' could not be resolved"))
  }

  /**
    * Attempts to resolve a local file path.
    *
    * @param fileName the name of the file to check
    */
  private def localResolveFunc(fileName: String): ResolverOutput = {
    val filePath = Paths.get(fileName)
    if (Files.exists(filePath)) {
      Left(fileName) // File exists locally, return the path as a string in the Left
    } else {
      throw new FileNotFoundException(s"Local file '$fileName' does not exist")
    }
  }

  /**
    * Attempts to resolve a DatasetFileDocument.
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    */
  private def remoteResolveFunc(fileName: String): ResolverOutput = {
    val filePath = Paths.get(fileName)
    Try(new DatasetFileDocument(filePath)) match {
      case Success(document) => Right(document)
      case Failure(_)        => throw new FileNotFoundException(s"Remote file '$fileName' cannot be resolved as DatasetFileDocument")
    }
  }
}