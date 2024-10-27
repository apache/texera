package edu.uci.ics.texera.workflow.common.storage

import java.nio.file.{Files, Paths}
import edu.uci.ics.amber.engine.common.storage.DatasetFileDocument
import org.apache.commons.vfs2.FileNotFoundException

import scala.util.{Success, Try}

object FileResolver {

  type FileResolverOutput = Either[String, DatasetFileDocument]

  /**
    * Attempts to resolve the given fileName using a list of resolver functions.
    *
    * @param fileName the name of the file to resolve
    * @throws FileNotFoundException if the file cannot be resolved by any resolver
    * @return Either[String, DatasetFileDocument] - the resolved path as a String or a DatasetFileDocument
    */
  def resolve(fileName: String): FileResolverOutput = {
    val resolvers: List[String => FileResolverOutput] = List(localResolveFunc, datasetResolveFunc)

    // Try each resolver function in sequence
    resolvers.iterator
      .map(resolver => Try(resolver(fileName)))
      .collectFirst {
        case Success(output) => output
      }
      .getOrElse(throw new FileNotFoundException(fileName))
  }

  /**
    * Attempts to resolve a local file path.
    * @throws FileNotFoundException if the local file does not exist
    * @param fileName the name of the file to check
    */
  private def localResolveFunc(fileName: String): FileResolverOutput = {
    val filePath = Paths.get(fileName)
    if (Files.exists(filePath)) {
      Left(fileName) // File exists locally, return the path as a string in the Left
    } else {
      throw new FileNotFoundException(s"Local file $fileName does not exist")
    }
  }

  /**
    * Attempts to resolve a DatasetFileDocument.
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    * @return Either[String, DatasetFileDocument] - Right(document) if creation succeeds
    * @throws FileNotFoundException if the dataset file does not exist or cannot be created
    */
  private def datasetResolveFunc(fileName: String): FileResolverOutput = {
    val filePath = Paths.get(fileName)
    val document = new DatasetFileDocument(filePath) // This will throw if creation fails
    Right(document)
  }
}
