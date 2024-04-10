package edu.uci.ics.amber.engine.common.storage
import java.net.URI
import java.nio.file.Path

// provide faster way of initializing a TexeraURI
object TexeraURI {

  val FILE_SCHEMA = "file"

  private val supportedSchemas = Set(FILE_SCHEMA)

  def apply(schema: String, uri: URI): TexeraURI = {
    if (!supportedSchemas.contains(schema)) {
      throw new RuntimeException("Given schema is not supported")
    }
    new TexeraURI(
      new URI(
        schema,
        uri.getUserInfo,
        uri.getHost,
        uri.getPort,
        uri.getPath,
        uri.getQuery,
        uri.getFragment
      )
    )
  }

  def apply(filePath: Path): TexeraURI = {
    apply(FILE_SCHEMA, filePath.toUri)
  }
}

/**
  * TexeraURI uniquely identifies each resource(either TexeraDocument or TexeraCollection) in Texera system.
  * Implementation-wise, it acts as a wrapper over java URI, providing better capabilities of restricting the schema of URI, and providing more native way of creating a URI
  * @param uri
  */
class TexeraURI private (val uri: URI) {
  // expose some URI operations
  def getScheme: String = uri.getScheme
  def getPath: String = uri.getPath
  def getHost: String = uri.getHost
  def getURI: URI = uri
  def containsChildPath(other: TexeraURI): Boolean = {
    getScheme == other.getScheme && other.getPath != getPath && other.getPath.startsWith(getPath)
  }
  override def toString: String = uri.toString

  override def equals(other: Any): Boolean =
    other match {
      case that: TexeraURI => this.uri == that.uri
      case _               => false
    }

  override def hashCode(): Int = uri.hashCode()
}
