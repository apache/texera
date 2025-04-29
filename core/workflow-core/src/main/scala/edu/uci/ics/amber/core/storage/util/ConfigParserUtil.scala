package edu.uci.ics.amber.core.storage.util

object ConfigParserUtil {
  def parseSizeStringToBytes(size: String): Long = {
    val sizePattern = """(\d+)([KMG]B)""".r
    size match {
      case sizePattern(value, unit) =>
        val multiplier = unit match {
          case "KB" => 1024L
          case "MB" => 1024L * 1024
          case "GB" => 1024L * 1024 * 1024
        }
        value.toLong * multiplier
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid s3 multipart part-size format in StorageConfig.scala with value $size"
        )
    }
  }
}
