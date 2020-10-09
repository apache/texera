package texera.operators.localscan

import Engine.Common.AmberTag.OperatorTag
import Engine.Common.TableMetadata
import Engine.Operators.OperatorMetadata

abstract class FileScanMetadata(
    tag: OperatorTag,
    val numWorkers: Int,
    val filePath: String,
    val delimiter: Char,
    val indicesToKeep: Array[Int],
    val tableMetadata: TableMetadata
) extends OperatorMetadata(tag) {
  val totalBytes: Long = 0

}
