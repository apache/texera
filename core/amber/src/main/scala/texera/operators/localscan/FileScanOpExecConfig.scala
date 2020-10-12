package texera.operators.localscan

import Engine.Common.AmberTag.OperatorTag
import Engine.Common.TableMetadata
import Engine.Operators.OpExecConfig

abstract class FileScanOpExecConfig(
    tag: OperatorTag,
    val numWorkers: Int,
    val filePath: String,
    val delimiter: Char,
    val indicesToKeep: Array[Int],
    val tableMetadata: TableMetadata
) extends OpExecConfig(tag) {
  val totalBytes: Long = 0

}
