package edu.uci.ics.amber.operator.source.scan.csv

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeTypeUtils, Schema, TupleLike}
import org.tukaani.xz.SeekableFileInputStream

import java.io.File
import java.util
import java.util.stream.{IntStream, Stream}
import scala.collection.compat.immutable.ArraySeq

class ParallelCSVScanSourceOpExec private[csv] (
    file: File,
    customDelimiter: Option[String],
    hasHeader: Boolean,
    startOffset: Long,
    endOffset: Long,
    schemaFunc: () => Schema
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var reader: BufferedBlockReader = _

  override def produceTuple(): Iterator[TupleLike] =
    new Iterator[TupleLike]() {
      override def hasNext: Boolean = reader.hasNext

      override def next(): TupleLike = {

        try {
          // obtain String representation of each field
          // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
          val line = reader.readLine
          if (line == null) {
            return null
          }
          var fields: Array[AnyRef] = line.toArray

          if (fields == null || util.Arrays.stream(fields).noneMatch(s => s != null)) {
            // discard tuple if it's null or it only contains null
            // which means it will always discard Tuple(null) from readLine()
            return null
          }

          // however the null values won't present if omitted in the end, we need to match nulls.
          if (fields.length != schema.getAttributes.size)
            fields = Stream
              .concat(
                util.Arrays.stream(fields),
                IntStream
                  .range(0, schema.getAttributes.size - fields.length)
                  .mapToObj((_: Int) => null)
              )
              .toArray()
          // parse Strings into inferred AttributeTypes
          val parsedFields: Array[Any] = AttributeTypeUtils.parseFields(
            fields.asInstanceOf[Array[Any]],
            schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
          )
          TupleLike(ArraySeq.unsafeWrapArray(parsedFields): _*)
        } catch {
          case _: Throwable => null
        }
      }

    }.filter(tuple => tuple != null)

  override def open(): Unit = {
    val stream = new SeekableFileInputStream(file)
    schema = schemaFunc()
    stream.seek(startOffset)
    reader = new BufferedBlockReader(
      stream,
      endOffset - startOffset,
      customDelimiter.get.charAt(0),
      null
    )
    // skip line if this worker reads from middle of a file
    if (startOffset > 0) reader.readLine
    // skip line if this worker reads the start of a file, and the file has a header line
    if (startOffset == 0 && hasHeader) reader.readLine
  }

  override def close(): Unit = reader.close()

}
