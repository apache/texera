package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.Table
import org.apache.iceberg.types.{Conversions, Types}

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
  * `TableStatistics` provides methods for extracting metadata statistics for the results
  *
  * **Statistics Computed:**
  * - **Numeric fields (Int, Long, Double)**: Computes `min` and `max`.
  * - **Date fields (Timestamp)**: Computes `min` and `max` (converted to `LocalDate`).
  * - **All fields**: Computes `not_null_count` (number of non-null values).
  */
case class TableStatistics(table: Table) {

  /**
    * Computes metadata statistics for all fields in the Iceberg table.
    *
    * @param table The Iceberg table to analyze.
    *
    * @return A `Map` where keys are field names and values are statistics (`min`, `max`, `not_null_count`).
    */
  def getTableStatistics: Map[String, Map[String, Any]] = {
    val schema = table.schema()

    val fieldTypes =
      schema.columns().asScala.map(col => col.name() -> (col.fieldId(), col.`type`())).toMap
    val fieldStats = mutable.Map[String, mutable.Map[String, Any]]()
    val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    fieldTypes.foreach {
      case (field, (_, fieldType)) =>
        val initialStats = mutable.Map[String, Any](
          "not_null_count" -> 0L
        )
        if (
          fieldType == Types.IntegerType.get() || fieldType == Types.LongType
            .get() || fieldType == Types.DoubleType.get()
        ) {
          initialStats("min") = Double.MaxValue
          initialStats("max") = Double.MinValue
        } else if (
          fieldType == Types.TimestampType.withoutZone() || fieldType == Types.TimestampType
            .withZone()
        ) {
          initialStats("min") = LocalDate.MAX.format(dateFormatter)
          initialStats("max") = LocalDate.MIN.format(dateFormatter)
        }

        fieldStats(field) = initialStats
    }

    table.newScan().includeColumnStats().planFiles().iterator().asScala.foreach { file =>
      val fileStats = file.file()
      val lowerBounds =
        Option(fileStats.lowerBounds()).getOrElse(Map.empty[Integer, ByteBuffer].asJava)
      val upperBounds =
        Option(fileStats.upperBounds()).getOrElse(Map.empty[Integer, ByteBuffer].asJava)
      val nullCounts =
        Option(fileStats.nullValueCounts()).getOrElse(Map.empty[Integer, java.lang.Long].asJava)
      val nanCounts =
        Option(fileStats.nanValueCounts()).getOrElse(Map.empty[Integer, java.lang.Long].asJava)

      fieldTypes.foreach {
        case (field, (fieldId, fieldType)) =>
          val lowerBound = Option(lowerBounds.get(fieldId))
          val upperBound = Option(upperBounds.get(fieldId))
          val nullCount: Long = Option(nullCounts.get(fieldId)).map(_.toLong).getOrElse(0L)
          val nanCount: Long = Option(nanCounts.get(fieldId)).map(_.toLong).getOrElse(0L)
          val fieldStat = fieldStats(field)

          if (
            fieldType == Types.IntegerType.get() || fieldType == Types.LongType
              .get() || fieldType == Types.DoubleType.get()
          ) {
            lowerBound.foreach { buffer =>
              val minValue =
                Conversions.fromByteBuffer(fieldType, buffer).asInstanceOf[Number].doubleValue()
              fieldStat("min") = Math.min(fieldStat("min").asInstanceOf[Double], minValue)
            }

            upperBound.foreach { buffer =>
              val maxValue =
                Conversions.fromByteBuffer(fieldType, buffer).asInstanceOf[Number].doubleValue()
              fieldStat("max") = Math.max(fieldStat("max").asInstanceOf[Double], maxValue)
            }
          } else if (
            fieldType == Types.TimestampType.withoutZone() || fieldType == Types.TimestampType
              .withZone()
          ) {
            lowerBound.foreach { buffer =>
              val epochMicros = Conversions
                .fromByteBuffer(Types.TimestampType.withoutZone(), buffer)
                .asInstanceOf[Long]
              val dateValue =
                Instant.ofEpochMilli(epochMicros / 1000).atZone(ZoneOffset.UTC).toLocalDate
              fieldStat("min") =
                if (
                  dateValue
                    .isBefore(LocalDate.parse(fieldStat("min").asInstanceOf[String], dateFormatter))
                )
                  dateValue.format(dateFormatter)
                else
                  fieldStat("min")
            }

            upperBound.foreach { buffer =>
              val epochMicros = Conversions
                .fromByteBuffer(Types.TimestampType.withoutZone(), buffer)
                .asInstanceOf[Long]
              val dateValue =
                Instant.ofEpochMilli(epochMicros / 1000).atZone(ZoneOffset.UTC).toLocalDate
              fieldStat("max") =
                if (
                  dateValue
                    .isAfter(LocalDate.parse(fieldStat("max").asInstanceOf[String], dateFormatter))
                )
                  dateValue.format(dateFormatter)
                else
                  fieldStat("max")
            }
          }
          fieldStat("not_null_count") = fieldStat("not_null_count").asInstanceOf[Long] +
            (fileStats.recordCount().toLong - nullCount - nanCount)
      }
    }
    fieldStats.map {
      case (field, stats) =>
        field -> stats.toMap
    }.toMap
  }
}
