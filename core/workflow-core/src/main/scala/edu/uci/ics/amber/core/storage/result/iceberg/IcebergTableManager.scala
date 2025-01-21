package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.Table
import scala.collection.mutable
import org.apache.iceberg.types.Types
import java.time.ZoneId
import java.util.Date
import edu.uci.ics.amber.core.storage.IcebergCatalogInstance
import scala.jdk.CollectionConverters._
import org.apache.iceberg.data.Record

/**
 * IcebergTableManager provides utility methods for calculating statistics
 * (e.g., numeric, date, categorical) for an Iceberg table.
 *
 * @param table The Iceberg table managed by this class.
 */
class IcebergTableManager(table: Table) {

  private val schema = table.schema()

  /**
   * Detects numeric fields in the table schema.
   * Numeric fields include Integer, Long, and Double types.
   *
   * @return A set of field names that are numeric.
   */
  private def detectNumericFields(): Set[String] = {
    schema.columns().asScala.collect {
      case col if col.`type`() == Types.IntegerType.get() ||
        col.`type`() == Types.LongType.get() ||
        col.`type`() == Types.DoubleType.get() =>
        col.name()
    }.toSet
  }

  /**
   * Detects date fields in the table schema.
   * Date fields include TimestampType with or without timezone.
   *
   * @return A set of field names that are date fields.
   */
  private def detectDateFields(): Set[String] = {
    schema.columns().asScala.collect {
      case col if col.`type`() == Types.TimestampType.withoutZone() ||
        col.`type`() == Types.TimestampType.withZone() =>
        col.name()
    }.toSet
  }

  /**
   * Detects categorical fields in the table schema.
   * Categorical fields include String and Boolean types.
   *
   * @return A set of field names that are categorical.
   */
  private def detectCategoricalFields(): Set[String] = {
    schema.columns().asScala.collect {
      case col if col.`type`() == Types.StringType.get() || col.`type`() == Types.BooleanType.get() =>
        col.name()
    }.toSet
  }

  /**
   * Calculates statistics for numeric fields in the table.
   * The statistics include minimum, maximum, and mean values for each field.
   *
   * @return A map where each key is a numeric field name, and the value is a map of statistics.
   */
  def calculateNumericStats(): Map[String, Map[String, Double]] = {
    val numericFields = detectNumericFields()

    val stats = mutable.Map[String, mutable.Map[String, Double]]()

    numericFields.foreach { field =>
      stats(field) = mutable.Map("min" -> Double.MaxValue, "max" -> Double.MinValue, "sum" -> 0.0, "count" -> 0.0)
    }

    // Iterate through table data and calculate stats for numeric fields
    sampleDocuments().foreach { record =>
        numericFields.foreach { field =>
          val value = Option(record.getField(field)).map(_.toString.toDouble).getOrElse(Double.NaN)
          if (!value.isNaN) {
            val fieldStats = stats(field)
            fieldStats("min") = Math.min(fieldStats("min"), value)
            fieldStats("max") = Math.max(fieldStats("max"), value)
            fieldStats("sum") += value
            fieldStats("count") += 1
          }
      }
    }

    // Convert mutable stats to immutable map
    stats.map { case (field, fieldStats) =>
      field -> Map(
        "min" -> fieldStats("min"),
        "max" -> fieldStats("max"),
        "mean" -> (if (fieldStats("count") > 0) fieldStats("sum") / fieldStats("count") else Double.NaN)
      )
    }.toMap
  }

  /**
   * Calculates statistics for date fields in the table.
   * The statistics include minimum and maximum dates for each field.
   *
   * @return A map where each key is a date field name, and the value is a map of statistics.
   */
  def calculateDateStats(): Map[String, Map[String, Date]] = {
    val dateFields = detectDateFields()

    val stats = mutable.Map[String, mutable.Map[String, Date]]()

    dateFields.foreach { field =>
      stats(field) = mutable.Map("min" -> new Date(Long.MaxValue), "max" -> new Date(Long.MinValue))
    }

    sampleDocuments().foreach { record =>
        dateFields.foreach { field =>
          val value = Option(record.getField(field)).map {
            case ldt: java.time.LocalDateTime =>
              Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant)
            case other =>
              throw new IllegalArgumentException(s"Unexpected type for field '$field': $other")
          }
          if (value.isDefined) {
            val fieldStats = stats(field)
            fieldStats("min") = if (value.get.before(fieldStats("min"))) value.get else fieldStats("min")
            fieldStats("max") = if (value.get.after(fieldStats("max"))) value.get else fieldStats("max")
          }
        }
      }

    stats.map { case (field, fieldStats) =>
      field -> Map(
        "min" -> fieldStats("min"),
        "max" -> fieldStats("max")
      )
    }.toMap
  }

  /**
   * Calculates statistics for categorical fields in the table.
   * The statistics include counts of unique values for each field.
   *
   * @return A map where each key is a categorical field name, and the value is a map of counts.
   */
  def calculateCategoricalStats(): Map[String, Map[String, Map[String, Integer]]] = {
    val categoricalFields = detectCategoricalFields()

    val stats = mutable.Map[String, mutable.Map[String, Int]]()

    categoricalFields.foreach { field =>
      stats(field) = mutable.Map[String, Int]()
    }

    sampleDocuments().foreach { record =>
        categoricalFields.foreach { field =>
          val value = Option(record.getField(field)).map(_.toString).getOrElse("null")
          stats(field).updateWith(value) {
            case Some(count) => Some(count + 1)
            case None => Some(1)
          }
      }
    }

    stats.map { case (field, fieldStats) =>
      field -> Map(
        "counts" -> fieldStats.toMap.map { case (key, value) => key -> Integer.valueOf(value) }
      )
    }.toMap
  }

  private def sampleDocuments(): Seq[Record] = {
    table.newScan()
      .planFiles()
      .iterator()
      .asScala
      .flatMap { file =>
        IcebergUtil.readDataFileAsIterator(file.file(), table.schema(), table)
      }
      .take(10)
      .toSeq
  }
}
