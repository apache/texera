package edu.uci.ics.texera.web.resource.dashboard

import org.jooq.impl.DSL.{condition, noCondition}
import org.jooq.{Condition, Field}

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.CollectionHasAsScala

object FulltextSearchQueryUtils {

  def getFullTextSearchFilter(
      keywords: Seq[String],
      fields: List[Field[String]]
  ): Condition = {
    if (fields.isEmpty) return noCondition()

    // Filter out empty keywords and trim
    val trimmedKeywords = keywords.filter(_.nonEmpty).map(_.trim)

    // Build a SQL expression that concatenates all fields with spaces,
    // then feeds them into to_tsvector('english', ...).
    // E.g.: to_tsvector('english', COALESCE(firstName, '') || ' ' || COALESCE(lastName, ''))
    val combinedFields = fields
      .map(f => s"COALESCE($f, '')") // handle null -> ''
      .mkString(" || ' ' || ")

    // Fold each keyword into the final Condition
    trimmedKeywords.foldLeft(noCondition()) { (acc, keyword) =>
      // For each "keyword", split it into words
      val words = keyword.split("\\s+").filter(_.nonEmpty)

      // In Postgres tsquery syntax, an AND search uses '&' between terms
      // e.g.: apple & banana
      val tsQuery = words.mkString(" & ")

      // Build the raw SQL string for Postgres FTS
      // e.g.: to_tsvector('english', COALESCE(firstName, '') || ' ' || COALESCE(lastName, '')) @@ to_tsquery('english', 'apple & banana')
      val conditionExpr =
        s"to_tsvector('english', $combinedFields) @@ to_tsquery('english', '$tsQuery')"

      // 'condition(...)' is presumably your helper method that takes a raw SQL string
      // and an optional binding for debug/logging
      acc.and(condition(conditionExpr, keyword))
    }
  }

  /**
    * Generates a filter condition for querying based on whether a specified field contains any of the given values.
    *
    * This method converts a Java list of values into a Scala set to ensure uniqueness, and then iterates over each unique value,
    * constructing a filter condition that checks if the specified field equals any of those values. The resulting condition
    * is a disjunction (`OR`) of all these equality conditions, which can be used in database queries to find records where
    * the field matches any of the provided values.
    *
    * @tparam T The type of the elements in the `values` list and the type of the field being compared.
    * @param values A Java list of values to be checked against the field. The list is converted to a Scala set to remove duplicates.
    * @param field  The field to be checked for containing any of the values in the `values` list. This is typically a field in a database table.
    * @return A `Condition` that represents the disjunction of equality checks between the field and each unique value in the input list.
    *         This condition can be used as part of a query to select records where the field matches any of the specified values.
    */
  def getContainsFilter[T](values: java.util.List[T], field: Field[T]): Condition = {
    val valueSet = values.asScala.toSet
    var filterForOneField: Condition = noCondition()
    for (value <- valueSet) {
      filterForOneField = filterForOneField.or(field.eq(value))
    }
    filterForOneField
  }

  /**
    * Returns a date filter condition for the specified date range and date type.
    *
    * @param startDate       A string representing the start date of the filter range in "yyyy-MM-dd" format.
    *                        If empty, the default value "1970-01-01" will be used.
    * @param endDate         A string representing the end date of the filter range in "yyyy-MM-dd" format.
    *                        If empty, the default value "9999-12-31" will be used.
    * @param fieldToFilterOn the field for applying the start and end dates.
    * @return A Condition object that can be used to filter workflows based on the date range and type.
    */
  @throws[ParseException]
  def getDateFilter(
      startDate: String,
      endDate: String,
      fieldToFilterOn: Field[Timestamp]
  ): Condition = {
    if (startDate.nonEmpty || endDate.nonEmpty) {
      val start = if (startDate.nonEmpty) startDate else "1970-01-01"
      val end = if (endDate.nonEmpty) endDate else "9999-12-31"
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val startTimestamp = new Timestamp(dateFormat.parse(start).getTime)
      val endTimestamp =
        if (end == "9999-12-31") {
          new Timestamp(dateFormat.parse(end).getTime)
        } else {
          new Timestamp(
            dateFormat.parse(end).getTime + TimeUnit.DAYS.toMillis(1) - 1
          )
        }
      fieldToFilterOn.between(startTimestamp, endTimestamp)
    } else {
      noCondition()
    }
  }

  /**
    * Helper function to retrieve the operators filter.
    * Applies a filter based on the specified operators.
    *
    * @param operators The list of operators to filter by.
    * @return The operators filter.
    */
  def getOperatorsFilter(
      operators: java.util.List[String],
      field: Field[String]
  ): Condition = {
    // Convert to a Set to avoid duplicates
    val operatorSet = operators.asScala.toSet
    // Start with a "no condition" (logical TRUE) so we can accumulate
    var fieldFilter = noCondition()

    // For each operator, build the substring pattern
    operatorSet.foreach { operator =>
      // e.g. => % "operatorType":"someOperator" %
      val searchKey = s"""%"operatorType":"$operator"%"""

      // Use jOOQ's likeIgnoreCase for case-insensitive matching
      val cond = field.likeIgnoreCase(searchKey)

      // Accumulate with OR
      fieldFilter = fieldFilter.or(cond)
    }

    fieldFilter
  }

}
