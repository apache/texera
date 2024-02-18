package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.jooq.{Condition, Field, OrderField}
import org.jooq.impl.DSL.{condition, noCondition}

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.CollectionHasAsScala

object FulltextSearchQueryUtils {

  def getFulltextSearchConditions(
      keywords: Seq[String],
      fields: List[Field[String]]
  ): Condition = {
    if (keywords.nonEmpty && fields.isEmpty) {
      return noCondition()
    }
    var result = noCondition()
    val fullFieldNames = fields.map(_.toString.replace("\"", ""))
    val indexedCompoundFields = fullFieldNames.mkString(",")
    for (key: String <- keywords) {
      if (key != "") {
        val words = key.split("\\s+")
        var conditionForKeyword = condition(
          s"MATCH($indexedCompoundFields) AGAINST('${words.mkString("+", " +", "")}' IN BOOLEAN MODE)",
          key
        )
        // perform exact "contains" to improve the quality of search results
        // if the following has a huge performance overhead, consider removing the LIKE part.
        for (fieldName <- fullFieldNames) {
          conditionForKeyword = conditionForKeyword.or(s"$fieldName LIKE '%$key%'")
        }
        result = result.and(conditionForKeyword)
      }
    }
    result
  }

  def getContainsFilter[T](values: java.util.List[T], field: Field[T]): Condition = {
    val valueSet = values.asScala.toSet
    var filterForOneField: Condition = noCondition()
    for (value <- valueSet) {
      filterForOneField = filterForOneField.or(field.eq(value))
    }
    // make sure every field contains the value
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
    val operatorSet = operators.asScala.toSet
    var fieldFilter = noCondition()
    for (operator <- operatorSet) {
      val quotes = "\""
      val searchKey =
        "%" + quotes + "operatorType" + quotes + ":" + quotes + operator + quotes + "%"
      fieldFilter = fieldFilter.or(field.likeIgnoreCase(searchKey))
    }
    fieldFilter
  }

}
