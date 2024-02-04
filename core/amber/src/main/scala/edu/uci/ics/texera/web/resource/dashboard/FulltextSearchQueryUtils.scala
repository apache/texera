package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.{
  SearchFieldMapping,
  SearchQueryParams
}
import org.jooq.{Condition, Field, OrderField, Record1, Result}
import org.jooq.impl.DSL.{condition, noCondition}
import org.jooq.types.UInteger

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.concurrent.TimeUnit
import scala.collection.Seq
import scala.jdk.CollectionConverters.CollectionHasAsScala

object FulltextSearchQueryUtils {
  def executeSearch(
      searchQueryParams: SearchQueryParams,
      fieldMapping: SearchFieldMapping
  ): Result[Record1[UInteger]] = {
    val splitKeywords = searchQueryParams.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq
    val whereCondition = noCondition()
      // Apply creation_time date filter
      .and(
        getDateFilter(
          searchQueryParams.creationStartDate,
          searchQueryParams.creationEndDate,
          fieldMapping.fieldsForCreationDate
        )
      )
      // Apply lastModified_time date filter
      .and(
        getDateFilter(
          searchQueryParams.modifiedStartDate,
          searchQueryParams.modifiedEndDate,
          fieldMapping.fieldsForModifiedDate
        )
      )
      // Apply workflowID filter
      .and(getContainsFilter(searchQueryParams.workflowIDs, fieldMapping.fieldsForWorkflowIds))
      // Apply owner filter
      .and(getContainsFilter(searchQueryParams.owners, fieldMapping.fieldsForOwner))
      // Apply operators filter
      .and(getOperatorsFilter(searchQueryParams.operators, fieldMapping.fieldsForOperators))
      // Apply projectId filter
      .and(getContainsFilter(searchQueryParams.projectIds, fieldMapping.fieldsForProjectIds))
      // Apply fulltext search filter
      .and(getFulltextSearchConditions(splitKeywords, fieldMapping.fieldsForKeywords))

    fieldMapping.baseQuery
      .and(whereCondition)
      .orderBy(getOrderFields(fieldMapping.specificResourceType, searchQueryParams): _*)
      .fetch()
  }

  private def getOrderFields(
      specificResourceType: String,
      searchQueryParams: SearchQueryParams
  ): List[OrderField[_]] = {
    // Regex pattern to extract column name and order direction
    val pattern = "(Name|CreateTime|EditTime)(Asc|Desc)".r

    searchQueryParams.orderBy match {
      case pattern(column, order) =>
        val field = getColumnField(specificResourceType, column)
        field match {
          case Some(value) =>
            List(order match {
              case "Asc"  => value.asc()
              case "Desc" => value.desc()
            })
          case None => List()
        }
      case _ => List() // Default case if the orderBy string doesn't match the pattern
    }
  }

  // Helper method to map column names to actual database fields based on resource type
  private def getColumnField(resourceType: String, columnName: String): Option[Field[_]] = {
    Option((resourceType, columnName) match {
      case ("workflow", "Name")       => WORKFLOW.NAME
      case ("workflow", "CreateTime") => WORKFLOW.CREATION_TIME
      case ("workflow", "EditTime")   => WORKFLOW.LAST_MODIFIED_TIME
      case ("project", "Name")        => PROJECT.NAME
      case ("project", "CreateTime")  => PROJECT.CREATION_TIME
      // Assuming PROJECT does not have a separate LAST_MODIFIED_TIME and uses CREATION_TIME for edits
      case ("project", "EditTime") => PROJECT.CREATION_TIME
      case ("file", "Name")        => FILE.NAME
      case ("file", "CreateTime")  => FILE.UPLOAD_TIME
      // Assuming FILE uses UPLOAD_TIME for edit time as well
      case ("file", "EditTime") => FILE.UPLOAD_TIME
      case _                    => null // Default case for unmatched resource types or column names
    })
  }

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
        val subStringSearchEnabled = if (words.length == 1) "*" else ""
        var conditionForKeyword = condition(
          s"MATCH($indexedCompoundFields) AGAINST('+$key$subStringSearchEnabled' IN BOOLEAN MODE)"
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

  def getContainsFilter[T](values: java.util.List[T], fields: List[Field[T]]): Condition = {
    if (values.asScala.nonEmpty && fields.isEmpty) {
      return noCondition()
    }
    var filter: Condition = noCondition()
    val valueSet = values.asScala.toSet
    for (field <- fields) {
      var filterForOneField: Condition = noCondition()
      for (value <- valueSet) {
        filterForOneField = filterForOneField.or(field.eq(value))
      }
      // make sure every field contains the value
      filter = filter.and(filterForOneField)
    }
    filter
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
      fieldToFilterOn: List[Field[Timestamp]]
  ): Condition = {
    if (fieldToFilterOn.isEmpty) {
      return noCondition()
    }
    var dateFilter: Condition = noCondition()
    for (field <- fieldToFilterOn) {
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
        dateFilter = dateFilter.and(field.between(startTimestamp, endTimestamp))
      }
    }
    dateFilter
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
      fields: List[Field[String]]
  ): Condition = {
    if (operators.asScala.nonEmpty && fields.isEmpty) {
      return noCondition()
    }
    var operatorsFilter: Condition = noCondition()
    val operatorSet = operators.asScala.toSet
    for (field <- fields) {
      var fieldFilter = noCondition()
      for (operator <- operatorSet) {
        val quotes = "\""
        val searchKey =
          "%" + quotes + "operatorType" + quotes + ":" + quotes + operator + quotes + "%"
        fieldFilter = fieldFilter.or(field.likeIgnoreCase(searchKey))
      }
      operatorsFilter = operatorsFilter.and(fieldFilter)
    }
    operatorsFilter
  }

}
