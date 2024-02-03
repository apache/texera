package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import org.jooq.{Condition, Field}
import org.jooq.impl.DSL

import java.sql.Timestamp
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

object FulltextSearchQueryBuilder {
  def buildQueries(user: SessionUser, searchParams: SearchParams): (Condition, Condition, Condition, Condition) = {
    val keywords = searchParams.query.asScala.flatMap(_.split("[+\\-()<>~*@\"]+").toList).filter(_.nonEmpty)

    // Constructing base conditions for full-text search
    val baseWorkflowCondition = DSL.noCondition()
    val baseProjectCondition = DSL.noCondition()
    val baseFileCondition = DSL.noCondition()

    // Enhancing conditions based on keywords
    val workflowMatchCondition = keywords.foldLeft(baseWorkflowCondition) { (condition, keyword) =>
      condition.and(
        DSL.condition("MATCH(workflow.name, workflow.description) AGAINST ({0} IN BOOLEAN MODE)", keyword)
      )
    }

    val projectMatchCondition = keywords.foldLeft(baseProjectCondition) { (condition, keyword) =>
      condition.and(
        DSL.condition("MATCH(project.name, project.description) AGAINST ({0} IN BOOLEAN MODE)", keyword)
      )
    }

    val fileMatchCondition = keywords.foldLeft(baseFileCondition) { (condition, keyword) =>
      condition.and(
        DSL.condition("MATCH(file.name, file.description) AGAINST ({0} IN BOOLEAN MODE)", keyword)
      )
    }

    // Applying additional filters
    val creationStartDate = Option(searchParams.createDateStart).filter(_.nonEmpty)
    val creationEndDate = Option(searchParams.createDateEnd).filter(_.nonEmpty)
    val modifiedStartDate = Option(searchParams.modifiedDateStart).filter(_.nonEmpty)
    val modifiedEndDate = Option(searchParams.modifiedEndDate).filter(_.nonEmpty)

    val workflowFilterCondition = workflowMatchCondition
      .and(getDateFilter(creationStartDate, creationEndDate, WORKFLOW.CREATION_TIME))
      .and(getDateFilter(modifiedStartDate, modifiedEndDate, WORKFLOW.LAST_MODIFIED_TIME))

    val projectFilterCondition = projectMatchCondition
      .and(getDateFilter(creationStartDate, creationEndDate, PROJECT.CREATION_TIME))
    // Projects might not have a modification date filter

    val fileFilterCondition = fileMatchCondition
      .and(getDateFilter(creationStartDate, creationEndDate, FILE.UPLOAD_TIME))
    // Files might not have a modification date filter

    // Assuming getOwnerFilter, getDateFilter, etc., are utility methods that construct JOOQ conditions
    (workflowFilterCondition, projectFilterCondition, fileFilterCondition, DSL.noCondition()) // Note: The last condition is a placeholder
  }

  // Utility method placeholders
  private def getDateFilter(startDate: Option[String], endDate: Option[String], dateField: Field[Timestamp]): Condition = {
    var condition = DSL.noCondition()
    startDate.foreach(start => condition = condition.and(dateField.greaterOrEqual(Timestamp.valueOf(start))))
    endDate.foreach(end => condition = condition.and(dateField.lessOrEqual(Timestamp.valueOf(end))))
    condition
  }

  private def getOwnerFilter(owners: List[String]): Condition = {
    owners match {
      case Nil => DSL.noCondition()
      case _ => USER.NAME.in(owners.toSeq:_*)
    }

  }
}




