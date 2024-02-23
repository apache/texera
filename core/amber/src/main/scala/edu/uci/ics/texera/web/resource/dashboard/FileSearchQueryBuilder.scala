package edu.uci.ics.texera.web.resource.dashboard
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.File
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getFullTextSearchFilter,
  getSubstringSearchFilter,
  getContainsFilter,
  getDateFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import org.jooq.{Condition, GroupField, Record, TableLike}
import org.jooq.impl.DSL
import org.jooq.types.UInteger

import scala.jdk.CollectionConverters.CollectionHasAsScala
object FileSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.FILE_RESOURCE_TYPE),
    name = FILE.NAME,
    description = FILE.DESCRIPTION,
    creationTime = FILE.UPLOAD_TIME,
    fid = FILE.FID,
    ownerId = FILE.OWNER_UID,
    lastModifiedTime = FILE.UPLOAD_TIME,
    filePath = FILE.PATH,
    fileSize = FILE.SIZE,
    userEmail = USER.EMAIL,
    fileUserAccess = USER_FILE_ACCESS.PRIVILEGE
  )

  override protected def constructFromClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams
  ): TableLike[_] = {
    FILE
      .leftJoin(USER_FILE_ACCESS)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .leftJoin(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(uid))
  }

  override protected def constructWhereClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      FILE.UPLOAD_TIME
    )
      .and(getContainsFilter(params.owners, USER.EMAIL))
      .and(
        getFullTextSearchFilter(
          splitKeywords,
          List(FILE.NAME, FILE.DESCRIPTION)
        ).or(getSubstringSearchFilter(splitKeywords, List(FILE.NAME, FILE.DESCRIPTION)))
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = Seq.empty

  override def toEntryImpl(
      uid: UInteger,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val df = DashboardFile(
      record.into(USER).getEmail,
      record
        .get(
          USER_FILE_ACCESS.PRIVILEGE,
          classOf[UserFileAccessPrivilege]
        )
        .toString,
      record.into(FILE).into(classOf[File])
    )
    DashboardClickableFileEntry(SearchQueryBuilder.FILE_RESOURCE_TYPE, file = Some(df))
  }
}
