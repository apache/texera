package edu.uci.ics.texera.web.resource.dashboard
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.File
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFulltextSearchConditions
}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import org.jooq.{Condition, GroupField, OrderField, Record, TableLike}
import org.jooq.impl.DSL

import scala.jdk.CollectionConverters.CollectionHasAsScala
class FileSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: ResourceFieldEntry = new ResourceFieldEntry(
    _resourceType = DSL.inline(SearchQueryBuilder.FILE_RESOURCE_TYPE),
    _name = FILE.NAME,
    _description = FILE.DESCRIPTION,
    _creationTime = FILE.UPLOAD_TIME,
    _fid = FILE.FID,
    _fileOwnerId = FILE.OWNER_UID,
    _fileUploadTime = FILE.UPLOAD_TIME,
    _filePath = FILE.PATH,
    _fileSize = FILE.SIZE,
    _userEmail = USER.EMAIL,
    _fileUserAccess = USER_FILE_ACCESS.PRIVILEGE
  )

  override protected def constructFromClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): TableLike[_] = {
    FILE
      .leftJoin(USER_FILE_ACCESS)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .leftJoin(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid))
  }

  override protected def constructWhereClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getFulltextSearchConditions(
      splitKeywords,
      List(mappedResourceSchema.name, mappedResourceSchema.description)
    )
      .and(
        getDateFilter(
          params.creationStartDate,
          params.creationEndDate,
          mappedResourceSchema.creationTime
        )
      )
      .and(getContainsFilter(params.owners, USER.EMAIL))
  }

  override protected def constructGroupByClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[GroupField] = Seq(FILE.FID)

  override protected def getOrderFields(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[OrderField[_]] =
    FulltextSearchQueryUtils.getOrderFields(SearchQueryBuilder.FILE_RESOURCE_TYPE, params)

  override def toEntry(
      user: SessionUser,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val df = DashboardFile(
      record.into(USER).getEmail,
      record
        .get(
          mappedResourceSchema.fileUserAccess,
          classOf[UserFileAccessPrivilege]
        )
        .toString,
      record.into(FILE).into(classOf[File])
    )
    DashboardClickableFileEntry(SearchQueryBuilder.FILE_RESOURCE_TYPE, file = Some(df))
  }
}
