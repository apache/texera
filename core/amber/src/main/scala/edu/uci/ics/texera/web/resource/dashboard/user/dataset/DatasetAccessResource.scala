package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.engine.common.Utils.withTransaction
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.dao.jooq.generated.Tables.USER
import edu.uci.ics.texera.dao.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetUserAccessDao, UserDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{DatasetUserAccess, User}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetAccessResource.{
  context,
  getOwner
}
import org.jooq.{Condition, DSLContext}
import org.jooq.impl.DSL
import org.jooq.types.UInteger

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

object DatasetAccessResource {
  private lazy val context: DSLContext = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()

  def userHasReadAccess(ctx: DSLContext, did: UInteger, uid: UInteger): Boolean = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val isDatasetPublic = Option(datasetDao.fetchOneByDid(did))
      .flatMap(dataset => Option(dataset.getIsPublic))
      .contains(1.toByte)

    isDatasetPublic ||
    userHasWriteAccess(ctx, did, uid) ||
    getDatasetUserAccessPrivilege(ctx, did, uid) == DatasetUserAccessPrivilege.READ
  }

  def userOwnDataset(ctx: DSLContext, did: UInteger, uid: UInteger): Boolean = {
    val datasetDao = new DatasetDao(ctx.configuration())

    Option(datasetDao.fetchOneByDid(did))
      .exists(_.getOwnerUid == uid)
  }

  def userHasWriteAccess(ctx: DSLContext, did: UInteger, uid: UInteger): Boolean = {
    userOwnDataset(ctx, did, uid) ||
    getDatasetUserAccessPrivilege(ctx, did, uid) == DatasetUserAccessPrivilege.WRITE
  }

  def getDatasetUserAccessPrivilege(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger
  ): DatasetUserAccessPrivilege = {
    Option(
      ctx
        .select(DATASET_USER_ACCESS.PRIVILEGE)
        .from(DATASET_USER_ACCESS)
        .where(
          DATASET_USER_ACCESS.DID
            .eq(did)
            .and(DATASET_USER_ACCESS.UID.eq(uid))
        )
        .fetchOneInto(classOf[DatasetUserAccessPrivilege])
    ).getOrElse(DatasetUserAccessPrivilege.NONE)
  }

  def getOwner(ctx: DSLContext, did: UInteger): User = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val userDao = new UserDao(ctx.configuration())

    Option(datasetDao.fetchOneByDid(did))
      .flatMap(dataset => Option(dataset.getOwnerUid))
      .map(ownerUid => userDao.fetchOneByUid(ownerUid))
      .orNull
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/dataset")
class DatasetAccessResource {

  /**
    * This method returns the owner of a dataset
    *
    * @param did ,  dataset id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{did}")
  def getOwnerEmailOfDataset(@PathParam("did") did: UInteger): String = {
    var email = "";
    withTransaction(context) { ctx =>
      val owner = getOwner(ctx, did)
      if (owner != null) {
        email = owner.getEmail
      }
    }
    email
  }

  /**
    * Returns information about all current shared access of the given dataset
    *
    * @param did dataset id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{did}")
  def getAccessList(
      @PathParam("did") did: UInteger
  ): util.List[AccessEntry] = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      ctx
        .select(
          USER.EMAIL,
          USER.NAME,
          DATASET_USER_ACCESS.PRIVILEGE
        )
        .from(DATASET_USER_ACCESS)
        .join(USER)
        .on(USER.UID.eq(DATASET_USER_ACCESS.UID))
        .where(
          DATASET_USER_ACCESS.DID
            .eq(did)
            .and(DATASET_USER_ACCESS.UID.notEqual(datasetDao.fetchOneByDid(did).getOwnerUid))
        )
        .fetchInto(classOf[AccessEntry])
    }
  }

  /**
    * This method shares a dataset to a user with a specific access type
    *
    * @param did       the given dataset
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{did}/{email}/{privilege}")
  def grantAccess(
      @PathParam("did") did: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())
      val userDao = new UserDao(ctx.configuration())
      datasetUserAccessDao.merge(
        new DatasetUserAccess(
          did,
          userDao.fetchOneByEmail(email).getUid,
          DatasetUserAccessPrivilege.valueOf(privilege)
        )
      )
      Response.ok().build()
    }
  }

  /**
    * This method revoke the user's access of the given dataset
    *
    * @param did   the given dataset
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{did}/{email}")
  def revokeAccess(
      @PathParam("did") did: UInteger,
      @PathParam("email") email: String
  ): Response = {
    withTransaction(context) { ctx =>
      val userDao = new UserDao(ctx.configuration())

      ctx
        .delete(DATASET_USER_ACCESS)
        .where(
          DATASET_USER_ACCESS.UID
            .eq(userDao.fetchOneByEmail(email).getUid)
            .and(DATASET_USER_ACCESS.DID.eq(did))
        )
        .execute()

      Response.ok().build()
    }
  }
}
