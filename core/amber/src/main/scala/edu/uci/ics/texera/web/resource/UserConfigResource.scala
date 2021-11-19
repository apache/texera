package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER_CONFIG
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserConfigDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, UserConfig}
import io.dropwizard.auth.Auth

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core._
import scala.jdk.CollectionConverters.asScalaBuffer

/**
  * This class handles requests to read and write the user dictionary,
  * an abstract collection of (key, value) string pairs that is unique for each user
  * This is accomplished using a mysql table called user_dictionary.
  * The details of user_dictionary can be found in /core/scripts/sql/texera_ddl.sql
  */
@PermitAll
@Path("/user/config")
@Consumes(Array(MediaType.TEXT_PLAIN))
class UserConfigResource {
  final private val userDictionaryDao = new UserConfigDao(
    SqlServer.createDSLContext.configuration
  )

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAllDict(@Auth sessionUser: SessionUser): Map[String, String] = {
    val user = sessionUser.getUser
    getDict(user)
  }

  /**
    * This method retrieves all of a user's dictionary entries in
    * the user_dictionary table as a json object
    */
  private def getDict(user: User): Map[String, String] = {
    Map(
      asScalaBuffer(
        SqlServer.createDSLContext
          .select()
          .from(USER_CONFIG)
          .where(USER_CONFIG.UID.eq(user.getUid))
          .fetchInto(classOf[UserConfig])
      ) map { entry => (entry.getKey, entry.getValue) }: _*
    )
  }

  @GET
  @Produces(Array(MediaType.TEXT_PLAIN))
  @Path("/{key}")
  def getEntry(@PathParam("key") key: String, @Auth sessionUser: SessionUser): String = {
    val user = sessionUser.getUser

    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (!dictEntryExists(user, key)) {
      null
    } else {
      getValueByKey(user, key)
    }
  }

  /**
    * This method retrieves a value from the user_dictionary table
    * given a user's uid and key. each tuple (uid, key) is a primary key
    * in user_dictionary, and should uniquely identify one value
    * @return String or null if entry doesn't exist
    */
  private def getValueByKey(user: User, key: String): String = {
    SqlServer.createDSLContext
      .fetchOne(
        USER_CONFIG,
        USER_CONFIG.UID.eq(user.getUid).and(USER_CONFIG.KEY.eq(key))
      )
      .getValue
  }

  /**
    * This method creates or updates an entry in the current in-session user's dictionary based on
    * the "key" and "value" attributes of the PostRequest
    */
  @PUT
  @Path("/{key}")
  def setEntry(
      @PathParam("key") key: String,
      value: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val user = sessionUser.getUser
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (dictEntryExists(user, key)) {
      userDictionaryDao.update(new UserConfig(user.getUid, key, value))
    } else {
      userDictionaryDao.insert(new UserConfig(user.getUid, key, value))
    }
  }

  /**
    * This method checks if a given entry exists
    * each tuple (uid, key) is a primary key in user_dictionary,
    * and should uniquely identify one value
    */
  private def dictEntryExists(user: User, key: String): Boolean = {
    userDictionaryDao.existsById(
      SqlServer.createDSLContext
        .newRecord(USER_CONFIG.UID, USER_CONFIG.KEY)
        .values(user.getUid, key)
    )
  }

  /**
    * This method deletes a key-value pair from the current in-session user's dictionary based on
    * the "key" attribute of the DeleteRequest
    *
    * @return
    * 401 unauthorized -
    * 400 bad request -
    * 422 Unprocessable Entity - payload: "no such entry" (if no entry exists for provided key)
    */
  @DELETE
  @Path("/{key}")
  def deleteEntry(@PathParam("key") key: String, @Auth sessionUser: SessionUser): Unit = {
    val user = sessionUser.getUser
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (dictEntryExists(user, key)) {
      deleteDictEntry(user, key)
    }
  }

  /**
    * This method deletes a single entry
    * each tuple (uid, key) is a primary key in user_dictionary,
    * and should uniquely identify one value
    */
  private def deleteDictEntry(user: User, key: String): Unit = {
    userDictionaryDao.deleteById(
      SqlServer.createDSLContext
        .newRecord(USER_CONFIG.UID, USER_CONFIG.KEY)
        .values(user.getUid, key)
    )
  }
}
