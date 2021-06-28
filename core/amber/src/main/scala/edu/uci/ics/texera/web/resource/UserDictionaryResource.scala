package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER_DICTIONARY
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDictionaryDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, UserDictionary}
import edu.uci.ics.texera.web.resource.auth.UserResource
import io.dropwizard.jersey.sessions.Session

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core._
import scala.jdk.CollectionConverters.asScalaBuffer

/**
  * This class handles requests to read and write the user dictionary,
  * an abstract collection of (key, value) string pairs that is unique for each user
  * This is accomplished using a mysql table called user_dictionary.
  * The details of user_dictionary can be found in /core/scripts/sql/texera_ddl.sql
  */
@Path("/users/dictionary")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserDictionaryResource {
  final private val userDictionaryDao = new UserDictionaryDao(
    SqlServer.createDSLContext.configuration
  )

  @GET
  def getAllDict(@Session session: HttpSession): Map[String, String] = {
    val user = UserResource.getUser(session)
    if (user.isEmpty) {
      throw new NotAuthorizedException("Request unauthorized: user not logged in")
    }
    getDict(user.get)
  }

  @GET
  @Path("/{key}")
  def getEntry(@Session session: HttpSession, @PathParam("key") key: String): String = {
    val user = UserResource.getUser(session)
    if (user.isEmpty) {
      throw new NotAuthorizedException("Request unauthorized: user not logged in")
    }
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (!dictEntryExists(user.get, key)) {
      null
    } else {
      getValueByKey(user.get, key)
    }
  }

  /**
    * This method creates or updates an entry in the current in-session user's dictionary based on
    * the "key" and "value" attributes of the PostRequest
    */
  @PUT
  @Path("/{key}")
  def setEntry(
      @Session session: HttpSession,
      @PathParam("key") key: String,
      @FormParam("value") value: String
  ): Unit = {
    val user = UserResource.getUser(session)
    if (user.isEmpty) {
      throw new NotAuthorizedException("Request unauthorized: user not logged in")
    }
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (dictEntryExists(user.get, key)) {
      userDictionaryDao.update(new UserDictionary(user.get.getUid, key, value))
    } else {
      userDictionaryDao.insert(new UserDictionary(user.get.getUid, key, value))
    }
  }

  /**
    * This method deletes a key-value pair from the current in-session user's dictionary based on
    * the "key" attribute of the DeleteRequest
    * @param session HttpSession
    * @param req DeleteRequest
    * @return
    * 401 unauthorized -
    * 400 bad request -
    * 422 Unprocessable Entity - payload: "no such entry" (if no entry exists for provided key)
    */
  @DELETE
  @Path("/{key}")
  def deleteEntry(@Session session: HttpSession, @PathParam("key") key: String): Unit = {
    val user = UserResource.getUser(session)
    if (user.isEmpty) {
      throw new NotAuthorizedException("Request unauthorized: user not logged in")
    }
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }
    if (dictEntryExists(user.get, key)) {
      deleteDictEntry(user.get, key)
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
        USER_DICTIONARY,
        USER_DICTIONARY.UID.eq(user.getUid).and(USER_DICTIONARY.KEY.eq(key))
      )
      .getValue
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
          .from(USER_DICTIONARY)
          .where(USER_DICTIONARY.UID.eq(user.getUid))
          .fetchInto(classOf[UserDictionary])
      ) map { entry => (entry.getKey, entry.getValue) }: _*
    )
  }

  /**
    * This method checks if a given entry exists
    * each tuple (uid, key) is a primary key in user_dictionary,
    * and should uniquely identify one value
    */
  private def dictEntryExists(user: User, key: String): Boolean = {
    userDictionaryDao.existsById(
      SqlServer.createDSLContext
        .newRecord(USER_DICTIONARY.UID, USER_DICTIONARY.KEY)
        .values(user.getUid, key)
    )
  }

  /**
    * This method deletes a single entry
    * each tuple (uid, key) is a primary key in user_dictionary,
    * and should uniquely identify one value
    */
  private def deleteDictEntry(user: User, key: String): Unit = {
    userDictionaryDao.deleteById(
      SqlServer.createDSLContext
        .newRecord(USER_DICTIONARY.UID, USER_DICTIONARY.KEY)
        .values(user.getUid, key)
    )
  }
}
