package edu.uci.ics.texera.web.resource

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER_DICTIONARY
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDictionaryDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserDictionary
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.UserResource
import io.dropwizard.jersey.sessions.Session

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

  /**
    * This method either retrieves all of the current in-session user's dictionary
    * as a json object or retrieves the value corresponding to the "key" attribute of the GetRequest.
    * @param session HttpSession
    * @param req GetRequest (key may be null for GetDictRequest)
    * @return Response with a UserDictionaryResponse payload, 200 ok - payload: EntryResponse or DictResponse,
    * 401 unauthorized - payload: ErrorResponse("invalid session"),
    * 400 bad request - payload: ErrorResponse("Unable to process JSON"),
    * 422 Unprocessable Entity - payload: ErrorResponse("no such entry") (if no entry exists for provided key)
    */
  @GET
  def getValue(@Session session: HttpSession, req: GetRequest): Response = {
    val user = UserResource.getUser(session).orNull
    if (user == null)
      Response.status(Response.Status.UNAUTHORIZED).entity(ErrorResponse("invalid session")).build()
    else if (req == null) {
      Response
        .status(Response.Status.BAD_REQUEST)
        .entity(ErrorResponse("Unable to process JSON"))
        .build()
    } else if (req.requestType == GetRequest.GET_ENTRY) {
      if (!dictEntryExists(user, req.key))
        Response.status(422).entity(ErrorResponse("no such entry")).build()
      else Response.ok(EntryResponse(getValueByKey(user, req.key))).build()
    } else {
      Response.ok(DictResponse(getDict(user))).build()
    }
  }

  /**
    * This method creates or updates an entry in the current in-session user's dictionary based on
    * the "key" and "value" attributes of the PostRequest
    * @param session HttpSession
    * @param req PostRequest
    * @return Response, 200 ok - payload: ConfirmationResponse("created" or "updated"),
    * 401 unauthorized - payload: ErrorResponse("invalid session"),
    * 400 bad request - payload: ErrorResponse("Unable to process JSON"),
    */
  @POST
  def setValue(
      @Session session: HttpSession,
      req: PostRequest
  ): Response = {
    val user = UserResource.getUser(session).orNull
    if (user == null)
      Response.status(Response.Status.UNAUTHORIZED).entity(ErrorResponse("invalid session")).build()
    else if (req == null) {
      Response
        .status(Response.Status.BAD_REQUEST)
        .entity(ErrorResponse("Unable to process JSON"))
        .build()
    } else if (dictEntryExists(user, req.key)) {
      userDictionaryDao.update(new UserDictionary(user.getUid, req.key, req.value))
      Response.ok(ConfirmationResponse("updated")).build()
    } else {
      userDictionaryDao.insert(new UserDictionary(user.getUid, req.key, req.value))
      Response.ok(ConfirmationResponse("created")).build()
    }
  }

  /**
    * This method deletes a key-value pair from the current in-session user's dictionary based on
    * the "key" attribute of the DeleteRequest
    * @param session HttpSession
    * @param req DeleteRequest
    * @return Response, 200 ok - payload: "deleted",
    * 401 unauthorized - payload: "invalid session",
    * 400 bad request - payload: ErrorResponse("Unable to process JSON"),
    * 422 Unprocessable Entity - payload: "no such entry" (if no entry exists for provided key)
    */
  @DELETE
  def deleteValue(@Session session: HttpSession, req: DeleteRequest): Response = {
    val user = UserResource.getUser(session).orNull
    if (user == null)
      Response.status(Response.Status.UNAUTHORIZED).entity(ErrorResponse("invalid session")).build()
    else if (req == null) {
      Response
        .status(Response.Status.BAD_REQUEST)
        .entity(ErrorResponse("Unable to process JSON"))
        .build()
    } else if (!dictEntryExists(user, req.key)) {
      Response.status(422).entity(ErrorResponse("no such entry")).build()
    } else {
      deleteDictEntry(user, req.key)
      Response.ok(ConfirmationResponse("deleted")).build()
    }
  }

  /**
    * This method retrieves a value from the user_dictionary table
    * given a user's uid and key. each tuple (uid, key) is a primary key
    * in user_dictionary, and should uniquely identify one value
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

/**
  * This class represents the json structure of all incoming requests to UserDictionaryResource
  */
abstract class UserDictionaryRequest {
  def key: String
}

case class GetRequest(
    key: String,
    requestType: Int // 0 - get entry, 1 - get dict
) extends UserDictionaryRequest

object GetRequest {
  val GET_ENTRY = 0
  val GET_DICT = 1
}

case class PostRequest(
    key: String,
    value: String
) extends UserDictionaryRequest

case class DeleteRequest(
    key: String
) extends UserDictionaryRequest

/**
  * This class represents the json structure of all outgoing responses from UserDictionaryResource
  */
abstract class UserDictionaryResponse {
  def code: Int
}

object UserDictionaryResponse {
  val CODE_ERROR: Int = -1
  val CODE_ENTRY: Int = 0
  val CODE_DICT: Int = 1
  val CODE_CONFIRM: Int = 2
}

case class ErrorResponse(
    message: String,
    code: Int = UserDictionaryResponse.CODE_ERROR
) extends UserDictionaryResponse

case class EntryResponse(
    result: String,
    code: Int = UserDictionaryResponse.CODE_ENTRY
) extends UserDictionaryResponse

case class DictResponse(
    result: Map[String, String],
    code: Int = UserDictionaryResponse.CODE_DICT
) extends UserDictionaryResponse

case class ConfirmationResponse(
    result: String,
    code: Int = UserDictionaryResponse.CODE_CONFIRM
) extends UserDictionaryResponse
