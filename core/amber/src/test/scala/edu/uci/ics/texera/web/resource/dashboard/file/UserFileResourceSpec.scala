package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.{MockTexeraDB, SqlServer}
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import org.glassfish.jersey.media.multipart.FormDataContentDisposition
import org.jooq.types.UInteger
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec

class UserFileResourceSpec extends AnyFlatSpec with BeforeAndAfterAll with MockTexeraDB {

  private val testUser: User = {
    val user = new User
    user.setUid(UInteger.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRole.ADMIN)
    user.setPassword("123")
    user
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    // add test user directly
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "user" should "be able to upload file" in {
    val fileResource = new UserFileResource()
    val source = "This is the content of the file"
    val in = org.apache.commons.io.IOUtils.toInputStream(source, "UTF-8")
    val fileDetail =
      new FormDataContentDisposition("form-data; name=\"file\"; filename=\"example.txt\"")
    val response = fileResource.uploadFile(
      in,
      fileDetail,
      UInteger.valueOf(source.length),
      "sample file for testing",
      new SessionUser(testUser)
    )
    assert(response.getStatusInfo.getStatusCode == 200)
  }

}
