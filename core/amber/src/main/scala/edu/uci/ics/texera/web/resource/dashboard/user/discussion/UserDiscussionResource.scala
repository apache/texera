package edu.uci.ics.texera.web.resource.dashboard.user.discussion

import com.mysql.cj.jdbc.MysqlDataSource
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth
import org.jooq.SQLDialect
import org.jooq.impl.DSL.{field, name, table, using}
import org.mindrot.jbcrypt.BCrypt.{gensalt, hashpw}

import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Path("/discussion")
class UserDiscussionResource {

  @PUT
  @Path("/register")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def register(@Auth user: SessionUser): Int = {
    val dataSource = new MysqlDataSource
    dataSource.setUrl(StorageConfig.jdbcUrl.replace("texera_db", "flarum"))
    dataSource.setUser(StorageConfig.jdbcUsername)
    dataSource.setPassword(StorageConfig.jdbcPassword)
    using(dataSource, SQLDialect.MYSQL)
      .insertInto(table(name("users")))
      .columns(
        field(name("username")),
        field(name("email")),
        field(name("is_email_confirmed")),
        field(name("password"))
      )
      .values(
        user.getEmail,
        user.getEmail,
        "1",
        hashpw(user.getGoogleId, gensalt())
      )
      .execute()
  }
}
