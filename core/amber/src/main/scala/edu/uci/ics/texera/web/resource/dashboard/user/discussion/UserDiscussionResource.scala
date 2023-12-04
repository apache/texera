package edu.uci.ics.texera.web.resource.dashboard.user.discussion

import com.mysql.cj.jdbc.MysqlDataSource
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth
import org.jooq.SQLDialect
import org.jooq.impl.DSL.{field, name, table, using}

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.mindrot.jbcrypt.BCrypt

@Path("/discussion")
class UserDiscussionResource {

  @PUT
  @Path("/register")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def register(@Auth user: SessionUser) = {
    val dataSource = new MysqlDataSource
    dataSource.setUrl(AmberUtils.amberConfig.getString("jdbc.url").replace("texera_db", "flarum"))
    dataSource.setUser(AmberUtils.amberConfig.getString("jdbc.username"))
    dataSource.setPassword(AmberUtils.amberConfig.getString("jdbc.password"))
    val hashed = BCrypt.hashpw(user.getGoogleId, BCrypt.gensalt());

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
        hashed
      )
      .execute()
  }
}
