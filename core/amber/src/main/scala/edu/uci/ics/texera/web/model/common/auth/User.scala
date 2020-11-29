package edu.uci.ics.texera.web.model.common.auth

import org.jooq.types.UInteger

object User {
  def generateErrorAccount = new User("", UInteger.valueOf(0))
}

class User(
              var userName: String,
              var userID: UInteger // the ID in MySQL database is unsigned int
          ) {
  def getUserName: String = userName

  def getUserID: UInteger = userID
}
