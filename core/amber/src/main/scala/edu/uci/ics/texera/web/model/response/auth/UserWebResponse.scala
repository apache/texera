package edu.uci.ics.texera.web.model.response.auth

import edu.uci.ics.texera.web.model.common.auth.User

/**
  * Corresponds to `src/app/common/type/user.ts`
  */
object UserWebResponse {
  def generateErrorResponse(message: String) =
    new UserWebResponse(1, User.generateErrorAccount, message)

  def generateSuccessResponse(user: User) =
    new UserWebResponse(0, user, null)
}

class UserWebResponse private(
                                 var code: Int, // 0 represents success and 1 represents error
                                 var user: User,
                                 var message: String
                             ) {}
