package edu.uci.ics.texera.web.resource

/**
  * EmailTemplate provides factory methods to generate email messages
  * for different user notification scenarios.
  */
object EmailTemplate {

  /**
    * Creates an email message to notify the administrator
    * that a new user account request is pending approval.
    *
    * @param receiverEmail the administrator's email address
    * @param userEmail the email address of the user requesting an account
    * @return an EmailMessage ready to be sent to the administrator
    */
  def createAdminNotification(receiverEmail: String, userEmail: String): EmailMessage = {
    val subject = "New Account Request Pending Approval"
    val content =
      s"""
         |Hello Admin,
         |
         |A new user has attempted to log in or register, but their account is not yet approved.
         |Please review the account request for the following email:
         |
         |$userEmail
         |
         |Thanks!
         |""".stripMargin
    EmailMessage(subject = subject, content = content, receiver = receiverEmail)
  }

  /**
    * Creates an email message to notify the user
    * that their account request has been received and is under review.
    *
    * @param receiverEmail the user's email address
    * @return an EmailMessage ready to be sent to the user
    */
  def createUserNotification(receiverEmail: String): EmailMessage = {
    val subject = "Account Request Received"
    val content =
      s"""
         |Hello,
         |
         |Thank you for submitting your account request.
         |We have received your request and it is currently under review.
         |Please be patient during this process. You will be notified once your account has been approved.
         |
         |Thank you for your interest!
         |""".stripMargin
    EmailMessage(subject = subject, content = content, receiver = receiverEmail)
  }
}
