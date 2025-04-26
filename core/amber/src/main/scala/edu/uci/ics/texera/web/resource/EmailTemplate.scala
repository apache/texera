package edu.uci.ics.texera.web.resource

object EmailTemplate {
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
