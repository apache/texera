/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.config.UserSystemConfig

/**
  * EmailTemplate provides factory methods to generate email messages
  * for different user notification scenarios.
  */
object EmailTemplate {

  private val deployment: String =
    UserSystemConfig.appDomain.map(_.replaceFirst("^https?://", "")).getOrElse("")

  /**
    * Creates an email message for user registration notifications.
    * Depending on the 'toAdmin' flag, it either notifies an administrator
    * of a pending account request or acknowledges receipt to the user.
    *
    * @param receiverEmail the email address of the receiver (admin or user)
    * @param userEmail optional; the email address of the user requesting an account (only needed if toAdmin is true)
    * @param toAdmin flag indicating whether the notification is for the admin (true) or the user (false)
    * @return an EmailMessage ready to be sent
    */
  def userRegistrationNotification(
      receiverEmail: String,
      userEmail: Option[String],
      toAdmin: Boolean
  ): EmailMessage = {
    if (toAdmin) {
      val subject =
        s"New Account Request Pending Approval${if (deployment.nonEmpty) s" for [$deployment]"
        else ""}"
      val content =
        s"""
           |Hello Admin,
           |
           |A new user has attempted to log in or register, but their account is not yet approved.
           |Please review the account request for the following email:
           |
           |${userEmail.getOrElse("Unknown")}
           |
           |Visit the admin panel at: $deployment
           |
           |Thanks!
           |""".stripMargin
      EmailMessage(subject = subject, content = content, receiver = receiverEmail)
    } else {
      val subject =
        s"Account Request Received${if (deployment.nonEmpty) s" for [$deployment]" else ""}"
      val content =
        s"""
           |Hello,
           |
           |Thank you for submitting your account request.
           |We have received your request and it is currently under review.
           |You will be notified once your account has been approved.
           |
           |Thank you for your interest in Texera!
           |""".stripMargin
      EmailMessage(subject = subject, content = content, receiver = receiverEmail)
    }
  }

  /**
    * Creates an email message to notify a user
    * that their role has been updated.
    *
    * @param receiverEmail the user's email address
    * @param newRole the new role assigned to the user
    * @return an EmailMessage ready to be sent to the user
    */
  def createRoleChangeTemplate(receiverEmail: String, newRole: UserRoleEnum): EmailMessage = {
    val subject =
      s"Your Role Has Been Updated${if (deployment.nonEmpty) s" for [$deployment]" else ""}"
    val content =
      s"""
         |Hello,
         |
         |Your user role has been updated to: $newRole.
         |
         |If you have any questions, please contact the administrator.
         |
         |Thank you for using Texera!
         |""".stripMargin

    EmailMessage(subject = subject, content = content, receiver = receiverEmail)
  }
}
