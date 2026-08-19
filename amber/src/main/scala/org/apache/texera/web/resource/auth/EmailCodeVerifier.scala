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

package org.apache.texera.web.resource.auth

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.auth.JwtAuth
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.web.resource.{EmailMessage, EmailTemplate, GmailResource}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.ws.rs.{NotAcceptableException, WebApplicationException}
import javax.ws.rs.core.Response

object EmailCodeVerifier {

  /** Proving an address before an account exists — scoped by the handle being registered. */
  val REGISTER = "register"

  /** Proving an address for an account that has none — scoped by its uid. */
  val ADD_EMAIL = "add-email"

  private[auth] val CODE_LENGTH = 6
  private[auth] val STEP_SECONDS = 300L
  private[auth] val MAX_ATTEMPTS = 5
  private[auth] val RESEND_INTERVAL_SECONDS = 60L

  /** Labels the derived key, so rotating the scheme is a matter of bumping the suffix. */
  private val KEY_LABEL = "texera-email-verification-v1"

  private val HMAC = "HmacSHA256"

  /** The production instance: wall clock, real mail, real config. */
  lazy val instance: EmailCodeVerifier = new EmailCodeVerifier()

  private def hmac(key: Array[Byte], message: String): Array[Byte] = {
    val mac = Mac.getInstance(HMAC)
    mac.init(new SecretKeySpec(key, HMAC))
    mac.doFinal(message.getBytes(StandardCharsets.UTF_8))
  }
}

/**
  * Issues and checks the codes that prove someone owns an email address, without storing any of
  * them.
  *
  * The code is derived rather than remembered: a truncated HMAC over the purpose, the scope, the
  * address and a coarse time step, keyed by a value derived from the JWT secret. Checking recomputes
  * it for the current and previous step, so a code lives 5-10 minutes and nothing has to be written
  * down — no table, and a restart loses nothing a table would have kept.
  */
class EmailCodeVerifier(
    secret: String = JwtAuth.TOKEN_SECRET,
    clock: () => Instant = () => Instant.now(),
    send: (EmailMessage, String) => Either[String, Unit] = GmailResource.sendEmail,
    smtpConfigured: () => Boolean = () => UserSystemConfig.gmail.trim.nonEmpty
) extends LazyLogging {

  import EmailCodeVerifier._

  private case class Tracker(lastIssuedAt: Instant, attempts: Int, touchedAt: Instant)

  private val trackers = new ConcurrentHashMap[String, Tracker]()

  private val key: Array[Byte] = hmac(secret.getBytes(StandardCharsets.UTF_8), KEY_LABEL)

  /** Derive a code and mail it, or log it when there is no configured sender. */
  def issue(purpose: String, scope: String, email: String): Unit = {
    evictStale()

    val address = normalize(email)
    var throttled = false
    trackers.compute(
      keyOf(purpose, scope),
      (_, existing) => {
        val at = clock()
        if (
          existing != null &&
          existing.lastIssuedAt.plusSeconds(RESEND_INTERVAL_SECONDS).isAfter(at)
        ) {
          throttled = true
          existing.copy(touchedAt = at)
        } else {
          Tracker(lastIssuedAt = at, attempts = 0, touchedAt = at)
        }
      }
    )

    if (throttled) {
      throw tooManyRequests(
        s"A code was just sent. Wait $RESEND_INTERVAL_SECONDS seconds before asking for another."
      )
    }

    deliver(address, codeFor(purpose, scope, address, stepOf(clock())))
  }

  /**
    * Check a code, counting the attempt. Throws rather than returning a verdict so that every
    * refusal reads the same to the caller: distinguishing "wrong" from "expired" would tell someone
    * guessing which of the two they had achieved.
    */
  def check(purpose: String, scope: String, email: String, code: String): Unit = {
    val supplied = Option(code).map(_.trim).getOrElse("")
    // Shape is checked before the budget so that a client bug (an empty field, a stray space) cannot
    // burn the attempts a real code would need.
    if (!supplied.matches(s"\\d{$CODE_LENGTH}")) throw invalidCode()

    val address = normalize(email)
    var capped = false
    trackers.compute(
      keyOf(purpose, scope),
      (_, existing) => {
        val at = clock()
        val current = Option(existing).getOrElse(Tracker(Instant.EPOCH, 0, at))
        if (current.attempts >= MAX_ATTEMPTS) {
          capped = true
          current.copy(touchedAt = at)
        } else {
          current.copy(attempts = current.attempts + 1, touchedAt = at)
        }
      }
    )

    if (capped) {
      throw tooManyRequests("Too many attempts. Ask for a new code and try again.")
    }

    val step = stepOf(clock())
    val accepted =
      matches(supplied, purpose, scope, address, step) ||
        matches(supplied, purpose, scope, address, step - 1)

    if (!accepted) throw invalidCode()
  }

  /**
    * Drop trackers nothing has touched for two steps. Bounds the map in a long-lived process; safe
    * to call concurrently with [[issue]] and [[check]].
    */
  private[auth] def evictStale(): Unit = {
    val cutoff = clock().minusSeconds(STEP_SECONDS * 2)
    trackers.entrySet().removeIf(entry => entry.getValue.touchedAt.isBefore(cutoff))
  }

  /** Visible for tests. */
  private[auth] def trackedSize: Int = trackers.size()

  private def deliver(address: String, code: String): Unit = {
    val message = EmailTemplate.emailVerificationCode(address, code)

    if (!smtpConfigured()) {
      // Deployments set user-sys.google.smtp.gmail; a developer who turns verification on locally
      // has not, and failing the request would make the flow impossible to exercise. This cannot
      // fire where mail is configured.
      logger.warn(
        s"SMTP is unconfigured (user-sys.google.smtp.gmail is empty), so the verification code for " +
          s"$address was not mailed. Code: $code"
      )
      return
    }

    send(message, address) match {
      case Right(_) => ()
      case Left(reason) =>
        logger.warn(s"Could not mail a verification code to $address: $reason")
        throw new WebApplicationException(
          "The verification code could not be sent. Try again in a moment.",
          Response.Status.SERVICE_UNAVAILABLE
        )
    }
  }

  /**
    * Truncate the HMAC to [[CODE_LENGTH]] digits the way RFC 4226 does: pick the offset from the
    * last nibble, read four bytes there, and clear the sign bit so the result does not depend on
    * the platform's integer representation.
    */
  private def codeFor(purpose: String, scope: String, address: String, step: Long): String = {
    val digest = hmac(key, s"$purpose|$scope|$address|$step")
    val offset = digest(digest.length - 1) & 0x0f
    val binary =
      ((digest(offset) & 0x7f) << 24) |
        ((digest(offset + 1) & 0xff) << 16) |
        ((digest(offset + 2) & 0xff) << 8) |
        (digest(offset + 3) & 0xff)
    val modulus = math.pow(10, CODE_LENGTH.toDouble).toInt
    String.format(s"%0${CODE_LENGTH}d", Integer.valueOf(binary % modulus))
  }

  private def matches(
      supplied: String,
      purpose: String,
      scope: String,
      address: String,
      step: Long
  ): Boolean =
    MessageDigest.isEqual(
      supplied.getBytes(StandardCharsets.UTF_8),
      codeFor(purpose, scope, address, step).getBytes(StandardCharsets.UTF_8)
    )

  /** Addresses are matched case-insensitively, the same way `fetchUserByEmailIgnoreCase` does. */
  private def normalize(email: String): String =
    Option(email).getOrElse("").trim.toLowerCase

  private def stepOf(at: Instant): Long = at.getEpochSecond / STEP_SECONDS

  private def keyOf(purpose: String, scope: String): String = s"$purpose|$scope"

  private def invalidCode(): WebApplicationException =
    new NotAcceptableException("That code is not valid or has expired.")

  private def tooManyRequests(message: String): WebApplicationException =
    new WebApplicationException(message, Response.Status.TOO_MANY_REQUESTS)
}
