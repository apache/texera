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

package org.apache.texera.service.resource

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.CuratedImageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.util.{ImagePrepullClient, ImageValidationClient}
import org.apache.texera.service.util.ImageValidationClient.ValidationState
import org.jooq.impl.DSL
import org.jooq.{DSLContext, Record}

import java.sql.Timestamp
import scala.jdk.CollectionConverters._

object CuratedImageResource extends LazyLogging {

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()

  // Plain DSL rather than generated DAOs: jOOQ sources are generated at build time against
  // a live database and are not in the repository, so this keeps a clean checkout building.
  private val CU_IMAGE = DSL.table(DSL.name("cu_image"))
  private val IID = DSL.field(DSL.name("iid"), classOf[Integer])
  private val NAME = DSL.field(DSL.name("name"), classOf[String])
  private val SOURCE_REF = DSL.field(DSL.name("source_ref"), classOf[String])
  private val SOURCE_DIGEST = DSL.field(DSL.name("source_digest"), classOf[String])
  private val STATUS = DSL.field(DSL.name("status"), classOf[String])
  private val ATTEMPT = DSL.field(DSL.name("attempt"), classOf[Integer])
  private val VALIDATION_LOG = DSL.field(DSL.name("validation_log"), classOf[String])
  private val CREATED_BY = DSL.field(DSL.name("created_by"), classOf[Integer])
  private val CREATION_TIME = DSL.field(DSL.name("creation_time"), classOf[Timestamp])
  private val UPDATE_TIME = DSL.field(DSL.name("update_time"), classOf[Timestamp])

  object Status {
    val Pending = "PENDING"
    val Validating = "VALIDATING"
    val Ready = "READY"
    val Failed = "FAILED"
  }

  case class CuratedImage(
      iid: Int,
      name: String,
      sourceRef: String,
      sourceDigest: String,
      status: String,
      imageTag: String,
      attempt: Int,
      creationTime: Long,
      updateTime: Long
  )

  case class CuratedImageRequest(name: String, sourceRef: String)
  case class ValidationLog(iid: Int, status: String, attempt: Int, log: String)

  // An allowlist: no quotes, angle brackets, or path and shell metacharacters, so a name is
  // safe to render and to log. Parentheses and "+" are allowed because real names use them
  // -- "Python ML (sklearn)".
  private val NamePattern = "^[A-Za-z0-9][A-Za-z0-9._ ()+-]*$".r

  /** Whether a display name is one an administrator is allowed to give an image. */
  private[resource] def isValidName(raw: String): Boolean = {
    val name = Option(raw).map(_.trim).getOrElse("")
    NamePattern.pattern.matcher(name).matches() && name.length <= MaxNameLength
  }
  // The characters a registry reference is made of. The reference reaches a shell only as
  // an environment value, never as script text, so this is a second line rather than the
  // defence -- but it refuses a malformed reference with a clear message instead of a
  // puzzling failure from skopeo.
  private val RefPattern = "^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$".r

  // Registries require the repository path to be lowercase, so an uppercase one is the
  // ordinary typo -- and the one worth catching here rather than in the validation job.
  private val RepoPattern = "^[a-z0-9][a-z0-9._:/-]*$".r

  /** The repository part of a reference: what is left once any tag or digest is removed. */
  private[service] def repositoryOf(reference: String): String = {
    val withoutDigest = reference.split("@").head
    val lastSlash = withoutDigest.lastIndexOf('/')
    val colon = withoutDigest.indexOf(':', lastSlash + 1)
    if (colon >= 0) withoutDigest.substring(0, colon) else withoutDigest
  }

  private val MaxNameLength = 128
  private val MaxRefLength = 512

  /** Accepts either an image reference or the Docker Hub page address it was copied from. */
  private[resource] def normaliseRef(raw: String): String = {
    val trimmed = raw.trim.stripSuffix("/")
    val withoutScheme = trimmed.replaceFirst("^https?://", "")
    val repo =
      if (withoutScheme.startsWith(DockerHubHost + "/")) dockerHubRepo(withoutScheme)
      else stripImplicitDockerHubRegistry(withoutScheme)

    // Default a missing tag to :latest, as every container tool does. Looks after the last
    // slash so a registry's port is not mistaken for a tag.
    val lastSegment = repo.substring(repo.lastIndexOf('/') + 1)
    if (lastSegment.contains(":") || repo.contains("@sha256:")) repo else s"$repo:latest"
  }

  private val DockerHubHost = "hub.docker.com"

  /**
    * Reduces the ways of naming a Docker Hub image to one string, so "owner/name:1" and
    * "docker.io/owner/name:1" are seen as the duplicate they are.
    */
  private val DockerHubRegistries =
    Seq("docker.io/", "index.docker.io/", "registry-1.docker.io/")

  private def stripImplicitDockerHubRegistry(reference: String): String =
    DockerHubRegistries.find(reference.startsWith) match {
      case None => reference
      case Some(registry) =>
        val path = reference.stripPrefix(registry)
        if (path.startsWith("library/")) path.stripPrefix("library/") else path
    }

  /**
    * The pull reference inside a Docker Hub web address, since pasting one is the easy
    * mistake to make:
    *
    *   hub.docker.com/r/<owner>/<name>                    the public page
    *   hub.docker.com/_/<name>                            an official image
    *   hub.docker.com/repository/docker/<owner>/<name>    the owner's own page
    *
    * A trailing tab segment (/general, /tags) belongs to the page, not the reference.
    * An unrecognised address is returned unchanged for validate() to reject.
    */
  private def dockerHubRepo(address: String): String = {
    val path = address.stripPrefix(DockerHubHost + "/")
    if (path.startsWith("_/")) path.stripPrefix("_/").split("/").head
    else if (path.startsWith("r/")) path.stripPrefix("r/").split("/").take(2).mkString("/")
    else if (path.startsWith("repository/docker/"))
      path.stripPrefix("repository/docker/").split("/").take(2).mkString("/")
    else address
  }

  /**
    * The reference a unit runs: the registered repository at the digest validation resolved.
    * Derived rather than stored, so the two can never disagree.
    */
  private[service] def pinnedRefOf(sourceRef: String, sourceDigest: String): Option[String] =
    Option(sourceDigest).filter(_.nonEmpty).map(ImageValidationClient.pinnedRef(sourceRef, _))

  private def toCuratedImage(record: Record): CuratedImage =
    CuratedImage(
      iid = record.get(IID),
      name = record.get(NAME),
      sourceRef = record.get(SOURCE_REF),
      sourceDigest = record.get(SOURCE_DIGEST),
      status = record.get(STATUS),
      imageTag = pinnedRefOf(record.get(SOURCE_REF), record.get(SOURCE_DIGEST)).orNull,
      attempt = record.get(ATTEMPT),
      creationTime = record.get(CREATION_TIME).getTime,
      updateTime = record.get(UPDATE_TIME).getTime
    )

  /**
    * The image a computing unit should start from, or None if it cannot be started from.
    * No ownership check: these are offered to every user by design.
    */
  def readyImageFor(iid: Int): Option[String] = {
    // A disabled deployment starts nothing, including from a row left behind by an
    // earlier enabled run.
    if (!CuratedImageConfig.enabled) return None
    val record = Option(
      context.select(STATUS, SOURCE_REF, SOURCE_DIGEST).from(CU_IMAGE).where(IID.eq(iid)).fetchOne()
    )
    record.flatMap { r =>
      if (r.get(STATUS) != Status.Ready) None
      else pinnedRefOf(r.get(SOURCE_REF), r.get(SOURCE_DIGEST))
    }
  }

  def nameOf(iid: Int): Option[String] =
    Option(context.select(NAME).from(CU_IMAGE).where(IID.eq(iid)).fetchOne()).map(_.get(NAME))

  private def sourceRefOf(iid: Int): Option[String] =
    Option(context.select(SOURCE_REF).from(CU_IMAGE).where(IID.eq(iid)).fetchOne())
      .map(_.get(SOURCE_REF))

  /**
    * Brings the pre-pulls in the cluster into line with the ready images in the database:
    * adds the missing, repoints the stale, and removes the rest.
    *
    * Reconciling rather than only adding is what makes the pre-pulls reapable at all.
    * Every other path that removes one -- a deleted row, an image that left READY -- can
    * be interrupted between the database write and the cluster call, and nothing else
    * revisits a row that is gone or not ready. Whatever is left over is found here.
    *
    * One labelled list and one query, so a settled deployment does no work.
    */
  private def reconcilePrepulls(): Unit = {
    try {
      // None means the cluster could not be asked, which is not "nothing is pre-pulled":
      // acting on that would fire a doomed call for every image on every read.
      ImagePrepullClient.prepulledRefs().foreach { prepulled =>
        // Turning pre-pulling off is what frees the node disk it costs, so the pre-pulls
        // already made are removed rather than merely left un-added to. Driven by what the
        // listing actually found, so a deployment that has been off stops calling once
        // there is nothing left rather than issuing a delete on every page load.
        val wantedByImage: Map[Int, String] =
          if (!CuratedImageConfig.prepullEnabled) Map.empty
          else
            context
              .select(IID, SOURCE_REF, SOURCE_DIGEST)
              .from(CU_IMAGE)
              .where(STATUS.eq(Status.Ready))
              .fetch()
              .asScala
              .flatMap { row =>
                pinnedRefOf(row.get(SOURCE_REF), row.get(SOURCE_DIGEST))
                  .map(row.get(IID).intValue() -> _)
              }
              .toMap

        // Compared by reference, not merely by presence: a refresh whose repoint failed
        // leaves a pre-pull holding the previous digest, and nothing else would correct it.
        wantedByImage.foreach {
          case (iid, wanted) =>
            if (!prepulled.get(iid).contains(wanted)) ImagePrepullClient.ensurePrepull(iid, wanted)
        }

        // Orphans: a row deleted while the cluster was unreachable, an image that left
        // READY, or a process that stopped between the two. No row refers to these.
        (prepulled.keySet -- wantedByImage.keySet).foreach(ImagePrepullClient.deletePrepull)
      }
    } catch {
      // Opportunistic, like reconciling validations: a listing that cannot be repaired is
      // still worth returning.
      case e: Throwable =>
        logger.warn("Could not reconcile the curated-image pre-pulls; leaving them as they are.", e)
    }
  }

  /**
    * Brings VALIDATING rows up to date with what the cluster did. Validation finishes on the
    * cluster, so a row learns its outcome when someone reads it -- no background threads or
    * leader election, at the cost of a status that is stale until that read.
    */
  private def reconcileRunningValidations(): Unit = {
    val running = context
      .select(IID, ATTEMPT, UPDATE_TIME, SOURCE_REF)
      .from(CU_IMAGE)
      .where(STATUS.eq(Status.Validating))
      .fetch()
      .asScala
      .toList

    running.foreach { row =>
      val iid = row.get(IID).intValue()
      val attempt = row.get(ATTEMPT).intValue()
      // The cluster may be unreachable, or the Role not yet reapplied after an upgrade.
      // Reconciling is opportunistic, so a row that cannot be checked is left as it is
      // rather than failing a read that would otherwise return every other image.
      try reconcileOne(iid, attempt, row.get(UPDATE_TIME).getTime)
      catch {
        case e: Throwable =>
          logger.warn(s"Could not check the validation of image $iid; leaving it as it is.", e)
      }
    }
  }

  private def reconcileOne(iid: Int, attempt: Int, updatedAt: Long): Unit = {
    // State first, then the log. The other order can read a log written while the job was
    // still running and then judge it against a state that says it finished -- the digest
    // line would be missing and a successful validation would be recorded as failed.
    val state = ImageValidationClient.validationState(iid, attempt)
    val log = ImageValidationClient.validationLog(iid, attempt)

    state match {
      case ValidationState.Running =>
        // Kept fresh so the log can be watched while the copy is still going.
        log.foreach(text => updateLogOnly(iid, attempt, text))

      // Only the log says what a successful job resolved. If it cannot be read this time --
      // the pod not listed yet, or the read failing -- the row is left as it is and tried
      // again on the next read. Recording FAILED on a guess would also delete the job, and
      // with it the only account of what really happened.
      case ValidationState.Succeeded if log.isEmpty =>
        logger.warn(s"Validation $iid/$attempt succeeded but its log could not be read yet.")

      case ValidationState.Succeeded =>
        val text = log.getOrElse("")
        val digest = ImageValidationClient.sourceDigestFrom(text)
        finishValidation(
          iid,
          attempt,
          // Without a digest there is nothing to pin, so the image cannot be started
          // from and calling it ready would strand a unit in ImagePullBackOff.
          if (digest.isDefined) Status.Ready else Status.Failed,
          digest,
          text + validationNote(digest, digest.flatMap(sameContentNote(iid, _)))
        )

      case ValidationState.Failed =>
        // A job stopped by activeDeadlineSeconds has its pods removed by the Job
        // controller, so there is no log left to explain it. The Job's own condition is
        // the only account of what happened.
        val reason =
          log.filter(_.nonEmpty).orElse(ImageValidationClient.failureReason(iid, attempt))
        finishValidation(
          iid,
          attempt,
          Status.Failed,
          None,
          reason.getOrElse("The validation failed without reporting a reason.")
        )

      case ValidationState.Absent =>
        // The job is created just after the row is marked VALIDATING and the two are not
        // atomic, so a validation submitted moments ago legitimately has no job yet. Only a
        // row that has been waiting a while is genuinely orphaned.
        val age = System.currentTimeMillis() - updatedAt
        if (age > AbsentGracePeriodMillis) {
          finishValidation(
            iid,
            attempt,
            Status.Failed,
            None,
            "The validation job disappeared before it reported a result."
          )
        }
    }
  }

  private val AbsentGracePeriodMillis = 60_000L

  private[service] def validate(request: CuratedImageRequest): Unit = {
    val name = Option(request.name).map(_.trim).getOrElse("")
    if (!isValidName(name)) {
      throw new BadRequestException(
        "Image name must start with a letter or digit and contain only letters, digits, " +
          "spaces, dots, hyphens, underscores, parentheses and plus signs, and be at " +
          s"most $MaxNameLength characters."
      )
    }
    val ref = Option(request.sourceRef).map(_.trim).getOrElse("")
    if (ref.isEmpty) {
      throw new BadRequestException("Docker Hub link cannot be empty.")
    }
    // Measured on the normalised reference, because that is the one stored: an untagged
    // reference grows by ":latest" and would otherwise overflow the column.
    if (normaliseRef(ref).length > MaxRefLength) {
      throw new BadRequestException(s"Docker Hub link exceeds $MaxRefLength characters.")
    }
    // Whitespace means two things were pasted; validation would fail less clearly.
    if (normaliseRef(ref).exists(_.isWhitespace)) {
      throw new BadRequestException("Docker Hub link cannot contain spaces.")
    }
    // An unrecognised hub.docker.com address would be pulled as if hub.docker.com were a
    // registry; skopeo gets HTML back and reports it verbatim. Say so now instead.
    if (normaliseRef(ref).startsWith(DockerHubHost + "/")) {
      throw new BadRequestException(
        s"'$ref' is a Docker Hub page, not an image reference, and its shape is not one " +
          "this recognises. Use the image's own reference -- for example " +
          "'acme/texera-cu-sklearn:1.0' -- or the address of its repository page."
      )
    }
    if (!RefPattern.pattern.matcher(normaliseRef(ref)).matches()) {
      throw new BadRequestException(
        "Docker Hub link must start with a letter or digit and contain only letters, " +
          "digits, dots, hyphens, underscores, slashes, colons, at signs and plus signs."
      )
    }
    if (!RepoPattern.pattern.matcher(repositoryOf(normaliseRef(ref))).matches()) {
      throw new BadRequestException(
        s"'$ref' has an uppercase letter in its repository name. Registries only accept " +
          "lowercase there -- a tag after the colon may have capitals, the part before it " +
          "may not."
      )
    }
  }

  /**
    * What a finished validation adds to its log: nothing in the ordinary case, the duplicate
    * note when one applies, and the cannot-be-pinned explanation only when no digest was
    * resolved.
    */
  private[service] def validationNote(
      digest: Option[String],
      duplicateNote: Option[String]
  ): String =
    if (digest.isEmpty) "\n\nThe image could not be pinned: no digest was resolved."
    else duplicateNote.getOrElse("")

  /**
    * Logs when another row resolved to this same digest. Only knowable after validation,
    * so it is a note rather than a rejection.
    */
  private def sameContentNote(iid: Int, digest: String): Option[String] = {
    val others = context
      .select(NAME)
      .from(CU_IMAGE)
      .where(SOURCE_DIGEST.eq(digest))
      .and(IID.ne(iid))
      .and(STATUS.eq(Status.Ready))
      .fetch()
      .asScala
      .map(_.get(NAME))
      .toList

    Option.when(others.nonEmpty)(
      s"\n\nNote: this is the same image as ${others.map("'" + _ + "'").mkString(", ")} " +
        "-- same digest, reached by a different reference. The registry stores the layers " +
        "once, so the duplicate costs little space, but only one of these rows is needed."
    )
  }

  private def updateLogOnly(iid: Int, attempt: Int, log: String): Unit =
    context
      .update(CU_IMAGE)
      .set(VALIDATION_LOG, log)
      .where(IID.eq(iid).and(ATTEMPT.eq(attempt)))
      .execute()

  /**
    * Records an outcome, but only against the attempt that produced it. A refresh running
    * at the same time moves the row to the next attempt, and without this guard the older
    * validation's result would land on it and the refresh would never be polled again.
    */
  private def finishValidation(
      iid: Int,
      attempt: Int,
      status: String,
      sourceDigest: Option[String],
      log: String
  ): Unit = {
    val update = context
      .update(CU_IMAGE)
      .set(STATUS, status)
      .set(VALIDATION_LOG, log)
      .set(UPDATE_TIME, new Timestamp(System.currentTimeMillis()))

    // Leaves the previous digest alone, so a unit on the last good one keeps working.
    val withDigest = sourceDigest.fold(update)(digest => update.set(SOURCE_DIGEST, digest))
    val stored = withDigest.where(IID.eq(iid).and(ATTEMPT.eq(attempt))).execute()
    // The job is kept only until its outcome is recorded, so finished jobs do not pile up
    // in the pool namespace.
    if (stored > 0) {
      ImageValidationClient.deleteValidation(iid, attempt)
      // Only on READY, because only then is there something a unit can be started from.
      // A refresh that resolved a moved tag arrives here too, and points the pre-pull at
      // the new digest rather than leaving the node holding bytes nothing runs.
      if (status == Status.Ready) {
        pinnedRefOf(sourceRefOf(iid).orNull, sourceDigest.orNull)
          .foreach(ImagePrepullClient.ensurePrepull(iid, _))
      } else {
        // A failed refresh keeps the previous digest, so the row still names an image --
        // but no unit can be started from it any more, and its pre-pull would go on
        // holding gigabytes on every node for something unusable. Best-effort here;
        // reconcilePrepulls is the backstop if this call does not happen.
        ImagePrepullClient.deletePrepull(iid)
      }
    }
  }
}

@Path("/cu-image")
@Produces(Array(MediaType.APPLICATION_JSON))
class CuratedImageResource extends LazyLogging {

  import CuratedImageResource._

  private def requireEnabled(): Unit =
    if (!CuratedImageConfig.enabled) {
      throw new ServiceUnavailableException("Curated images are not enabled on this deployment.")
    }

  /**
    * Any signed-in user may read the list, since the computing-unit dropdown is built from
    * it. Only an administrator may change it -- that restriction is what makes these images
    * trusted.
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("")
  def list(@Auth user: SessionUser): List[CuratedImage] = {
    requireEnabled()
    reconcileRunningValidations()
    reconcilePrepulls()
    context
      // Not select(): validation_log is unbounded and this endpoint discards it.
      .select(IID, NAME, SOURCE_REF, SOURCE_DIGEST, STATUS, ATTEMPT, CREATION_TIME, UPDATE_TIME)
      .from(CU_IMAGE)
      .orderBy(NAME.asc())
      .fetch()
      .asScala
      .map(toCuratedImage)
      .toList
  }

  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/{iid}/log")
  def log(@PathParam("iid") iid: Int, @Auth user: SessionUser): ValidationLog = {
    requireEnabled()
    reconcileRunningValidations()
    val record = Option(
      context.select(STATUS, ATTEMPT, VALIDATION_LOG).from(CU_IMAGE).where(IID.eq(iid)).fetchOne()
    ).getOrElse(throw new NotFoundException(s"No curated image $iid."))

    ValidationLog(
      iid = iid,
      status = record.get(STATUS),
      attempt = record.get(ATTEMPT).intValue(),
      log = Option(record.get(VALIDATION_LOG)).getOrElse("")
    )
  }

  @POST
  @RolesAllowed(Array("ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("")
  def create(request: CuratedImageRequest, @Auth user: SessionUser): CuratedImage = {
    requireEnabled()
    validate(request)
    val name = request.name.trim
    val sourceRef = normaliseRef(request.sourceRef)

    if (context.fetchExists(context.selectFrom(CU_IMAGE).where(NAME.eq(name)))) {
      throw new BadRequestException(s"An image named '$name' already exists.")
    }

    // Naming the existing row lets the administrator use or refresh it, instead of ending up
    // with two rows for one image.
    val duplicate = Option(
      context.select(NAME).from(CU_IMAGE).where(SOURCE_REF.eq(sourceRef)).fetchAny()
    ).map(_.get(NAME))
    duplicate.foreach { existing =>
      throw new BadRequestException(
        s"'$sourceRef' is already curated as '$existing'. Use that image, or refresh it " +
          "to pick up a moved tag, instead of registering the same reference twice."
      )
    }

    // The checks above are a read then a write, so two simultaneous registrations both pass
    // them; the unique constraints refuse the second. Caught here to answer with the same
    // explanation rather than a bare 500.
    val iid =
      try {
        context
          .insertInto(CU_IMAGE)
          .set(NAME, name)
          .set(SOURCE_REF, sourceRef)
          .set(STATUS, Status.Pending)
          .set(CREATED_BY, Integer.valueOf(user.getUid.intValue()))
          .returning(IID)
          .fetchOne()
          .get(IID)
          .intValue()
      } catch {
        case _: org.jooq.exception.IntegrityConstraintViolationException =>
          throw new BadRequestException(
            s"'$sourceRef' or the name '$name' was registered a moment ago by someone " +
              "else. Reload the list -- the image is already there."
          )
      }

    startValidation(iid, sourceRef)
    fetch(iid)
  }

  /**
    * Validates the source again: how a deployment picks up a moved tag, and how a validation
    * that failed on the network is retried.
    */
  @POST
  @RolesAllowed(Array("ADMIN"))
  @Path("/{iid}/refresh")
  def refresh(@PathParam("iid") iid: Int, @Auth user: SessionUser): CuratedImage = {
    requireEnabled()
    val record = Option(
      context.select(SOURCE_REF).from(CU_IMAGE).where(IID.eq(iid)).fetchOne()
    ).getOrElse(throw new NotFoundException(s"No curated image $iid."))

    startValidation(iid, record.get(SOURCE_REF))
    fetch(iid)
  }

  @DELETE
  @RolesAllowed(Array("ADMIN"))
  @Path("/{iid}")
  def delete(@PathParam("iid") iid: Int, @Auth user: SessionUser): Unit = {
    requireEnabled()
    // Nothing of ours holds a copy. A running unit keeps going on what its node pulled.
    ImageValidationClient.deleteAllValidations(iid)
    // The row goes first, so a concurrent read cannot see a row that is still READY and
    // re-create the pre-pull just removed. If this call is the one that does not happen,
    // reconcilePrepulls removes what no ready row wants on the next read.
    val deleted = context.deleteFrom(CU_IMAGE).where(IID.eq(iid)).execute()
    if (deleted == 0) {
      throw new NotFoundException(s"No curated image $iid.")
    }
    // Left behind, it would go on holding the image on every node for an image no unit can
    // be started from any more.
    ImagePrepullClient.deletePrepull(iid)
  }

  /** Marks the row as being validated and submits the job, in that order. */
  private def startValidation(iid: Int, sourceRef: String): Unit = {
    // Read and claimed in one statement. Two refreshes at the same moment would otherwise
    // both compute the same attempt, and the second would delete the first's job.
    val attempt = Option(
      context
        .update(CU_IMAGE)
        .set(STATUS, Status.Validating)
        .set(ATTEMPT, ATTEMPT.plus(1))
        .set(VALIDATION_LOG, "")
        .set(UPDATE_TIME, new Timestamp(System.currentTimeMillis()))
        .where(IID.eq(iid))
        .returningResult(ATTEMPT)
        .fetchOne()
    ).map(_.value1().intValue())
      .getOrElse(throw new NotFoundException(s"No curated image $iid."))

    try {
      ImageValidationClient.startValidation(iid, attempt, sourceRef)
    } catch {
      // Without this the row would sit in VALIDATING waiting for a job that was never
      // created, and only the grace period would eventually call it failed.
      case e: Throwable =>
        logger.error(s"Could not start validation for image $iid", e)
        context
          .update(CU_IMAGE)
          .set(STATUS, Status.Failed)
          .set(VALIDATION_LOG, ImageValidationClient.describeStartFailure(e))
          .where(IID.eq(iid).and(ATTEMPT.eq(attempt)))
          .execute()
    }
  }

  private def fetch(iid: Int): CuratedImage =
    Option(context.select().from(CU_IMAGE).where(IID.eq(iid)).fetchOne())
      .map(toCuratedImage)
      .getOrElse(throw new NotFoundException(s"No curated image $iid."))
}
