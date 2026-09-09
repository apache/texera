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

import org.apache.texera.common.config.CuratedImageConfig
import io.fabric8.kubernetes.api.model.batch.v1.{Job, JobBuilder, JobCondition, JobConditionBuilder}
import org.apache.texera.service.util.ImageValidationClient
import org.apache.texera.service.util.ImageValidationClient.ValidationState
import org.scalatest.OptionValues._

import scala.jdk.CollectionConverters._
import org.scalatest.flatspec.AnyFlatSpec
import jakarta.ws.rs.BadRequestException
import org.scalatest.matchers.should.Matchers

class CuratedImageResourceSpec extends AnyFlatSpec with Matchers {

  import CuratedImageResource.{isValidName, normaliseRef}

  // The regression this guards: the pattern used to exclude parentheses, so the names an
  // administrator actually reaches for -- and the ones the demo instructions themselves
  // used -- were rejected with HTTP 400 before anything was created.
  "isValidName" should "accept the names administrators actually type" in {
    isValidName("Texera Default") shouldBe true
    isValidName("Python ML (sklearn)") shouldBe true
    isValidName("PyTorch 2.6 (CUDA 12)") shouldBe true
    isValidName("cu-image_v1.0") shouldBe true
    isValidName("gcc+cuda") shouldBe true
  }

  it should "still refuse anything that is not a plain display name" in {
    isValidName("") shouldBe false
    isValidName("   ") shouldBe false
    // must start alphanumeric, so no leading punctuation or whitespace-only leaders
    isValidName("-leading-hyphen") shouldBe false
    isValidName("(leading-paren)") shouldBe false
    // no quoting, markup, path or shell metacharacters
    isValidName("name\"quote") shouldBe false
    isValidName("<script>") shouldBe false
    isValidName("a/b") shouldBe false
    isValidName("a;rm -rf") shouldBe false
    isValidName("a\nb") shouldBe false
    isValidName("caf\u00e9") shouldBe false
  }

  it should "reject a name longer than the column allows" in {
    isValidName("a" * 128) shouldBe true
    isValidName("a" * 129) shouldBe false
  }

  "normaliseRef" should "leave a complete image reference alone" in {
    normaliseRef("texera/cu-alphafold3:1.0") shouldBe "texera/cu-alphafold3:1.0"
  }

  it should "default a missing tag rather than reject it" in {
    normaliseRef("texera/cu-alphafold3") shouldBe "texera/cu-alphafold3:latest"
  }

  // The whole reason this function exists: an administrator curating from a browser will
  // paste the page they are looking at, not a reference they had to construct.
  it should "turn a Docker Hub page address into a pull reference" in {
    normaliseRef("https://hub.docker.com/r/texera/cu-alphafold3") shouldBe
      "texera/cu-alphafold3:latest"
    normaliseRef("hub.docker.com/r/texera/cu-alphafold3") shouldBe "texera/cu-alphafold3:latest"
  }

  // The address bar shows this while an owner manages their own image, so it is the one
  // an administrator curating their own build is most likely to paste.
  it should "turn an owner's own repository page address into a pull reference" in {
    normaliseRef("https://hub.docker.com/repository/docker/acme/texera-cu-sklearn") shouldBe
      "acme/texera-cu-sklearn:latest"
  }

  // /general, /tags and /settings are parts of the web page, not of the reference.
  it should "drop the page's tab segment" in {
    normaliseRef(
      "https://hub.docker.com/repository/docker/acme/texera-cu-sklearn/general"
    ) shouldBe
      "acme/texera-cu-sklearn:latest"
    normaliseRef(
      "https://hub.docker.com/repository/docker/acme/texera-cu-sklearn/tags"
    ) shouldBe
      "acme/texera-cu-sklearn:latest"
    normaliseRef("https://hub.docker.com/r/acme/texera-cu-sklearn/tags") shouldBe
      "acme/texera-cu-sklearn:latest"
  }

  it should "leave a Docker Hub address it does not recognise alone, for validate to reject" in {
    // Better an immediate rejection than a validation that discovers hub.docker.com is not
    // a registry and prints the 404 page it got back.
    normaliseRef("https://hub.docker.com/u/acme") should startWith("hub.docker.com/")
  }

  it should "tolerate a trailing slash, which a copied address usually has" in {
    normaliseRef("https://hub.docker.com/r/texera/cu-alphafold3/") shouldBe
      "texera/cu-alphafold3:latest"
  }

  // An official image lives under /_/ and is pulled by its bare name, so the path prefix
  // has to come off or the reference would name a repository that does not exist.
  it should "handle a Docker Hub official image" in {
    normaliseRef("https://hub.docker.com/_/ubuntu") shouldBe "ubuntu:latest"
  }

  it should "trim surrounding whitespace" in {
    normaliseRef("  texera/img:1.0  ") shouldBe "texera/img:1.0"
  }

  // The regression this guards: a registry's port contains a colon, and looking for one
  // anywhere in the reference would read ":5000/team/img" as a tag and leave the image
  // untagged.
  it should "not mistake a registry port for a tag" in {
    normaliseRef("myregistry.io:5000/team/img") shouldBe "myregistry.io:5000/team/img:latest"
    normaliseRef("10.96.0.99:5000/texera/computing-unit-master:dev") shouldBe
      "10.96.0.99:5000/texera/computing-unit-master:dev"
  }

  // Docker Hub is what a bare reference already means, so both forms have to reduce to
  // the same string. Otherwise one image registered as "owner/name:1" and again as
  // "docker.io/owner/name:1" is curated twice -- the duplicate check compares references,
  // so it can only catch what normalisation made equal.
  it should "reduce an explicit Docker Hub registry to the bare reference" in {
    normaliseRef("docker.io/acme/texera-cu-sklearn:1.0") shouldBe
      "acme/texera-cu-sklearn:1.0"
    normaliseRef("index.docker.io/acme/texera-cu-sklearn:1.0") shouldBe
      "acme/texera-cu-sklearn:1.0"
    normaliseRef("registry-1.docker.io/acme/texera-cu-sklearn:1.0") shouldBe
      "acme/texera-cu-sklearn:1.0"
  }

  // "library/" is Docker Hub's namespace for official images, whose reference is the name.
  it should "reduce an official image's fully qualified reference" in {
    normaliseRef("docker.io/library/ubuntu:22.04") shouldBe "ubuntu:22.04"
  }

  // A real registry that merely starts with similar text must be left alone.
  it should "not mistake another registry for Docker Hub" in {
    normaliseRef("docker.io.evil.example/team/img:1") shouldBe "docker.io.evil.example/team/img:1"
    normaliseRef("ghcr.io/apache/texera:latest") shouldBe "ghcr.io/apache/texera:latest"
  }

  it should "leave a digest-pinned reference untagged" in {
    normaliseRef("texera/img@sha256:abc123") shouldBe "texera/img@sha256:abc123"
  }

  // A unit is started from the administrator's repository at the digest validation
  // resolved, so the tag they typed has to come off first.
  // The ordinary success -- a digest resolved, no other row sharing it -- must add nothing.
  // The regression this guards said "could not be pinned" on every such READY image.
  "validationNote" should "add nothing when a digest was resolved and is unique" in {
    CuratedImageResource.validationNote(Some("sha256:abc"), None) shouldBe ""
  }

  it should "add the duplicate note when another row is the same image" in {
    CuratedImageResource.validationNote(Some("sha256:abc"), Some("\n\nSame as 'other'.")) shouldBe
      "\n\nSame as 'other'."
  }

  it should "explain the failure to pin only when no digest was resolved" in {
    CuratedImageResource.validationNote(None, None) should include("could not be pinned")
  }

  private def rejects(name: String, ref: String): String =
    intercept[BadRequestException](
      CuratedImageResource.validate(CuratedImageResource.CuratedImageRequest(name, ref))
    ).getMessage

  // The reference reaches the validation job as an environment value, never as script
  // text, so a shell metacharacter cannot run anything. It is still refused here so the
  // administrator gets a clear message rather than a puzzling failure from skopeo.
  "validate" should "refuse a reference carrying shell metacharacters" in {
    rejects("ok", "acme/img$(id)") should include("letters")
    rejects("ok", "acme/img\";id;\"") should include("letters")
    rejects("ok", "acme/img`id`") should include("letters")
  }

  it should "accept the reference shapes a registry actually uses" in {
    noException should be thrownBy
      CuratedImageResource.validate(CuratedImageResource.CuratedImageRequest("ok", "acme/img:1.0"))
    noException should be thrownBy
      CuratedImageResource.validate(
        CuratedImageResource.CuratedImageRequest("ok", "registry.example:5000/team/img@sha256:abc")
      )
  }

  // Measured after normalising, because ":latest" is added before the value is stored and
  // the column is only so wide.
  it should "measure the length of the reference it will store, not the one given" in {
    val untagged = "acme/" + ("a" * 503)
    untagged.length shouldBe 508
    rejects("ok", untagged) should include("exceeds")
  }

  // image_tag used to be a stored column. It is now derived on read, so these guard that
  // the derivation gives the same answer -- including the null-digest case the column had.
  "pinnedRefOf" should "combine the registered reference with the resolved digest" in {
    CuratedImageResource.pinnedRefOf("acme/img:1.0", "sha256:abc") shouldBe
      Some("acme/img@sha256:abc")
  }

  it should "have nothing to offer until a validation has resolved a digest" in {
    CuratedImageResource.pinnedRefOf("acme/img:1.0", null) shouldBe None
    CuratedImageResource.pinnedRefOf("acme/img:1.0", "") shouldBe None
  }

  "pinnedRef" should "address the same repository by digest instead of by tag" in {
    ImageValidationClient.pinnedRef("texera/img:1.0", "sha256:abc") shouldBe "texera/img@sha256:abc"
    ImageValidationClient.pinnedRef("ghcr.io/apache/texera:latest", "sha256:def") shouldBe
      "ghcr.io/apache/texera@sha256:def"
  }

  it should "leave a reference that already names a digest alone" in {
    ImageValidationClient.pinnedRef("texera/img@sha256:abc", "sha256:zzz") shouldBe
      "texera/img@sha256:abc"
  }

  // The same trap as everywhere else: a registry's port is a colon that is not a tag, and
  // truncating there would pin a repository that does not exist.
  it should "not mistake a registry port for a tag" in {
    ImageValidationClient.pinnedRef("registry.example:5000/team/img:2", "sha256:abc") shouldBe
      "registry.example:5000/team/img@sha256:abc"
    ImageValidationClient.pinnedRef("registry.example:5000/team/img", "sha256:abc") shouldBe
      "registry.example:5000/team/img@sha256:abc"
  }

  it should "pin an untagged reference as it stands" in {
    ImageValidationClient.pinnedRef("texera/img", "sha256:abc") shouldBe "texera/img@sha256:abc"
  }

  "sourceDigestFrom" should "read the digest a finished validation printed" in {
    val log =
      """Inspecting texera/cu-alphafold3:1.0
        |Start command: [bin/computing-unit-master] []
        |TEXERA_SOURCE_DIGEST=sha256:0123abc
        |Validated texera/cu-alphafold3:1.0
        |""".stripMargin
    ImageValidationClient.sourceDigestFrom(log) shouldBe Some("sha256:0123abc")
  }

  // The regression this guards: the bare exception message can be something as useless as
  // "An error has occurred." when the client fell back to a default API address, which
  // reads as a Texera bug rather than a missing cluster.
  "describeStartFailure" should "name the failure type and where it was talking to" in {
    val described = ImageValidationClient.describeStartFailure(
      new RuntimeException("An error has occurred.")
    )
    described should include("RuntimeException")
    described should include("An error has occurred.")
    described should include("Kubernetes API address")
    described should include("reachable cluster")
  }

  it should "still say something useful when the exception has no message" in {
    val described = ImageValidationClient.describeStartFailure(new NullPointerException)
    described should include("NullPointerException")
    described should include("no message")
  }

  it should "return nothing when validation failed before printing one" in {
    val log =
      """Inspecting texera/not-a-cu-image:1.0
        |Start command: [/bin/bash] []
        |ERROR: texera/not-a-cu-image:1.0 does not look like a Texera computing-unit image.
        |""".stripMargin
    ImageValidationClient.sourceDigestFrom(log) shouldBe None
  }

  // Off until the UI ships, so a deployment that has not opted in starts no unit from a
  // curated image -- including from a row left behind if it was enabled and turned off.
  "the feature flag" should "be off unless a deployment turns it on" in {
    CuratedImageConfig.enabled shouldBe false
  }

  it should "start no unit from a curated image while it is off" in {
    CuratedImageResource.readyImageFor(1) shouldBe None
  }

  // The states below are the ones a real cluster produces; the DeadlineExceeded shape was
  // taken from a job actually killed by activeDeadlineSeconds (failed=1, condition Failed
  // with that reason, and no pods left to read a log from).
  private def job(succeeded: Integer, failed: Integer, conditions: JobCondition*): Job =
    new JobBuilder()
      .withNewMetadata()
      .withName("cu-image-check-1-1")
      .endMetadata()
      .withNewStatus()
      .withSucceeded(succeeded)
      .withFailed(failed)
      .withConditions(conditions.toList.asJava)
      .endStatus()
      .build()

  private def condition(condType: String, reason: String, message: String): JobCondition =
    new JobConditionBuilder()
      .withType(condType)
      .withReason(reason)
      .withMessage(message)
      .build()

  "stateOf" should "report a job that is not there as absent" in {
    ImageValidationClient.stateOf(None) shouldBe ValidationState.Absent
  }

  it should "report a job with no terminal count as still running" in {
    ImageValidationClient.stateOf(Some(job(null, null))) shouldBe ValidationState.Running
  }

  it should "report a succeeded job as succeeded" in {
    ImageValidationClient.stateOf(Some(job(1, null))) shouldBe ValidationState.Succeeded
  }

  it should "report a failed job as failed" in {
    ImageValidationClient.stateOf(Some(job(null, 1))) shouldBe ValidationState.Failed
  }

  // A job killed by its deadline reports failed=1, so the row is settled rather than left
  // waiting for a result that will never come.
  it should "treat a job stopped by its deadline as failed" in {
    val deadline =
      job(
        null,
        1,
        condition("Failed", "DeadlineExceeded", "Job was active longer than specified deadline")
      )
    ImageValidationClient.stateOf(Some(deadline)) shouldBe ValidationState.Failed
  }

  "failureReasonOf" should "explain a deadline in terms of the timeout that caused it" in {
    val deadline =
      job(
        null,
        1,
        condition("Failed", "DeadlineExceeded", "Job was active longer than specified deadline")
      )
    val reason = ImageValidationClient.failureReasonOf(deadline)
    reason.value should include(CuratedImageConfig.validationTimeoutSeconds.toString)
    reason.value should include("did not answer in time")
  }

  it should "pass on the message of any other failure" in {
    val other = job(
      null,
      1,
      condition("Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit")
    )
    ImageValidationClient.failureReasonOf(other).value should include("backoff limit")
  }

  it should "fall back to the reason when the message is empty" in {
    val bare = job(null, 1, condition("Failed", "BackoffLimitExceeded", ""))
    ImageValidationClient.failureReasonOf(bare) shouldBe Some("BackoffLimitExceeded")
  }

  // Nothing to say about a job that has not failed, so the caller keeps its own wording.
  it should "have nothing to say about a job with no failure condition" in {
    ImageValidationClient.failureReasonOf(job(1, null)) shouldBe None
    ImageValidationClient.failureReasonOf(
      job(null, 1, condition("Complete", "", "done"))
    ) shouldBe None
  }

  private def labelledJob(iid: Int, attempt: Int, labelled: Boolean = true): Job = {
    val b = new JobBuilder().withNewMetadata().withName(s"cu-image-check-$iid-$attempt")
    val withLabels =
      if (labelled)
        b.addToLabels("texera-cu-image", iid.toString)
          .addToLabels("texera-cu-image-attempt", attempt.toString)
      else b.addToLabels("texera-cu-image", iid.toString)
    withLabels.endMetadata().build()
  }

  // The race this guards: two refreshes claim 2 and 3 atomically but reach the cluster in
  // any order. The one holding 2 must not delete the job of 3, or the row waits for a job
  // that no longer exists.
  "supersededJobs" should "select only attempts below the one starting" in {
    val jobs = List(labelledJob(7, 1), labelledJob(7, 2), labelledJob(7, 3))
    ImageValidationClient.supersededJobs(jobs, 3) should contain theSameElementsAs
      List("cu-image-check-7-1", "cu-image-check-7-2")
  }

  it should "never select a newer attempt than the one starting" in {
    val jobs = List(labelledJob(7, 2), labelledJob(7, 3))
    ImageValidationClient.supersededJobs(jobs, 2) shouldBe empty
  }

  it should "not select the attempt that is starting" in {
    ImageValidationClient.supersededJobs(List(labelledJob(7, 4)), 4) shouldBe empty
  }

  // A job created before the attempt label existed still has to be reapable.
  it should "fall back to the trailing number of the name when the label is absent" in {
    ImageValidationClient.supersededJobs(List(labelledJob(7, 1, labelled = false)), 3) shouldBe
      List("cu-image-check-7-1")
  }

  "attemptOf" should "prefer the label over the name" in {
    val odd = new JobBuilder()
      .withNewMetadata()
      .withName("cu-image-check-7-99")
      .addToLabels("texera-cu-image-attempt", "4")
      .endMetadata()
      .build()
    ImageValidationClient.attemptOf(odd) shouldBe Some(4)
  }

  it should "have no answer for a name it cannot read a number from" in {
    val odd = new JobBuilder().withNewMetadata().withName("something-else").endMetadata().build()
    ImageValidationClient.attemptOf(odd) shouldBe None
  }

  // Without a message or a reason there is nothing to report, and the caller's own wording
  // must survive rather than being replaced by an empty string.
  it should "leave a bare failure condition to the caller's fallback" in {
    ImageValidationClient.failureReasonOf(job(null, 1, condition("Failed", "", ""))) shouldBe None
  }

  // Registries reject an uppercase repository path, so it is caught here rather than in the
  // job. A tag may have capitals; the part before the colon may not.
  "validate" should "refuse an uppercase repository name" in {
    rejects("ok", "Acme/img:1.0") should include("lowercase")
    rejects("ok", "acme/MyImage:1.0") should include("lowercase")
  }

  it should "allow capitals in a tag" in {
    noException should be thrownBy
      CuratedImageResource.validate(
        CuratedImageResource.CuratedImageRequest("ok", "acme/img:V1.0-RC")
      )
  }

  "repositoryOf" should "drop a tag but keep a registry port" in {
    CuratedImageResource.repositoryOf("registry.example:5000/team/img:2") shouldBe
      "registry.example:5000/team/img"
    CuratedImageResource.repositoryOf("acme/img:1.0") shouldBe "acme/img"
    CuratedImageResource.repositoryOf("acme/img") shouldBe "acme/img"
  }

  it should "drop a digest" in {
    CuratedImageResource.repositoryOf("acme/img@sha256:abc") shouldBe "acme/img"
  }

}
