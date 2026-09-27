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

package org.apache.texera.web.resource.pythonvirtualenvironment

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.VIRTUAL_ENVIRONMENTS
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.pythonvirtualenvironment.PveResource.SavePvePayload
import org.apache.commons.lang3.SystemUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import javax.ws.rs.core.Response
import scala.jdk.CollectionConverters._
import scala.sys.process.ProcessLogger

class PveResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testCuid = 256
  private val testUid = 8000 + scala.util.Random.nextInt(1000)
  private var testPveName: String = _
  private var testRoot: Path = _
  private var queue: LinkedBlockingQueue[String] = _

  // Exit codes the mock returns for the next venv / pip invocation. Reset to
  // success in beforeEach; individual tests flip one to force a failure.
  private var venvExit = 0
  private var installExit = 0
  private var uninstallExit = 0

  // Kept separate from installExit so a test can let `pip freeze` print a package
  // and *still* fail — a freeze that dies halfway is the case where "return what
  // was collected" and "return nothing" differ.
  private var freezeExit = 0

  // When set, the mocked runner throws for a `pip uninstall` instead of returning
  // an exit code. `Process.!` throws rather than returning non-zero when the
  // interpreter has gone missing or is not executable, which is reachable here:
  // nothing stops the venv directory being removed between the guard and the spawn.
  private var uninstallThrows = false

  // Every command the mocked runner is handed, in order. Lets a test assert what
  // did *not* run, which is the only thing that separates "gave up at this step"
  // from "ran everything and happened to produce nothing".
  private val recordedCommands = scala.collection.mutable.ListBuffer[Seq[String]]()

  // What the mocked `pip freeze` reports as the resolved system set. pyarrow is
  // always a hard dependency in amber/requirements.txt, so it stands in for "a
  // system package the user may neither install nor delete".
  //
  // Deliberately not clean: a real `pip freeze` emits blank lines and `## FIXME:`
  // comment lines for editable/VCS installs it cannot pin, and surrounding whitespace
  // is not guaranteed. Those lines must not reach `systemPackages`, because they are
  // written verbatim into the `--constraint` file every user install is pinned against,
  // where a malformed line makes pip abort. A fixture of one already-trimmed,
  // already-non-comment line leaves the resolver's sanitising step unconstrained.
  //
  // The `## FIXME:` line is indented too, which is what makes the *order* of the two
  // sanitising steps observable: trimming and then filtering is not the same as filtering
  // and then trimming, and an indented comment is the only line that can tell them apart —
  // it is a comment only once the leading whitespace is gone.
  private val systemFreeze =
    Seq("", "  ## FIXME: could not find svn location", "  pyarrow==23.0.1  ")

  // What the resolver is required to turn `systemFreeze` into.
  private val expectedSystemPackages = Seq("pyarrow==23.0.1")

  private val realRunner = PveManager.runProcess

  // Mocks every child process PveManager spawns (venv creation, pip
  // install/uninstall/freeze) so the spec is hermetic — no real venv, no pip,
  // no network. ScalaMock expectations are per-test, so expectProcessCalls() is
  // called at the top of each test that exercises a process. The single handler
  // dispatches on the command: a venv create fabricates <dir>/bin/{python,pip},
  // freeze emits the system set, install/uninstall just return the configured
  // exit code. PveManager still owns the metadata files and queue messages.
  private val runProcessMock =
    mockFunction[Seq[String], Seq[(String, String)], ProcessLogger, Int]

  private def expectProcessCalls(): Unit =
    runProcessMock
      .expects(*, *, *)
      .onCall { (command: Seq[String], _: Seq[(String, String)], logger: ProcessLogger) =>
        recordedCommands += command
        if (command.contains("venv")) {
          if (venvExit == 0) {
            val bin = Paths.get(command.last).resolve("bin")
            Files.createDirectories(bin)
            Seq("python", "pip").foreach { exe =>
              val f = bin.resolve(exe)
              Files.write(f, Array.emptyByteArray)
              f.toFile.setExecutable(true)
            }
          }
          venvExit
        } else if (command.contains("freeze")) {
          systemFreeze.foreach(line => logger.out(line))
          freezeExit
        } else if (command.contains("uninstall")) {
          if (uninstallThrows) throw new java.io.IOException("boom")
          logger.out("mock uninstall")
          // A failing pip writes the reason to stderr; that is the only line telling the
          // user *why* the delete failed, so the fixture has to produce one for the
          // stderr arm of deletePackages' ProcessLogger to be reachable at all.
          if (uninstallExit != 0) logger.err("ERROR: Cannot uninstall colorama")
          uninstallExit
        } else if (command.contains("install")) {
          logger.out("mock install")
          installExit
        } else 0
      }
      .anyNumberOfTimes()

  override protected def beforeAll(): Unit = {
    PveManager.runProcess = runProcessMock
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    val user = new User
    user.setUid(testUid)
    user.setName("pve_resource_spec_user")
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    userDao.insert(user)
  }

  override protected def afterAll(): Unit = {
    PveManager.runProcess = realRunner
    closeConnectionPool()
  }

  override protected def beforeEach(): Unit = {
    venvExit = 0
    installExit = 0
    uninstallExit = 0
    freezeExit = 0
    uninstallThrows = false
    recordedCommands.clear()
    testPveName = s"testenv${System.currentTimeMillis()}"
    testRoot = Paths.get("/tmp/texera-pve/venvs").resolve(testCuid.toString)
    queue = new LinkedBlockingQueue[String]()
    getDSLContext
      .deleteFrom(VIRTUAL_ENVIRONMENTS)
      .where(VIRTUAL_ENVIRONMENTS.UID.eq(testUid))
      .execute()
  }

  override protected def afterEach(): Unit = {
    PveManager.deleteEnvironments(testCuid)
  }

  /** Where PveManager looks for a venv's interpreter on this platform. */
  private def pythonBinFor(pveName: String): Path = {
    val venv = testRoot.resolve(pveName).resolve("pve")
    if (SystemUtils.IS_OS_WINDOWS) venv.resolve("Scripts").resolve("python.exe")
    else venv.resolve("bin").resolve("python")
  }

  private def queueText(): String = {
    queue.iterator().asScala.toList.mkString("\n")
  }

  /**
    * Puts just the interpreter where PveManager looks for it, without going through
    * `createNewPve`. The mocked venv creation fabricates a POSIX `bin/python` layout,
    * so the create flow cannot stand a PVE up on a platform whose interpreter lives
    * somewhere else; `pythonBinFor` asks PveManager's own question instead.
    */
  private def fabricatePve(pveName: String): Path = {
    val python = pythonBinFor(pveName)
    Files.createDirectories(python.getParent)
    Files.write(python, Array.emptyByteArray)
    python.toFile.setExecutable(true)
    python
  }

  /** Where PveManager records the packages the user installed into a PVE. */
  private def userPackagesFile(pveName: String): Path =
    testRoot.resolve(pveName).resolve("user-packages.txt")

  /**
    * Calls `PveManager.resolveSystemPackages()` directly.
    *
    * Its public face is `getSystemPackages`, which reads the `systemPackages` lazy val.
    * That val is memoised for the life of the JVM and amber's suites share one, so a test
    * that forced it through a failing runner would fix the system package set at empty for
    * every suite that follows — starting with this spec's own two system-package tests.
    * Reaching the resolver directly is what makes its failure arms testable without that
    * side effect; there is no seam that would let an ordinary call do the same.
    */
  private def resolveSystemPackages(): Seq[String] = {
    val method = PveManager.getClass.getDeclaredMethod("resolveSystemPackages")
    method.setAccessible(true)
    method.invoke(PveManager).asInstanceOf[Seq[String]]
  }

  /**
    * A computing-unit id whose venv directory does not exist on this machine.
    * PveManager.getEnvironments lists /tmp/texera-pve/venvs/<cuid> directly, so a fixed id
    * could pick up environments left behind by an earlier local run.
    */
  private def unusedCuid(): Int = {
    val venvRoot = Paths.get("/tmp/texera-pve/venvs")
    Iterator
      .continually(900000 + scala.util.Random.nextInt(90000))
      .find(cuid => !Files.exists(venvRoot.resolve(cuid.toString)))
      .get
  }

  "PveManager" should "create a new PVE and list it" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val logs = queueText()

    logs should not include "[PVE][ERR]"
    logs should include(s"[PVE] Created new environment for cuid = $testCuid")

    val pvePath = testRoot.resolve(testPveName).resolve("pve")
    val pythonPath = pvePath.resolve("bin").resolve("python")
    val pipPath = pvePath.resolve("bin").resolve("pip")

    Files.exists(pvePath) shouldBe true
    Files.exists(pythonPath) shouldBe true
    Files.exists(pipPath) shouldBe true

    PveManager.getEnvironments(testCuid).map(_.pveName) should contain(testPveName)
  }

  "PveManager" should "install a user package and list it for the PVE" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val packageName = "colorama"
    val packageVersion = "0.4.6"
    val packageSpec = s"$packageName==$packageVersion"

    queue.clear()

    PveManager.installUserPackages(
      List(packageSpec),
      testCuid,
      queue,
      testPveName
    )

    val logs = queueText()

    logs should not include "[PVE][ERR]"
    logs should include(s"[PVE] Installing package: $packageSpec")
    logs should include(s"[user-package] $packageSpec")

    val pve = PveManager
      .getEnvironments(testCuid)
      .find(_.pveName == testPveName)

    pve should not be empty
    pve.get.userPackages should contain(packageSpec)
  }

  "PveManager" should "delete a user package and remove it from the PVE package list" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val packageName = "colorama"
    val packageVersion = "0.4.6"
    val packageSpec = s"$packageName==$packageVersion"

    queue.clear()

    PveManager.installUserPackages(
      List(packageSpec),
      testCuid,
      queue,
      testPveName
    )

    PveManager
      .getEnvironments(testCuid)
      .find(_.pveName == testPveName)
      .get
      .userPackages should contain(packageSpec)

    val deleteLogs = PveManager.deletePackages(
      testCuid,
      packageName,
      testPveName
    )

    deleteLogs.mkString("\n") should not include "[PVE][ERR]"
    deleteLogs.mkString("\n") should include(s"[PVE] Uninstalled $packageName successfully")

    val pve = PveManager
      .getEnvironments(testCuid)
      .find(_.pveName == testPveName)

    pve should not be empty
    pve.get.userPackages should not contain packageSpec
  }

  "PveManager" should "report an error when venv creation fails" in {
    expectProcessCalls()
    venvExit = 1

    PveManager.createNewPve(testCuid, queue, testPveName)

    val logs = queueText()
    logs should include("[PVE][ERR] Failed to create venv")
    Files.exists(testRoot.resolve(testPveName).resolve("pve")) shouldBe false
  }

  it should "report an error when the system requirements install fails" in {
    expectProcessCalls()
    installExit = 1

    PveManager.createNewPve(testCuid, queue, testPveName)

    val logs = queueText()
    logs should include("[PVE][ERR] Failed to install requirements files")
  }

  it should "refuse to install a package that is part of the system set" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)
    queue.clear()

    PveManager.installUserPackages(List("pyarrow==23.0.1"), testCuid, queue, testPveName)

    val logs = queueText()
    logs should include("[PVE][ERR] pyarrow==23.0.1 is a system package")

    PveManager
      .getEnvironments(testCuid)
      .find(_.pveName == testPveName)
      .get
      .userPackages should not contain "pyarrow==23.0.1"
  }

  it should "report an error when a user package install fails" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)
    installExit = 1
    queue.clear()

    PveManager.installUserPackages(List("colorama==0.4.6"), testCuid, queue, testPveName)

    val logs = queueText()
    logs should include("[PVE][ERR] Failed to install package: colorama==0.4.6")

    PveManager
      .getEnvironments(testCuid)
      .find(_.pveName == testPveName)
      .get
      .userPackages should not contain "colorama==0.4.6"
  }

  "PveManager" should "delete all PVEs for a computing unit" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    Files.exists(testRoot.resolve(testPveName)) shouldBe true

    PveManager.deleteEnvironments(testCuid)

    Files.exists(testRoot) shouldBe false
    PveManager.getEnvironments(testCuid) shouldBe empty
  }

  "PveManager.getPythonBin" should "return Some for an existing venv" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val result = PveManager.getPythonBin(testCuid, testPveName)
    result shouldBe defined
    result.get.toString should endWith(s"$testPveName/pve/bin/python")
  }

  it should "return None when the venv does not exist" in {
    PveManager.getPythonBin(testCuid, "no-such-env") shouldBe None
  }

  it should "reject pveNames containing a path separator" in {
    // Both names carry a '/', so SafePveName rejects them before any path is built.
    PveManager.getPythonBin(testCuid, "../../../etc") shouldBe None
    PveManager.getPythonBin(testCuid, "foo/bar") shouldBe None
  }

  it should "return None for a dot-only pveName, which has no venv" in {
    // This does NOT exercise the `!resolved.startsWith(root)` guard in
    // getPythonBin. SafePveName's character class admits '.', so ".." is accepted,
    // and <VenvRoot>/<cuid>/../pve normalises to <VenvRoot>/pve — still under the
    // root. The None below therefore comes from the Files.exists check, exactly as
    // it does for any other name with no venv on disk.
    //
    // That guard is unreachable by construction rather than untested: SafePveName
    // forbids '/', so no accepted name can leave the root, and cuid is an Int.
    // Deleting the guard leaves this spec entirely green, so no assertion here can
    // pin it; making it reachable (or dropping it) is a production decision.
    PveManager.getPythonBin(testCuid, "..") shouldBe None
  }

  it should "reject pveNames with disallowed characters" in {
    PveManager.getPythonBin(testCuid, "") shouldBe None
    PveManager.getPythonBin(testCuid, "name with spaces") shouldBe None
    PveManager.getPythonBin(testCuid, "name;rm") shouldBe None
  }

  "PveManager.savePve + listPvesForUser" should "round-trip a row for the owning user" in {
    val veid = PveManager.savePve(testUid, "env-a", """{"numpy":"==1.26.0"}""")
    veid should be > 0

    val rows = PveManager.listPvesForUser(testUid)
    rows.map(_.name) should contain("env-a")
    val row = rows.find(_.veid == veid).get
    row.name shouldBe "env-a"
    row.packagesJson should include(""""numpy"""")
    row.packagesJson should include(""""==1.26.0"""")
  }

  "PveManager.updatePve" should "mutate an owned row and refuse rows owned by someone else" in {
    val veid = PveManager.savePve(testUid, "env-b", "{}")

    PveManager.updatePve(veid, testUid, "env-b-renamed", """{"pandas":""}""") shouldBe true

    val updated = PveManager.listPvesForUser(testUid).find(_.veid == veid).get
    updated.name shouldBe "env-b-renamed"
    updated.packagesJson should include(""""pandas"""")

    val otherUid = testUid + 1
    PveManager.updatePve(veid, otherUid, "hijacked", "{}") shouldBe false
    PveManager.listPvesForUser(testUid).find(_.veid == veid).get.name shouldBe "env-b-renamed"
  }

  "PveManager.deletePveFromDb" should "remove an owned row and return false for missing veids" in {
    val veid = PveManager.savePve(testUid, "env-c", "{}")

    PveManager.deletePveFromDb(veid, testUid) shouldBe true
    PveManager.listPvesForUser(testUid).map(_.veid) should not contain veid

    PveManager.deletePveFromDb(veid, testUid) shouldBe false
    PveManager.deletePveFromDb(-1, testUid) shouldBe false
  }

  // Builds a SessionUser carrying testUid so resource-layer methods can read
  // the owning user without going through real JWT auth.
  private def sessionUser: SessionUser = {
    val user = new User
    user.setUid(testUid)
    new SessionUser(user)
  }

  "PveResource.listPves" should "return every row owned by the current user" in {
    PveManager.savePve(testUid, "env-1", """{"numpy":"==1.26.0"}""")
    PveManager.savePve(testUid, "env-2", "{}")

    val items = new PveResource().listPves(sessionUser).asScala
    items.map(_.name).toSet shouldBe Set("env-1", "env-2")
  }

  "PveResource.savePve" should "create a new row and return 201" in {
    val resp =
      new PveResource().savePve(SavePvePayload("env-new", Map("numpy" -> "==1.26.0")), sessionUser)
    resp.getStatus shouldBe Response.Status.CREATED.getStatusCode
  }

  it should "return 400 for an invalid name" in {
    val resp =
      new PveResource().savePve(SavePvePayload("bad name with spaces", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  it should "return 409 when the user already has an env with that name" in {
    PveManager.savePve(testUid, "env-dup", "{}")
    val resp = new PveResource().savePve(SavePvePayload("env-dup", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.CONFLICT.getStatusCode
  }

  "PveResource.updatePve" should "rename an owned row and return 200" in {
    val veid = PveManager.savePve(testUid, "env-original", "{}")
    val resp =
      new PveResource().updatePve(veid, SavePvePayload("env-renamed", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "return 400 for an invalid name" in {
    val resp = new PveResource().updatePve(1, SavePvePayload("bad name", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  it should "return 404 for a veid the user doesn't own" in {
    val resp = new PveResource().updatePve(-1, SavePvePayload("env-x", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.NOT_FOUND.getStatusCode
  }

  it should "return 409 when renaming onto a name the user already uses" in {
    PveManager.savePve(testUid, "env-existing", "{}")
    val target = PveManager.savePve(testUid, "env-other", "{}")
    val resp =
      new PveResource().updatePve(target, SavePvePayload("env-existing", Map.empty), sessionUser)
    resp.getStatus shouldBe Response.Status.CONFLICT.getStatusCode
  }

  "PveResource.deletePveFromDb" should "delete an owned row and return 204" in {
    val veid = PveManager.savePve(testUid, "env-todelete", "{}")
    val resp = new PveResource().deletePveFromDb(veid, sessionUser)
    resp.getStatus shouldBe Response.Status.NO_CONTENT.getStatusCode
  }

  it should "return 404 for a veid the user doesn't own" in {
    val resp = new PveResource().deletePveFromDb(-1, sessionUser)
    resp.getStatus shouldBe Response.Status.NOT_FOUND.getStatusCode
  }

  "PveResource.getSystemPackages" should "wrap the manager's list under a 'system' key" in {
    // PveManager.systemPackages is a lazy val resolved via a (mocked) `pip freeze`,
    // so allow the process call in case this test is the first to force resolution.
    expectProcessCalls()
    val result = new PveResource().getSystemPackages
    result.keySet.asScala shouldBe Set("system")
    result.get("system") shouldBe PveManager.getSystemPackages.toList.asJava
  }

  "PveResource.fetchPVEs" should "return 400 when the cuid query parameter is missing" in {
    val resp = new PveResource().fetchPVEs(null)
    resp.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  it should "return the environments of a computing unit" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val resp = new PveResource().fetchPVEs(Int.box(testCuid))
    resp.getStatus shouldBe Response.Status.OK.getStatusCode
    val pves = resp.getEntity.asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala
    pves.map(_.get("pveName")) should contain(testPveName)
  }

  it should "return an empty list for a computing unit with no environments" in {
    // getEnvironments reads /tmp/texera-pve/venvs/<cuid> straight off disk, so use a cuid
    // that cannot collide with leftovers from an earlier local run.
    val resp = new PveResource().fetchPVEs(Int.box(unusedCuid()))

    resp.getStatus shouldBe Response.Status.OK.getStatusCode
    resp.getEntity.asInstanceOf[java.util.List[_]].asScala shouldBe empty
  }

  "PveResource.deleteEnvironments" should "remove every environment of the computing unit" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)
    PveManager.getEnvironments(testCuid).map(_.pveName) should contain(testPveName)

    new PveResource().deleteEnvironments(testCuid)

    PveManager.getEnvironments(testCuid) shouldBe empty
  }

  it should "be a no-op for a computing unit that has none" in {
    noException should be thrownBy new PveResource().deleteEnvironments(unusedCuid())
  }

  "PveResource.deletePackage" should "return 200 when the uninstall succeeds" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)
    PveManager.installUserPackages(List("colorama==0.4.6"), testCuid, queue, testPveName)

    val resp = new PveResource().deletePackage(testCuid, testPveName, "colorama")
    resp.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "return 400 when the package is part of the system set" in {
    expectProcessCalls()
    PveManager.createNewPve(testCuid, queue, testPveName)

    val resp = new PveResource().deletePackage(testCuid, testPveName, "pyarrow")
    resp.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  // ─── duplicate-name conflicts ──────────────────────────────────────────────
  // The unique index on (uid, name) is what surfaces a duplicate as SQLSTATE 23505,
  // so these drive real constraint violations rather than mocking the DAO.
  // The resources' 500 handlers are not covered here: PveManager.getSystemPackages
  // returns a cached value and never throws, and the generic `case e: Exception` arms
  // would need the DAO mocked out to reach.

  "PveResource.savePve" should "return 409 when the user already has an environment with that name" in {
    PveManager.savePve(testUid, "env-dup", "{}")

    val resp = new PveResource().savePve(SavePvePayload("env-dup", Map.empty), sessionUser)

    resp.getStatus shouldBe Response.Status.CONFLICT.getStatusCode
    resp.getEntity shouldBe """An environment named "env-dup" already exists."""
  }

  it should "still accept the same name for a different user" in {
    val otherUid = testUid + 1
    val otherUser = new User
    otherUser.setUid(otherUid)
    otherUser.setName(s"pve_other_$otherUid")
    otherUser.setEmail(s"other_${UUID.randomUUID()}@example.com")
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(otherUser)
    try {
      PveManager.savePve(otherUid, "env-shared", "{}")

      val resp = new PveResource().savePve(SavePvePayload("env-shared", Map.empty), sessionUser)

      resp.getStatus shouldBe Response.Status.CREATED.getStatusCode
    } finally {
      getDSLContext
        .deleteFrom(VIRTUAL_ENVIRONMENTS)
        .where(VIRTUAL_ENVIRONMENTS.UID.eq(otherUid))
        .execute()
      userDao.deleteById(otherUid)
    }
  }

  "PveResource.listPves" should "return an empty list when the user owns nothing" in {
    new PveResource().listPves(sessionUser).asScala shouldBe empty
  }

  /*
   * PveManager's two pure guards. Everything above reaches them incidentally through the
   * create/install flows; these take each conjunct's untaken side directly, which is what the
   * partially-covered branch arms on this file are.
   */
  "PveManager.isValidPveName" should "reject a null name" in {
    PveManager.isValidPveName(null) shouldBe false
  }

  it should "reject a name longer than 128 characters" in {
    PveManager.isValidPveName("a" * 129) shouldBe false
    // The boundary itself is allowed.
    PveManager.isValidPveName("a" * 128) shouldBe true
  }

  it should "reject a name with characters outside the safe set" in {
    PveManager.isValidPveName("has space") shouldBe false
    PveManager.isValidPveName("has/slash") shouldBe false
    PveManager.isValidPveName("") shouldBe false
  }

  it should "accept a name of safe characters" in {
    PveManager.isValidPveName("env-1.2_3") shouldBe true
  }

  "PveManager.getPythonBin" should "refuse a name outside the safe set without touching the disk" in {
    PveManager.getPythonBin(testCuid, "../escape") shouldBe None
  }

  it should "return nothing when the interpreter has not been created" in {
    PveManager.getPythonBin(testCuid, testPveName) shouldBe None
  }

  it should "return nothing when the interpreter exists but is not executable" in {
    val python = pythonBinFor(testPveName)
    Files.createDirectories(python.getParent)
    Files.write(python, Array.emptyByteArray)
    python.toFile.setExecutable(false)
    // Clearing the bit is not something every filesystem can represent (Windows ACLs, a
    // root user, some mount options). Assert the state this test needs and cancel rather
    // than fail where the platform cannot produce it.
    assume(!Files.isExecutable(python), "filesystem cannot represent a non-executable file")

    PveManager.getPythonBin(testCuid, testPveName) shouldBe None
  }

  it should "return the interpreter once it exists and is executable" in {
    val python = pythonBinFor(testPveName)
    Files.createDirectories(python.getParent)
    Files.write(python, Array.emptyByteArray)
    python.toFile.setExecutable(true)
    // Likewise for the other direction: a noexec mount would keep the bit off.
    assume(Files.isExecutable(python), "filesystem cannot represent an executable file")

    PveManager.getPythonBin(testCuid, testPveName) shouldBe Some(python.toAbsolutePath.normalize())
  }

  /*
   * `deletePackages`' missing-interpreter guard. Everything above reaches it only from the
   * other side, with a venv already fabricated.
   *
   * The guard answers before the method reads `systemPackageNames`, which is what makes it
   * safe to drive here: `systemPackages` is a lazy val resolved once per JVM through a
   * `pip freeze`, so a test that forced it with the wrong runner installed would fix the
   * system set for every suite that follows. This test sets no `runProcessMock`
   * expectation, so an unexpected process spawn fails it rather than escaping to real pip —
   * which is also what keeps "stop before the system-package check" honest.
   *
   * `installUserPackages`' matching guard is NOT tested here: PveWebsocketResourceSpec's
   * install test already drives it end to end (its cuid has no venv on any platform), so a
   * copy here would only re-cover lines that spec already owns.
   */
  "PveManager.deletePackages" should "report a missing interpreter and stop before the system-package check" in {
    val absent = s"$testPveName-absent"

    val output = PveManager.deletePackages(testCuid, "colorama", absent)

    output should have size 1
    output.head shouldBe
      s"[PVE][ERR] Python executable not found for PVE: ${pythonBinFor(absent).toAbsolutePath}"
  }

  /*
   * The uninstall's two unhappy endings. Both are about the same thing: the recorded package
   * list is a claim about what is installed in the venv, so it may only change when pip
   * actually removed something.
   */
  it should "report the failure and leave the recorded package list alone when pip uninstall exits non-zero" in {
    expectProcessCalls()
    fabricatePve(testPveName)
    val metadata = userPackagesFile(testPveName)
    Files.write(metadata, Seq("colorama==0.4.6").asJava)
    uninstallExit = 1

    val output = PveManager.deletePackages(testCuid, "colorama", testPveName)

    output should contain("[PVE][ERR] Failed to uninstall package: colorama")
    output should not contain "[PVE] Uninstalled colorama successfully"
    // colorama is still in the venv, so the manifest has to keep saying so.
    Files.readAllLines(metadata).asScala should contain("colorama==0.4.6")

    // What was actually handed to pip. `PIP_NO_INPUT=1` is set in pipEnv, so an
    // uninstall missing `-y` aborts instead of prompting — dropping it would break
    // every user package deletion while leaving every exit-code assertion above green,
    // because the fake runner answers on the command's *shape*, not its argv.
    val uninstall = recordedCommands.last
    uninstall should contain("uninstall")
    uninstall should contain("-y")
    uninstall.last shouldBe "colorama"
    uninstall.head shouldBe pythonBinFor(testPveName).toAbsolutePath.toString

    // pip's own output is the payload PveResource.deletePackage hands back to the UI;
    // both streams have to survive the trip, or "report the failure" reports nothing
    // but PveManager's own generic wrapper line.
    output should contain("[pip] mock uninstall")
    output should contain("[pip][ERR] ERROR: Cannot uninstall colorama")
  }

  it should "return an error list rather than throwing when the uninstall cannot be spawned" in {
    expectProcessCalls()
    fabricatePve(testPveName)
    uninstallThrows = true

    val output = PveManager.deletePackages(testCuid, "colorama", testPveName)

    // The caller is a JAX-RS resource that turns this list into a 200/400; an escaping
    // exception would reach it as a 500 with no message about which PVE failed.
    output should have size 1
    output.head should include(s"cuid=$testCuid")
    output.head should include("boom")
  }

  /*
   * The other half of what the resolved system set is for. Refusing to install a package
   * whose name collides with a system one is covered above; this is the constraint file,
   * which is how the resolved *versions* reach pip. Without it a user install is free to
   * pull a different pyarrow in as a transitive dependency of something else and break the
   * Python workers, and the install still reports success.
   */
  "PveManager.installUserPackages" should "pin the install against the resolved system versions" in {
    expectProcessCalls()
    fabricatePve(testPveName)

    PveManager.installUserPackages(List("colorama==0.4.6"), testCuid, queue, testPveName)

    val install = recordedCommands.last
    install should contain("--constraint")

    // pip reads the path as the argument of the flag, so the two travel together and in
    // this order; handing it the package name instead is still a well-formed command line.
    val constraintFile = Paths.get(install(install.indexOf("--constraint") + 1))

    // Equality, against the same sanitised set the resolver is required to produce: this
    // file is where those lines end up, and it is the reason the blank and `## FIXME:`
    // lines of `systemFreeze` may not survive resolution — pip aborts the whole install
    // on a constraint line it cannot parse.
    Files.readAllLines(constraintFile).asScala.toSeq shouldBe expectedSystemPackages
  }

  /*
   * `resolveSystemPackages`' three give-up arms.
   *
   * The resolved set is what stops a user from installing a package that shadows one the
   * Python workers depend on, so what matters at each failure is that the resolver stops
   * there rather than carrying a half-built answer forward: no pip install into a venv that
   * was never created, no `pip freeze` of a venv whose install failed, and no partial freeze
   * promoted to "the system set".
   *
   * These assert the giving up, not that an empty set is a good answer to give the rest of
   * the app — an empty set is what `systemPackageNames` and `--constraint` both degrade to,
   * which is fail-open. That is a property of the caller, and it is not pinned here.
   */
  "PveManager's system-package resolution" should "give up without attempting the install when the throwaway venv cannot be created" in {
    expectProcessCalls()
    venvExit = 1

    resolveSystemPackages() shouldBe empty

    recordedCommands should have size 1
    recordedCommands.head should contain("venv")
  }

  it should "give up without freezing when the requirements install fails" in {
    expectProcessCalls()
    installExit = 1

    resolveSystemPackages() shouldBe empty

    recordedCommands should have size 2
    recordedCommands.exists(_.contains("freeze")) shouldBe false

    // The install has to go into the throwaway venv, whose directory is the last argument
    // of the create that preceded it. Aiming it at the system interpreter instead still
    // produces two commands in the right order and still returns empty here, so the count
    // above cannot tell the two apart — and getting it wrong would pip-install
    // amber/requirements.txt straight into the machine's Python.
    recordedCommands(1).head should startWith(recordedCommands.head.last)

    // The throwaway venv is a real directory tree by this point — the create step above
    // succeeded, so it holds the interpreter the fake runner fabricated, as a real
    // `python -m venv` would. Giving up early must still take it away with it: nothing
    // revisits the directory afterwards, and its name is a fresh temp path each time, so
    // whatever is left there is left for good.
    Files.exists(Paths.get(recordedCommands.head.last)) shouldBe false
  }

  it should "discard what a failing pip freeze already printed" in {
    expectProcessCalls()

    // Control: with the freeze succeeding, this same fixture does produce a system set. Without
    // it, the assertion below would hold just as well for a resolver that returned whatever the
    // freeze printed, because a fixture that printed nothing would look identical.
    //
    // Pinned as an equality, not a `contain`: `systemFreeze` deliberately includes a blank
    // line and a `## FIXME:` comment, and the exact answer is what says those are dropped
    // and the surviving line is trimmed. A `contain` would pass on a resolver that handed
    // the raw freeze output through into the `--constraint` file.
    resolveSystemPackages() shouldBe expectedSystemPackages

    // The freeze has to interrogate the throwaway venv, not whatever interpreter is on PATH:
    // freezing the system Python would report that machine's packages as "the system set".
    recordedCommands.last.head should startWith(recordedCommands.head.last)

    recordedCommands.clear()
    freezeExit = 1

    // The mock still emits pyarrow==23.0.1 before reporting the failure, so a resolver that
    // kept the collected lines would answer with a system set of one.
    resolveSystemPackages() shouldBe empty
    recordedCommands.last should contain("freeze")
  }

}
