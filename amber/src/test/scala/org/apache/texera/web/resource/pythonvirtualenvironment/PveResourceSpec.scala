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

  // What the mocked `pip freeze` reports as the resolved system set. pyarrow is
  // always a hard dependency in amber/requirements.txt, so it stands in for "a
  // system package the user may neither install nor delete".
  private val systemFreeze = Seq("pyarrow==23.0.1")

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
          0
        } else if (command.contains("uninstall")) {
          logger.out("mock uninstall")
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

  private def queueText(): String = {
    queue.iterator().asScala.toList.mkString("\n")
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

  it should "reject pveNames containing path-traversal segments" in {
    PveManager.getPythonBin(testCuid, "..") shouldBe None
    PveManager.getPythonBin(testCuid, "../../../etc") shouldBe None
    PveManager.getPythonBin(testCuid, "foo/bar") shouldBe None
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
    val resp = new PveResource().fetchPVEs(Int.box(testCuid + 1))

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
    noException should be thrownBy new PveResource().deleteEnvironments(testCuid + 1)
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

  // ─── conflict and error branches ───────────────────────────────────────────
  // The unique index on (uid, name) is what surfaces a duplicate as SQLSTATE 23505,
  // so these drive real constraint violations rather than mocking the DAO.

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

}
