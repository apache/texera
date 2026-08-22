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

import org.apache.commons.lang3.SystemUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import javax.websocket.{RemoteEndpoint, Session}
import scala.jdk.CollectionConverters._

/**
  * Unit tests for the PVE websocket endpoint.
  *
  * `onOpen` is the whole class: it reads the handshake parameters, kicks off the requested
  * action on one Future and a pump that drains the action's log queue to the socket on
  * another. Both Futures' bodies compile to lambdas, so JaCoCo's synthetic filter drops
  * them from the line count entirely — the 9 lines it does track here are the straight-line
  * handshake prologue, and it records no branch counter at all for this file. So the
  * contracts below are what these tests actually buy; the coverage number is not evidence
  * for any of them:
  *
  *   - the pump forwards every queued line in order, and stops on the `__DONE__` sentinel:
  *     it closes the socket and then leaves the loop rather than parking in `queue.take()`
  *     on a queue nothing will ever fill again,
  *   - the failure path is symmetric with the happy one — the catch arm reports the action's
  *     own exception message and the `finally` still emits the sentinel,
  *   - a malformed handshake is a failure of that same shape and not a special case: an
  *     absent, empty or blank `cuid` or `pveName`, or a `cuid` that is not an Int, produces
  *     an `[ERR]` line and then the sentinel. The reads have to stay inside the Future for
  *     that — hoisted back out of it they throw from `onOpen` itself, ahead of both the catch
  *     arm and the pump, and the client sees a socket that closes with nothing on it, and
  *   - `action` selects the branch, with the cuid and pveName parsed off the handshake
  *     handed to `PveManager` unswapped.
  *
  * Only two of the four things `action` can dispatch to are asserted (`install` and the
  * unknown-action arm). Known gaps, each stated as what survives a mutation rather than as
  * a merely uncovered path:
  *   - `action=create`, and the `create` default a missing `action` falls back to, are not
  *     exercised, because `PveManager.createNewPve` goes straight to the production process
  *     runner: the test would build a real venv and pip-install `amber/requirements.txt`.
  *     Faking the runner is not an option either — it is a JVM-global `var`, and
  *     `PveResourceSpec` captures it in its constructor, so a mock installed here can be
  *     laundered into that suite's idea of the real runner. `PveResourceSpec` owns the
  *     create path instead. Consequence: the `getOrDefault("action", List.of("create"))`
  *     default can be replaced with any garbage string and both tests still pass.
  *   - the `packages` query parameter's parse chain is unobservable, and mutations to it
  *     survive: `installUserPackages` reports the missing interpreter and returns before it
  *     ever reads the list, so replacing the parsed list with `Nil` — or inverting the
  *     `filter(_.nonEmpty)` — changes nothing any assertion can see. Pinning it needs the
  *     same fake runner.
  *   - the pump's guard is pinned at ENTRY ONLY. `while (!done && session.isOpen)` narrowed
  *     to `while (!done)` survives, because every session here is open for the whole
  *     exchange; killing it needs a test that enters the loop with an already-closed
  *     session, whose only available assertion ("nothing was sent") is a sleep against a
  *     Future.
  */
class PveWebsocketResourceSpec extends AnyFlatSpec with Matchers with MockFactory {

  /** Where `PveManager` looks for a PVE's interpreter on this platform. */
  private def pythonBinFor(cuid: Int, pveName: String): Path = {
    val venv = Paths.get("/tmp/texera-pve/venvs", cuid.toString, pveName, "pve")
    if (SystemUtils.IS_OS_WINDOWS) venv.resolve("Scripts").resolve("python.exe")
    else venv.resolve("bin").resolve("python")
  }

  /**
    * How many times correct code evaluates `session.isOpen`. The pump's guard is
    * `while (!done && session.isOpen)`, so for a stream of n lines terminated by the
    * sentinel it evaluates the session exactly n times — the (n+1)th pass short-circuits on
    * `!done` and never touches it. Every test here drives exactly two lines (one from the
    * action, then `__DONE__`) and asserts that length, so two is the whole budget; a test
    * that streams a different number of lines has to revisit this.
    */
  private val expectedGuardEvals = 2

  /**
    * A mocked `Session` plus the three observables the endpoint produces: the text it wrote,
    * the close that ends the exchange, and whether the pump went back to polling the session
    * after the sentinel.
    *
    * The endpoint writes from a Future, so the assertions have to wait for the close before
    * they read `sentLines` — that is also what keeps ScalaMock's end-of-test verification
    * from racing a call still in flight on the pump thread.
    */
  private class Fixture(
      val session: Session,
      private val sent: ConcurrentLinkedQueue[String],
      private val closed: CountDownLatch,
      private val extraGuardEval: CountDownLatch
  ) {
    def awaitClose(): Unit =
      withClue("the endpoint never closed the session: ") {
        closed.await(30, TimeUnit.SECONDS) shouldBe true
      }

    /**
      * An `isOpen` evaluation past the budget means the pump re-entered its guard after
      * writing the sentinel, i.e. `done` was never set. It then blocks in `queue.take()`
      * forever, because the action Future has already run its `finally` and nothing will
      * ever be enqueued again — one leaked global-EC thread per websocket connection, with
      * the socket closed and every assertion above still green. Call after `awaitClose()`.
      */
    def assertPumpStopped(): Unit =
      withClue("the pump kept polling the session after the sentinel: ") {
        extraGuardEval.await(500, TimeUnit.MILLISECONDS) shouldBe false
      }

    def sentLines: List[String] = sent.iterator().asScala.toList
  }

  private def fixture(params: (String, String)*): Fixture =
    fixtureOf(params.map { case (key, value) => key -> List(value).asJava }.toMap.asJava)

  private def fixtureOf(
      parameterMap: java.util.Map[String, java.util.List[String]]
  ): Fixture = {
    val sent = new ConcurrentLinkedQueue[String]()
    val closed = new CountDownLatch(1)
    val extraGuardEval = new CountDownLatch(1)
    val guardEvals = new AtomicInteger(0)

    val basic = mock[RemoteEndpoint.Basic]
    (basic
      .sendText(_: String))
      .expects(*)
      .onCall { (text: String) =>
        sent.add(text)
        ()
      }
      .anyNumberOfTimes()

    val session = mock[Session]
    (() => session.getRequestParameterMap).expects().returning(parameterMap).anyNumberOfTimes()
    (() => session.isOpen)
      .expects()
      .onCall { () =>
        if (guardEvals.incrementAndGet() > expectedGuardEvals) extraGuardEval.countDown()
        true
      }
      .anyNumberOfTimes()
    (() => session.getBasicRemote).expects().returning(basic).anyNumberOfTimes()
    (() => session.close())
      .expects()
      .onCall { () =>
        closed.countDown()
        ()
      }
      .anyNumberOfTimes()

    new Fixture(session, sent, closed, extraGuardEval)
  }

  "PveWebsocketResource.onOpen" should "report an unknown action and close on the sentinel" in {
    val f = fixture(
      "cuid" -> "424242",
      "pveName" -> "ws-unknown-env",
      "action" -> "explode"
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    // Order and length both matter: the pump must forward the action's own line first and
    // close only once it has also written the sentinel.
    f.sentLines shouldBe List("[ERR] Unknown action: explode", "__DONE__")
    f.assertPumpStopped()
  }

  it should "route action=install to PveManager with the cuid and pveName from the handshake" in {
    val cuid = 313131
    val pveName = "ws-install-env"

    // No `packages` parameter on purpose: whatever this test passed would be unobservable
    // (see the header), and the `getOrDefault` default takes over. Its absence is the honest
    // signal — version-pinned literals here would imply a contract nothing pins.
    val f = fixture(
      "cuid" -> cuid.toString,
      "pveName" -> pveName,
      "action" -> "install"
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    // No venv exists for this cuid, so PveManager's missing-interpreter guard answers. That
    // message carries the interpreter path it derived, which is the only place the parsed
    // cuid and pveName become observable — hence two values that cannot be mistaken for
    // each other.
    f.sentLines should have size 2
    f.sentLines.head shouldBe
      s"[PVE][ERR] Python executable not found for PVE: ${pythonBinFor(cuid, pveName).toAbsolutePath}"
    f.sentLines.last shouldBe "__DONE__"
    f.assertPumpStopped()
  }

  it should "report the action's own exception and still send the sentinel" in {
    // `packages` is present but EMPTY, so `getOrDefault` hands back that empty list and the
    // `.get(0)` on it throws inside the Future. That is the cheapest reachable failure: it
    // needs no fake process runner, and it throws before `PveManager` is entered at all, so
    // the memoized `systemPackages` is never forced.
    val f = fixtureOf(
      java.util.Map.of(
        "cuid",
        java.util.List.of("424243"),
        "pveName",
        java.util.List.of("ws-boom"),
        "action",
        java.util.List.of("install"),
        "packages",
        java.util.List.of[String]()
      )
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    f.sentLines should have size 2
    // The catch arm has to report the actual exception message (not a fixed apology).
    // IndexOutOfBoundsException message wording varies across JDKs/collections, so match loosely.
    f.sentLines.head should (startWith("[ERR] Index") and include("0"))
    // And the `finally` has to emit the sentinel on the failure path too, or the client waits
    // for an end-of-stream that never arrives.
    f.sentLines.last shouldBe "__DONE__"
    f.assertPumpStopped()
  }

  it should "report a handshake with no cuid rather than dying on the socket" in {
    // The frontend always sends `cuid`, so this is a hand-rolled or stale handshake. With the
    // read hoisted out of the Future the absent key threw a NullPointerException from `onOpen`
    // -- ahead of both the catch arm and the pump -- so the client got no error line, no
    // sentinel, and nothing to tell this apart from a socket that simply went away.
    val f = fixture(
      "pveName" -> "ws-no-cuid",
      "action" -> "install"
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    f.sentLines shouldBe List("[ERR] Missing required parameter: cuid", "__DONE__")
    f.assertPumpStopped()
  }

  it should "report a cuid whose value list is empty" in {
    // A present-but-empty value list is the other half of the presence guard: `.get(0)` on it
    // raises IndexOutOfBoundsException rather than the NullPointerException the absent key
    // above raises, so dropping either half of `values == null || values.isEmpty` has to fail
    // one of these two tests.
    val f = fixtureOf(
      java.util.Map.of(
        "cuid",
        java.util.List.of[String](),
        "pveName",
        java.util.List.of("ws-empty-cuid"),
        "action",
        java.util.List.of("install")
      )
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    f.sentLines shouldBe List("[ERR] Missing required parameter: cuid", "__DONE__")
    f.assertPumpStopped()
  }

  it should "report a non-numeric cuid" in {
    // Present and non-blank but not an Int, so it fails past the presence guard, in the parse.
    // The message has to name the offending value: `NumberFormatException`'s own wording says
    // "For input string" and never mentions which parameter it came from.
    val f = fixture(
      "cuid" -> "not-a-number",
      "pveName" -> "ws-bad-cuid",
      "action" -> "install"
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    f.sentLines shouldBe List("[ERR] Invalid cuid: not-a-number", "__DONE__")
    f.assertPumpStopped()
  }

  it should "report a blank pveName, naming the parameter that is missing" in {
    // Blank is what a servlet container hands back for `?pveName=`, and it is as malformed as
    // an absent key: there is no PVE whose name is whitespace, and `PveManager` would resolve
    // it to a venv directory named "" under the user's. Asserting on pveName rather than cuid
    // also pins that the message interpolates the parameter's name instead of hardcoding one.
    val f = fixture(
      "cuid" -> "424244",
      "pveName" -> "   ",
      "action" -> "install"
    )

    new PveWebsocketResource().onOpen(f.session)
    f.awaitClose()

    f.sentLines shouldBe List("[ERR] Missing required parameter: pveName", "__DONE__")
    f.assertPumpStopped()
  }
}
