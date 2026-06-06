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

package org.apache.texera.amber.engine.common

import com.twitter.util.{
  Await => TwitterAwait,
  Future => TwitterFuture,
  TimeoutException => TwitterTimeoutException
}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.common.FutureBijection._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future => ScalaFuture}

class EngineCommonConversionsSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Suite-local ActorSystem for Pekko Serialization (SerializedState section)
  // ---------------------------------------------------------------------------
  //
  // Owned by this suite and shut down in afterAll so no Pekko threads outlive
  // the run. Matches the pattern used by CheckpointSubsystemSpec.

  private val testSystem: ActorSystem =
    ActorSystem("EngineCommonConversionsSpec-test", AmberRuntime.pekkoConfig)
  private val testSerde: Serialization = SerializationExtension(testSystem)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // FutureBijection — Twitter Future → Scala Future
  // ---------------------------------------------------------------------------

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val awaitTimeout: FiniteDuration = 5.seconds

  "FutureBijection.RichTwitterFuture.asScala" should
    "resolve the Scala Future with the value of a Twitter Return" in {
    val tf: TwitterFuture[Int] = TwitterFuture.value(42)
    val sf: ScalaFuture[Int] = tf.asScala
    assert(Await.result(sf, awaitTimeout) == 42)
  }

  it should "fail the Scala Future with the exception of a Twitter Throw" in {
    val boom = new RuntimeException("boom")
    val tf: TwitterFuture[Int] = TwitterFuture.exception(boom)
    val sf: ScalaFuture[Int] = tf.asScala
    val caught = intercept[RuntimeException] {
      Await.result(sf, awaitTimeout)
    }
    assert(caught eq boom, "the original exception must be preserved without rewrapping")
  }

  it should "propagate completion asynchronously (Scala Future is not pre-resolved)" in {
    // A Twitter Promise resolved later must propagate to the Scala Future
    // only when the Promise completes. Pin so a regression that pre-
    // resolves on conversion breaks here.
    val tp = com.twitter.util.Promise[String]()
    val sf: ScalaFuture[String] = (tp: TwitterFuture[String]).asScala
    assert(!sf.isCompleted, "Scala Future must not pre-resolve before the Twitter source completes")
    tp.setValue("late")
    assert(Await.result(sf, awaitTimeout) == "late")
  }

  // ---------------------------------------------------------------------------
  // FutureBijection — Scala Future → Twitter Future
  // ---------------------------------------------------------------------------

  "FutureBijection.RichScalaFuture.asTwitter" should
    "resolve the Twitter Future with the value of a Scala Future.successful" in {
    val sf: ScalaFuture[Int] = ScalaFuture.successful(7)
    val tf: TwitterFuture[Int] = sf.asTwitter()
    assert(TwitterAwait.result(tf, com.twitter.util.Duration.fromSeconds(5)) == 7)
  }

  it should "fail the Twitter Future with the exception of a Scala Future.failed" in {
    val boom = new IllegalStateException("nope")
    val sf: ScalaFuture[Int] = ScalaFuture.failed(boom)
    val tf: TwitterFuture[Int] = sf.asTwitter()
    val caught = intercept[IllegalStateException] {
      TwitterAwait.result(tf, com.twitter.util.Duration.fromSeconds(5))
    }
    assert(caught eq boom, "the original exception must be preserved without rewrapping")
  }

  it should "propagate completion asynchronously (Twitter Future is not pre-resolved)" in {
    val sp = scala.concurrent.Promise[String]()
    val tf: TwitterFuture[String] = sp.future.asTwitter()
    assert(!tf.isDefined, "Twitter Future must not pre-resolve before the Scala source completes")
    sp.success("late")
    assert(TwitterAwait.result(tf, com.twitter.util.Duration.fromSeconds(5)) == "late")
  }

  it should "leave a never-completing Scala Future un-resolved on the Twitter side" in {
    // A Promise that never completes must NOT silently resolve the Twitter
    // Future. Pin via a TwitterFutureTimeoutException from a short Await.
    val sp = scala.concurrent.Promise[Int]()
    val tf: TwitterFuture[Int] = sp.future.asTwitter()
    intercept[TwitterTimeoutException] {
      TwitterAwait.result(tf, com.twitter.util.Duration.fromMilliseconds(50))
    }
  }

  // ---------------------------------------------------------------------------
  // SerializedState — key constants
  // ---------------------------------------------------------------------------

  "SerializedState key constants" should "expose the documented Amber payload-keys" in {
    // Pin the wire-format keys so a future renaming surfaces here — any
    // checkpoint files written under the old keys would otherwise be
    // silently unreadable.
    assert(SerializedState.CP_STATE_KEY == "Amber_CPState")
    assert(SerializedState.DP_STATE_KEY == "Amber_DPState")
    assert(SerializedState.IN_FLIGHT_MSG_KEY == "Amber_Inflight_Messages")
    assert(SerializedState.DP_QUEUED_MSG_KEY == "Amber_DP_Queued_Messages")
    assert(SerializedState.OUTPUT_MSG_KEY == "Amber_Output_Messages")
  }

  // ---------------------------------------------------------------------------
  // SerializedState.fromObject / toObject round-trip
  // ---------------------------------------------------------------------------
  //
  // The companion's `fromObject` captures the serializer id and manifest at
  // serialize time, and the case-class's `toObject` re-resolves the
  // serializer from those captured bits on deserialize. The two halves must
  // round-trip a value-equal object.

  // Realistic payload kinds Amber stores:
  // - a small case class (defined at top level on the companion below to
  //   avoid capturing the enclosing test scope, which would fail Kryo's
  //   "Closure must implement java.io.Serializable" check)
  // - a primitive box (java.lang.Integer)
  // - a Map[String, Int] (engine-common payload shape)

  import EngineCommonConversionsSpec.Payload

  "SerializedState.fromObject" should "capture bytes, serializerId, and manifest" in {
    val s = SerializedState.fromObject(Payload("alpha", 1), testSerde)
    assert(s.bytes.nonEmpty)
    // serializerId is whatever Pekko's serializer registry assigns; we just
    // assert the field is populated (default Java serializer id is 1 in
    // out-of-the-box Pekko config).
    assert(s.serializerId >= 0)
    // manifest is permitted to be empty for non-Pekko-aware serializers,
    // so we don't assert on its value beyond non-null.
    assert(s.manifest != null)
  }

  "SerializedState round-trip" should "preserve a case-class payload" in {
    val original = Payload("alpha", 1)
    val s = SerializedState.fromObject(original, testSerde)
    val restored = s.toObject[Payload](testSerde)
    assert(restored == original)
  }

  it should "preserve a boxed primitive payload" in {
    val original: java.lang.Integer = Int.box(42)
    val s = SerializedState.fromObject(original, testSerde)
    val restored = s.toObject[java.lang.Integer](testSerde)
    assert(restored == original)
  }

  it should "preserve a Map[String, Int] payload" in {
    // Deserialize into the trait `immutable.Map[String, Int]`, not the
    // concrete `HashMap` impl — the serializer is allowed to restore a
    // different (still value-equal) immutable Map implementation, and
    // we are pinning value preservation, not the concrete class.
    val original: scala.collection.immutable.Map[String, Int] =
      scala.collection.immutable.HashMap("x" -> 1, "y" -> 2, "z" -> 3)
    val s = SerializedState.fromObject(original, testSerde)
    val restored = s.toObject[scala.collection.immutable.Map[String, Int]](testSerde)
    assert(restored == original)
  }

  "SerializedState.size" should "equal bytes.length" in {
    val s = SerializedState.fromObject(Payload("alpha", 1), testSerde)
    assert(s.size() == s.bytes.length.toLong)
  }

  it should "return zero when the payload serializes to an empty byte array" in {
    // Pin the trivial case: an empty SerializedState reports size 0.
    val empty = SerializedState(Array.empty[Byte], serializerId = 0, manifest = "")
    assert(empty.size() == 0L)
    assert(empty.bytes.isEmpty)
  }
}

object EngineCommonConversionsSpec {
  // Top-level case class so Kryo doesn't try to serialize a captured
  // outer reference (Scala's nested case class otherwise captures the
  // enclosing instance, which fails Kryo's "Closure must implement
  // java.io.Serializable" check on our test ExecutionContext).
  final case class Payload(name: String, n: Int)
}
