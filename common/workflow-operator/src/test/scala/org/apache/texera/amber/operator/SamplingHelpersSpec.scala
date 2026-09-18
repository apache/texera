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

package org.apache.texera.amber.operator

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

/** The transcribed java.util.Random, checked against what Java would answer.
  *
  * Only the generator is exercised here. What each sampling operator does with
  * the numbers is its own spec's business.
  */
class SamplingHelpersSpec extends AnyFlatSpec with Matchers {

  /** Run `body` after the helper class, and return what it printed. */
  private def runWithHelper(body: String): (Int, String) = {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    val script = Files.createTempFile("sampling-helpers-", ".py")
    script.toFile.deleteOnExit()
    Files.write(
      script,
      (SamplingHelpers.JavaRandom + "\n\n" + body).getBytes(StandardCharsets.UTF_8)
    )
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(60, TimeUnit.SECONDS)
    (process.exitValue(), out)
  }

  // A bound of 2**30 + 1 puts about half of the draws in the rejection zone, so
  // the very first one with this seed is a draw Java throws away. Java decides
  // that by letting `u - r + m` overflow an int; carrying the sum instead keeps
  // the discarded value and shifts every later draw by one, which is what these
  // five numbers pin. They are what java.util.Random answers for this seed.
  "SamplingHelpers.JavaRandom" should "reject the draws Java's overflow check rejects" in {
    val (exit, out) = runWithHelper(
      """r = _TexeraJavaRandom(1)
        |print([r.next_int((1 << 30) + 1) for _ in range(5)])
        |""".stripMargin
    )
    withClue(s"python said:\n$out") {
      exit shouldBe 0
      out.trim shouldBe "[215764588, 880641847, 874970313, 446064254, 77814904]"
    }
  }

  // java.util.Random#nextInt(int) opens with this check, and Reservoir Sampling
  // reaches it with a reservoir of zero: the engine ends the run there, so a
  // script that answered with an empty table instead would be reporting a result
  // the run never had.
  it should "refuse a bound that is not positive, the way Java does" in {
    val (exit, out) = runWithHelper(
      """r = _TexeraJavaRandom(1)
        |try:
        |    r.next_int(0)
        |    print("no error")
        |except ValueError as e:
        |    print("ValueError:", e)
        |""".stripMargin
    )
    withClue(s"python said:\n$out") {
      exit shouldBe 0
      out.trim shouldBe "ValueError: bound must be positive"
    }
  }

  // The power-of-two bound takes the other branch, which has no rejection at
  // all, so it would keep working even if the check above were dropped.
  it should "take the power-of-two shortcut without rejecting" in {
    val (exit, out) = runWithHelper(
      """r = _TexeraJavaRandom(42)
        |print(all(0 <= r.next_int(256) < 256 for _ in range(1000)))
        |""".stripMargin
    )
    withClue(s"python said:\n$out") {
      exit shouldBe 0
      out.trim shouldBe "True"
    }
  }

  private def resolvePython(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def runnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(runnable)
  }
}
