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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class CanonicalFixtureSpec extends AnyFlatSpec with Matchers {

  "CanonicalFixture" should "have at least 10 rows per port with partial id overlap" in {
    CanonicalFixture.port0Rows.size should be >= 10
    CanonicalFixture.port1Rows.size should be >= 10
    val ids0 = CanonicalFixture.port0Rows.map(_.getField[Integer]("id")).toSet
    val ids1 = CanonicalFixture.port1Rows.map(_.getField[Integer]("id")).toSet
    (ids0 intersect ids1) should not be empty
    (ids0 diff ids1) should not be empty
    (ids1 diff ids0) should not be empty
  }

  // The windows are a rule over the table's size rather than fixed indices, so
  // that a row added to the JSON is read by a port instead of falling off the
  // end. Fixed indices made that silent: the row was simply never fed to
  // anything, and every assertion above still held.
  it should "leave no row outside both ports" in {
    val onAPort = (CanonicalFixture.port0Rows ++ CanonicalFixture.port1Rows)
      .map(_.getField[Integer]("id"))
      .toSet
    onAPort shouldBe CanonicalFixture.allRows.map(_.getField[Integer]("id")).toSet
  }

  it should "contain the canonical value \"1\" in some but not all name cells" in {
    val names = CanonicalFixture.port0Rows.map(_.getField[String]("name"))
    names.count(_ == "1") should be > 0
    names.count(_ == "1") should be < names.size
  }

  it should "expose a valid OHLC block (high >= open/close >= low) for candlestick-style ops" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val o = t.getField[java.lang.Double]("open").doubleValue
      val h = t.getField[java.lang.Double]("high").doubleValue
      val l = t.getField[java.lang.Double]("low").doubleValue
      val c = t.getField[java.lang.Double]("close").doubleValue
      h should be >= math.max(o, c)
      l should be <= math.min(o, c)
    }
  }

  it should "keep pvalue strictly inside (0, 1) for probability-domain fields" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val p = t.getField[java.lang.Double]("pvalue").doubleValue
      p should (be > 0.0 and be < 1.0)
    }
  }

  it should "keep log2fc signed and centered (both a negative and a positive present)" in {
    val vals = CanonicalFixture.port0Rows.map(_.getField[java.lang.Double]("log2fc").doubleValue)
    vals.min should be < 0.0
    vals.max should be > 0.0
  }

  it should "keep ternary components strictly positive" in {
    CanonicalFixture.port0Rows.foreach { t =>
      t.getField[java.lang.Double]("comp_a").doubleValue should be > 0.0
      t.getField[java.lang.Double]("comp_b").doubleValue should be > 0.0
      t.getField[java.lang.Double]("comp_c").doubleValue should be > 0.0
    }
  }

  it should "expose uniq_name as globally distinct so name-keyed ops have no duplicates" in {
    val names = CanonicalFixture.port0Rows.map(_.getField[String]("uniq_name"))
    names.distinct.size shouldBe names.size
  }

  it should "expose a valid ternary simplex (positive parts summing to 100)" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val a = t.getField[java.lang.Double]("simplex_a").doubleValue
      val b = t.getField[java.lang.Double]("simplex_b").doubleValue
      val c = t.getField[java.lang.Double]("simplex_c").doubleValue
      a should be > 0.0
      b should be > 0.0
      c should be > 0.0
      (a + b + c) shouldBe 100.0 +- 1e-9
    }
  }

  it should "expose trade_date as a real ISO-8601 date (parseable, not the old day-N)" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val d = t.getField[String]("trade_date")
      noException should be thrownBy java.time.LocalDate.parse(d)
    }
  }

  it should "expose edge_pair as single-rooted 2-element list literals" in {
    // Every cell is "[0, child]" → parses to a 2-list rooted at 0, so TreePlot
    // builds one connected tree instead of an error page.
    CanonicalFixture.port0Rows.foreach { t =>
      t.getField[String]("edge_pair") should fullyMatch regex """\[0, \d+\]"""
    }
  }

  it should "expose overlapping node_src/node_dst so graph ops have drawable edges" in {
    val src = CanonicalFixture.port0Rows.map(_.getField[String]("node_src")).toSet
    val dst = CanonicalFixture.port0Rows.map(_.getField[String]("node_dst")).toSet
    (src intersect dst) should not be empty
  }

  it should "expose finish_ts strictly after start_ts (non-degenerate Gantt bar)" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val s = t.getField[java.sql.Timestamp]("start_ts")
      val f = t.getField[java.sql.Timestamp]("finish_ts")
      f.after(s) shouldBe true
    }
  }

  it should "round-trip TIMESTAMP columns losslessly through TupleIO (write then read)" in {
    val root = Files.createTempDirectory("canonical-fixture-ts-")
    val path = CanonicalFixture.writeInputs(root, inputPortCount = 1)(PortIdentity(0))
    val schema = TupleIO.readSchemaSidecar(path)
    val rows = TupleIO.readTuples(path, schema).toList
    rows should not be empty
    val read = rows.head
    val orig = CanonicalFixture.port0Rows.head
    // The JDBC-string codec is the exact inverse of Timestamp.toString, so the
    // value read back equals the value written — no timezone drift.
    read.getField[java.sql.Timestamp]("start_ts") shouldBe orig.getField[java.sql.Timestamp](
      "start_ts"
    )
    read.getField[java.sql.Timestamp]("finish_ts") shouldBe orig.getField[java.sql.Timestamp](
      "finish_ts"
    )
  }

  it should "expose non-empty short_text sentences for text-classification ops" in {
    CanonicalFixture.port0Rows.foreach { t =>
      t.getField[String]("short_text").trim should not be empty
    }
  }

  it should "expose long_text with several sentences so summarization is non-trivial" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val txt = t.getField[String]("long_text")
      // multiple sentence-terminating periods → real content to condense
      txt.count(_ == '.') should be >= 2
    }
  }

  // The sklearn families fit `species` against the petal columns, so the table
  // has to hold up as training data. The estimators that cross-validate pass no
  // fold count and so take sklearn's default of five, and a class of fewer than
  // five rows leaves a fold holding none of it — no error, just a warning and a
  // fold that asks nothing. A label the features cannot separate is the other
  // half: it leaves the fit breaking ties, which is where two paths drift apart.
  it should "expose species as a petal-separable label with enough members to fold on" in {
    val rows = CanonicalFixture.allRows
    val byClass = rows.groupBy(_.getField[java.lang.Integer]("species").intValue)
    byClass.keySet shouldBe Set(0, 1)
    byClass.values.foreach(_.size should be >= 5)
    rows.foreach { t =>
      val large = t.getField[java.lang.Double]("petal_length").doubleValue >= 3.9
      t.getField[java.lang.Integer]("species").intValue shouldBe (if (large) 1 else 0)
    }
  }

  // The countVectorizer=true path fits the same label on short_text alone, and a
  // sentence appearing under both labels makes that set unlearnable.
  it should "keep every short_text sentence inside one species" in {
    CanonicalFixture.allRows
      .groupBy(_.getField[String]("short_text"))
      .foreach {
        case (sentence, rows) =>
          withClue(s"$sentence: ") {
            rows.map(_.getField[java.lang.Integer]("species")).distinct.size shouldBe 1
          }
      }
  }

  it should "expose iris petal columns in a realistic centimetre range" in {
    CanonicalFixture.port0Rows.foreach { t =>
      val len = t.getField[java.lang.Double]("petal_length").doubleValue
      val wid = t.getField[java.lang.Double]("petal_width").doubleValue
      len should (be > 0.0 and be < 8.0)
      wid should (be > 0.0 and be < 3.0)
    }
  }

  // A single-token row would make split/explode a no-op, so both windows need
  // rows that fan out AND a row that doesn't — the two branches of an unnest.
  it should "expose csv_list as a clean delimited list with varying token counts" in {
    Seq(CanonicalFixture.port0Rows, CanonicalFixture.port1Rows).foreach { rows =>
      val tokenCounts = rows.map { t =>
        val raw = t.getField[String]("csv_list")
        raw should not startWith ","
        raw should not endWith ","
        val tokens = raw.split(",", -1)
        tokens.foreach(_.trim should not be empty)
        tokens.length
      }
      tokenCounts.min shouldBe 1
      tokenCounts.max should be > 1
    }
  }

  // A case flag is only worth sweeping where flipping it changes WHICH rows match,
  // which needs rows of all three kinds in EVERY window a test reads — hence the
  // per-port check rather than one over the whole table.
  it should "expose mixed_case with lower, upper and letterless rows on every port" in {
    Seq(CanonicalFixture.port0Rows, CanonicalFixture.port1Rows).foreach { rows =>
      val values = rows.map(_.getField[String]("mixed_case"))
      values.count(v => v.exists(_.isLower)) should be > 0
      values.count(v => v.exists(_.isUpper) && !v.exists(_.isLower)) should be > 0
      values.count(v => !v.exists(_.isLetter)) should be > 0
    }
  }

  it should "write one JSONL fixture per requested input port" in {
    val root = Files.createTempDirectory("canonical-fixture-")
    val inputs = CanonicalFixture.writeInputs(root, inputPortCount = 2)
    inputs.keySet shouldBe Set(PortIdentity(0), PortIdentity(1))
    inputs.values.foreach(p => Files.size(p) should be > 0L)
  }

  it should "reject unsupported port counts" in {
    val root = Files.createTempDirectory("canonical-fixture-")
    an[IllegalArgumentException] should be thrownBy
      CanonicalFixture.writeInputs(root, inputPortCount = 3)
  }

  it should "empty every column but id exactly once in the gapped table" in {
    val rows = CanonicalFixture.emptyOneCellPerColumn(CanonicalFixture.port0Rows)
    rows.size shouldBe CanonicalFixture.port0Rows.size

    CanonicalFixture.schema.getAttributes.foreach { attr =>
      val empties = rows.count(_.getField[AnyRef](attr.getName) == null)
      // id carries the joins, so it keeps every value; everything else gets one
      // hole, which is what makes the case a null case rather than an empty table.
      if (attr.getName == "id") empties shouldBe 0
      else empties shouldBe 1
    }
  }

  it should "leave every row in the gapped table with something in it" in {
    val names = CanonicalFixture.schema.getAttributes.map(_.getName)
    CanonicalFixture.emptyOneCellPerColumn(CanonicalFixture.port0Rows).foreach { t =>
      // A wholly empty row would test the operator's handling of an empty table
      // instead, and would say nothing about a null beside a filled neighbour.
      names.count(n => t.getField[AnyRef](n) != null) should be > 0
    }
  }
}
