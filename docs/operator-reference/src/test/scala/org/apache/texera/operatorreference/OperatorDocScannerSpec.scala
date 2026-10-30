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

package org.apache.texera.operatorreference

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class OperatorDocScannerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val outputDir: Path = Files.createTempDirectory("operator-reference")
  private val handWritten = outputDir.resolve("output-modes.md")
  private val stale = outputDir.resolve("data-cleaning/removed-operator.md")

  override def beforeAll(): Unit = {
    Files.write(handWritten, "hand-written".getBytes(StandardCharsets.UTF_8))
    Files.createDirectories(stale.getParent)
    Files.write(stale, "stale".getBytes(StandardCharsets.UTF_8))
    OperatorDocScanner.main(Array(outputDir.toString))
  }

  override def afterAll(): Unit =
    Files.walk(outputDir).iterator().asScala.toList.reverse.foreach(Files.delete)

  private def read(relative: String): String =
    new String(Files.readAllBytes(outputDir.resolve(relative)), StandardCharsets.UTF_8)

  "OperatorDocScanner" should "keep the hand-written page and drop a stale operator page" in {
    read("output-modes.md") shouldBe "hand-written"
    Files.exists(stale) shouldBe false
  }

  it should "write an operator page under its group path with a breadcrumb to the root" in {
    val page = read("data-cleaning/join/hash-join.md")
    page should startWith("<!--")
    page should include("\ntitle: \"Hash Join\"\n")
    page should include("\ntags: [data-cleaning, join]\n")
    page should include("[Home](../../../) > [Data Cleaning](../../) > [Join](../)")
    page should include("[Set Snapshot](../../../output-modes/#set-snapshot)")
  }

  it should "number a group index by its position among siblings and list its operators" in {
    val index = read("machine-learning/advanced-sklearn/_index.md")
    index should include regex "\nweight: [1-9]\\d*\n"
    index should include("[Home](../../) > [Machine Learning](../) > Advanced Sklearn")
    index should include("| [KNN Classifier](knn-classifier/) |")
    read("data-cleaning/_index.md") should include("## Subcategories\n\n- [Join](join/)")
  }

  it should "link the root index to every top-level group with a cumulative count" in {
    val root = read("_index.md")
    root should include("- [Parameter Reference](parameters/)")
    root should include regex "\n- \\[Data Cleaning\\]\\(data-cleaning/\\) \\(\\d+ operators\\)\n"
    root should include regex "\n  - \\[Join\\]\\(data-cleaning/join/\\) \\(\\d+ operators\\)\n"
  }

  it should "write one parameter page per hyperparameter enum" in {
    read("parameters/_index.md") should include(
      "| [SklearnAdvancedKNN](sklearn-advanced-knn-parameters/) | 2 | KNN Classifier, KNN Regressor |"
    )
    val page = read("parameters/sklearn-advanced-knn-parameters.md")
    page should include(
      "- [KNN Classifier](../../machine-learning/advanced-sklearn/knn-classifier/)"
    )
    page should include("| n_neighbors | `int` | 5 | >=1 |")
  }
}
