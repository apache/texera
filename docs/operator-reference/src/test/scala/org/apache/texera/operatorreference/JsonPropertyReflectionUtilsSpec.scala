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

import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.{
  SklearnAdvancedKNNClassifierTrainerOpDesc,
  SklearnAdvancedKNNParameters
}
import org.apache.texera.amber.operator.source.apis.twitter.v2.TwitterSearchSourceOpDesc
import org.apache.texera.amber.operator.source.sql.postgresql.PostgreSQLSourceOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonPropertyReflectionUtilsSpec extends AnyFlatSpec with Matchers {

  private def table(clazz: Class[_]): String =
    JsonPropertyReflectionUtils.generatePropertiesTable(clazz, "../../").get

  "generatePropertiesTable" should "inline a nested config class and wrap long cells" in {
    val rows = table(classOf[SpecializedFilterOpDesc]).linesIterator.toList
    rows should contain(
      "| Predicates | ✓ | List<Filter Predicate> | - | Multiple predicates in OR |"
    )
    rows should contain("| ↳ Attribute | ✓ | String | - |  |")
    rows should contain("| ↳ Condition | ✓ | =, >, >=, <, <=, !=, is null,<br>is not null | - |  |")
    rows should contain(
      "| ↳ Value |  | String | - | Hidden when `Condition` matches<br>`is null\\|is not null` |"
    )
  }

  it should "keep declaration order and nest a field under the boolean that reveals it" in {
    val rows = table(classOf[SplitOpDesc]).linesIterator.toList.drop(2)
    rows.map(_.split('|')(1).trim) shouldBe List("Split Percentage", "Auto-Generate Seed", "↳ Seed")
  }

  it should "nest the fields a toggleHidden switch reveals and resolve Option element types" in {
    val rows = table(classOf[PostgreSQLSourceOpDesc]).linesIterator.toList
    rows should contain("| Limit |  | Long | - | Max output count |")
    rows.indexOf("| ↳ Keyword Search Column |  | String | - |  |") shouldBe
      rows.indexOf("| Keyword Search? |  | Boolean | false |  |") + 1
  }

  it should "render a code property as a template section" in {
    val output = table(classOf[PythonUDFOpDescV2])
    output should include(
      "| Python script | ✓ | Code (python) | `See template below` | Input your code here |"
    )
    output should include("#### Default Code Template\n\n**Python script**\n\n```python\n")
  }

  it should "link a hyperparameter list to its parameter page" in {
    table(classOf[SklearnAdvancedKNNClassifierTrainerOpDesc]) should include(
      "| Parameter Setting | ✓ | [SklearnAdvancedKNNParameters](../../parameters/sklearn-advanced-knn-parameters/) | - |  |"
    )
  }

  it should "read a schema description and a default set only on the field" in {
    table(classOf[TwitterSearchSourceOpDesc]) should include(
      "| Stop Upon Rate Limit | ✓ | Boolean | false | Stop when hitting rate limit? |"
    )
    table(classOf[LimitOpDesc]) should include(
      "| Limit | ✓ | Integer | 0 | The max number of output rows |"
    )
  }

  it should "be empty for an operator without properties" in {
    JsonPropertyReflectionUtils.generatePropertiesTable(
      classOf[DistinctOpDesc],
      "../../"
    ) shouldBe None
  }

  "generateOutputSection" should "link each port mode to the output-modes page" in {
    JsonPropertyReflectionUtils.generateOutputSection(
      classOf[SpecializedFilterOpDesc],
      "../../"
    ) should include(
      "| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |"
    )
  }

  "extractEnumMetadata" should "read the hyperparameter names and types" in {
    JsonPropertyReflectionUtils.getEnumClass(
      classOf[SklearnAdvancedKNNClassifierTrainerOpDesc]
    ) shouldBe
      Some(classOf[SklearnAdvancedKNNParameters])
    JsonPropertyReflectionUtils
      .extractEnumMetadata(classOf[SklearnAdvancedKNNParameters])
      .head shouldBe
      EnumParam("n_neighbors", "int", "5", ">=1")
    JsonPropertyReflectionUtils.parameterPageSlug(classOf[SklearnAdvancedKNNParameters]) shouldBe
      "sklearn-advanced-knn-parameters"
  }
}
