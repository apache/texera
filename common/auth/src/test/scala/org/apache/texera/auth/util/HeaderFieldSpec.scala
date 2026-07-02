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

package org.apache.texera.auth.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HeaderFieldSpec extends AnyFlatSpec with Matchers {

  "HeaderField.UserComputingUnitAccess" should "equal \"x-user-computing-unit-access\"" in {
    HeaderField.UserComputingUnitAccess shouldBe "x-user-computing-unit-access"
  }

  "HeaderField.UserId" should "equal \"x-user-id\"" in {
    HeaderField.UserId shouldBe "x-user-id"
  }

  "HeaderField.UserName" should "equal \"x-user-name\"" in {
    HeaderField.UserName shouldBe "x-user-name"
  }

  "HeaderField.UserEmail" should "equal \"x-user-email\"" in {
    HeaderField.UserEmail shouldBe "x-user-email"
  }

  "HeaderField constants" should "all match the x-user-* namespace in lowercase-hyphen format" in {
    val rfcPattern = "^x-user-[a-z]+(?:-[a-z]+)*$".r
    val allConstants = Seq(
      HeaderField.UserComputingUnitAccess,
      HeaderField.UserId,
      HeaderField.UserName,
      HeaderField.UserEmail
    )
    allConstants.foreach { value =>
      rfcPattern.matches(value) shouldBe true
    }
  }

  it should "all be distinct (no accidental aliasing)" in {
    val allConstants = Seq(
      HeaderField.UserComputingUnitAccess,
      HeaderField.UserId,
      HeaderField.UserName,
      HeaderField.UserEmail
    )
    allConstants.distinct should have size allConstants.size
  }
}
