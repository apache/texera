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

package org.apache.texera.demovideos.controllers

// Timing constants shared by the controllers; tuned against the real UI.
// Later controllers extend these objects as they need more tiers.

/** Semantic wait tiers for `page.waitForTimeout(...)`, named by what is being waited on. */
private[controllers] object Delays {
  val Tick: Int = 80 // DOM micro-updates between user-like actions
  val Settle: Int = 150 // dropdown / collapse / toggle animation settling
  val Long: Int = 300 // major UI transition: panel collapse, page settling
  val Network: Int = 700 // backend round trip: upload, login submission
}

/** Upper bounds for `Locator.waitFor(...)` and similar. */
private[controllers] object Timeouts {
  val Quick: Int = 2000 // element expected to already be present
  val Medium: Int = 5000 // navigation, or a dialog that has to open
  val Long: Int = 20000 // page load with heavy assets
}

/** Iteration counts for predicate polling loops. */
private[controllers] object Retries {
  val Short: Int = 8
  val Medium: Int = 16
}
