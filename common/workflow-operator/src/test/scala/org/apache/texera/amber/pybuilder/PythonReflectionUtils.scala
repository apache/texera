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

package org.apache.texera.amber.pybuilder

import org.apache.texera.amber.operator.PythonOperatorDescriptor

import java.lang.reflect.{Type, TypeVariable}

object PythonReflectionUtils {

  final case class RawInvalidTextResult(changed: Seq[String], failed: Seq[String])
  final case class Finding(clazz: String, kind: String, message: String)
  final case class Captured[A](value: A, out: String, err: String)

  // Type-variable substitution environment
  type TypeEnv = Map[TypeVariable[_], Type]

  /** Scan non-abstract, non-interface candidates under acceptPackages. */
  def scanCandidates(
      base: Class[_],
      acceptPackages: Seq[String],
      classLoader: ClassLoader
  ): Seq[Class[_]] =
    PythonClassgraphScanner.scanCandidates(base, acceptPackages, classLoader)

  /** Run the full instantiate -> fill -> inject -> execute -> leak check pipeline for one descriptor class. */
  def checkDescriptor(
      clazz: Class[_ <: PythonOperatorDescriptor],
      rawInvalidText: String,
      maxDepth: Int
  ): Seq[Finding] =
    new DescriptorChecker(rawInvalidText, maxDepth).check(clazz)

  /** Same pipeline, but also returns the generated Python code when available. */
  def checkDescriptorWithCode(
      clazz: Class[_ <: PythonOperatorDescriptor],
      rawInvalidText: String,
      maxDepth: Int
  ): DescriptorChecker.CheckResult =
    new DescriptorChecker(rawInvalidText, maxDepth).checkWithCode(clazz)

  def renderReport(findings: Seq[Finding], total: Int): String =
    PythonRawTextReportRenderer.render(findings, total)

  def captureOutErr[A](thunk: => A): Captured[A] =
    PythonConsoleCapture.captureOutErr(thunk)

}
