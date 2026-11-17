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

package org.apache.texera.amber.operator.sklearn

import com.fasterxml.jackson.annotation.JsonIgnore

/** The one definition of which columns an estimator is given.
  *
  * A model is fitted by one operator and read by another, so the rule has to
  * hold across operators rather than within one: a column the fitting side
  * leaves out is a column the predicting side must leave out too, or scikit-learn
  * refuses the frame for naming features it never saw. One definition, mixed in
  * wherever a frame is handed to an estimator, is what makes that true by
  * construction instead of by four copies agreeing.
  *
  * Not on [[SklearnModelOpDesc]], where this began: Linear Regression and the
  * prediction and testing operators descend from `PythonOperatorDescriptor`
  * directly and could not reach it there.
  */
trait SklearnFittableColumns {

  /** Python that narrows `frame` to the columns an estimator can fit, written at
    * `indent`.
    *
    * A column the user did not mean as a feature, a note beside the numbers,
    * would otherwise end the run from inside scikit-learn. Booleans are kept:
    * they fit as 0/1. What was dropped is printed, so the choice is visible
    * rather than silent.
    *
    * A table whose every column but the target is text leaves nothing behind,
    * and a frame of no columns has no dtype for scikit-learn to read, so it
    * raises `at least one array or dtype is required` from inside numpy. The
    * narrowing says so itself instead, naming the columns it left out.
    */
  @JsonIgnore
  protected def narrowToFittableColumns(frame: String, indent: String): String =
    s"""${indent}_fittable = $frame.select_dtypes(include=["number", "bool"])
       |${indent}_ignored = [c for c in $frame.columns if c not in _fittable.columns]
       |${indent}if _ignored:
       |${indent}    print("Ignoring columns an estimator cannot fit:", _ignored)
       |${indent}if _fittable.columns.empty:
       |${indent}    raise ValueError(f"No column left to fit on: an estimator cannot fit {_ignored}. Give it a numeric or boolean column.")
       |${indent}$frame = _fittable""".stripMargin
}
