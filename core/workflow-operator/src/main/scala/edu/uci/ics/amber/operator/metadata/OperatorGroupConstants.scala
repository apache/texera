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

package edu.uci.ics.amber.operator.metadata

object OperatorGroupConstants {
  final val INPUT_GROUP = "Data Input"
  final val DATABASE_GROUP = "Database Connector"
  final val SEARCH_GROUP = "Search"
  final val CLEANING_GROUP = "Data Cleaning"
  final val JOIN_GROUP = "Join"
  final val SET_GROUP = "Set"
  final val AGGREGATE_GROUP = "Aggregate"
  final val SORT_GROUP = "Sort"
  final val UTILITY_GROUP = "Utilities"
  final val API_GROUP = "External API"
  final val VISUALIZATION_GROUP = "Visualization"
  final val VISUALIZATION_BASIC_GROUP = "Basic"
  final val VISUALIZATION_STATISTICAL_GROUP = "Statistical"
  final val VISUALIZATION_SCIENTIFIC_GROUP = "Scientific"
  final val VISUALIZATION_FINANCIAL_GROUP = "Financial"
  final val VISUALIZATION_MEDIA_GROUP = "Media"
  final val VISUALIZATION_ADVANCED_GROUP = "Advanced"
  final val MACHINE_LEARNING_GROUP = "Machine Learning"
  final val ADVANCED_SKLEARN_GROUP = "Advanced Sklearn"
  final val HUGGINGFACE_GROUP = "Hugging Face"
  final val SKLEARN_GROUP = "Sklearn"
  final val UDF_GROUP = "User-defined Functions"
  final val PYTHON_GROUP = "Python"
  final val JAVA_GROUP = "Java"
  final val R_GROUP = "R"
  final val MACHINE_LEARNING_GENERAL_GROUP = "Machine Learning General"
  final val CONTROL_GROUP = "Control Block"

  /**
    * The order of the groups to show up in the frontend operator panel.
    * The order numbers are relative.
    */
  final val OperatorGroupOrderList: List[GroupInfo] = List(
    GroupInfo(INPUT_GROUP),
    GroupInfo(DATABASE_GROUP),
    GroupInfo(SEARCH_GROUP),
    GroupInfo(
      CLEANING_GROUP,
      List(GroupInfo(JOIN_GROUP), GroupInfo(AGGREGATE_GROUP), GroupInfo(SORT_GROUP))
    ),
    GroupInfo(
      MACHINE_LEARNING_GROUP,
      List(
        GroupInfo(SKLEARN_GROUP),
        GroupInfo(ADVANCED_SKLEARN_GROUP),
        GroupInfo(HUGGINGFACE_GROUP),
        GroupInfo(MACHINE_LEARNING_GENERAL_GROUP)
      )
    ),
    GroupInfo(UTILITY_GROUP),
    GroupInfo(API_GROUP),
    GroupInfo(UDF_GROUP, List(GroupInfo(PYTHON_GROUP), GroupInfo(JAVA_GROUP), GroupInfo(R_GROUP))),
    GroupInfo(
      VISUALIZATION_GROUP,
      List(
        GroupInfo(VISUALIZATION_BASIC_GROUP),
        GroupInfo(VISUALIZATION_STATISTICAL_GROUP),
        GroupInfo(VISUALIZATION_SCIENTIFIC_GROUP),
        GroupInfo(VISUALIZATION_FINANCIAL_GROUP),
        GroupInfo(VISUALIZATION_MEDIA_GROUP),
        GroupInfo(VISUALIZATION_ADVANCED_GROUP)
      )
    ),
    GroupInfo(CONTROL_GROUP)
  )
}
