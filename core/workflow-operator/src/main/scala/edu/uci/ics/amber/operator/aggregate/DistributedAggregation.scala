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

package edu.uci.ics.amber.operator.aggregate

import edu.uci.ics.amber.core.tuple.Tuple

/**
  * This class defines the necessary functions required by a distributed aggregation.
  *
  * Explanation using "average" as an example:
  * To compute the average of data on multiple nodes,
  * each node first computes the sum and count of its local data (partial result),
  * then the partial results are sent to one node, where it adds up the sum and count of all nodes,
  * finally, average is computed by calculating sum/count.
  *
  * Corresponding to the functions:
  * init:     initializes partial result:   partial = (sum=0, count=0)
  * iterate:  accumulates each input data:  sum += inputValue, count += 1
  * merge:    adds up all partial results:  sum += partialSum, count += partialCount
  * finalAgg: calculates final result:      average = sum / count
  *
  * These function definitions are from
  * "Distributed Aggregation for Data-Parallel Computing: Interfaces and Implementations"
  * https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
  */
case class DistributedAggregation[P <: AnyRef](
    // () => PartialObject
    init: () => P,
    // PartialObject + Tuple => PartialObject
    iterate: (P, Tuple) => P,
    // PartialObject + PartialObject => PartialObject
    merge: (P, P) => P,
    // PartialObject => Tuple with one column, later be combined into FinalObject
    finalAgg: (P) => Object
)
