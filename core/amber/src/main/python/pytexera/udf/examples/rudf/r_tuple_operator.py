# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Note: make sure R path is initialized in udf.conf and make sure that
# the following packages (in R) are installed: coro, arrow
# --- Source Operator Examples ---
r_tuple_source_zero_tuple = """
library(coro)
coro::generator(function() {
    yield (list())
    # yield (NULL) works too
    })
"""

r_tuple_source_one_tuple = """
library(coro)
coro::generator(function() {
    yield (list(
            attr1 = 1L, # R integer
            attr2 = "A", # R string
            attr3 = TRUE, # R logical (boolean)
            ))
    })
"""

r_tuple_source_multiple_tuples = """
library(coro)
coro::generator(function() {
    for (i in 1:5) {
        yield (list(
            attr1 = 1L, # R integer
            attr2 = "A", # R string
            attr3 = TRUE, # R logical (boolean)
            ))
    })
"""

# --- UDF Operator ---
r_tuple_udf_zero_tuple = """
library(coro)
coro::generator(function(tuple, port) {
    yield (list())
    })
"""

r_tuple_udf_echo = """
library(coro)
coro::generator(function(tuple, port) {
    yield (tuple)
    })
"""

r_tuple_udf_echo_multiple_tuples = """
library(coro)
coro::generator(function(tuple, port) {
    for (i in 1:5) {
        yield (tuple)
    })
"""
