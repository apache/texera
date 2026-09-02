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

package org.apache.texera.amber.operator

/** Python definitions shared by several operators' standalone code, emitted
  * once per script via [[StandaloneCodeGenerator.standaloneHelpers]].
  */
object StandaloneHelpers {

  /**
    * A Python transcription of `java.util.Random`, for operators whose executor
    * draws from one.
    *
    * A sampler decides per row whether to keep it, so which rows survive is
    * fixed by the exact sequence the generator produces. Seeding Python's
    * `random` or numpy's with the engine's seed selects a different set, and
    * the script would then report a different sample than the workflow it came
    * from. Only the same generator gives the same rows.
    */
  val JavaRandom: String =
    """# java.util.Random, transcribed so sampling matches the engine.
      |class _TexeraJavaRandom:
      |    _MASK = (1 << 48) - 1
      |    _MULTIPLIER = 0x5DEECE66D
      |    _ADDEND = 0xB
      |
      |    def __init__(self, seed):
      |        self._seed = (seed ^ self._MULTIPLIER) & self._MASK
      |
      |    def _next(self, bits):
      |        self._seed = (self._seed * self._MULTIPLIER + self._ADDEND) & self._MASK
      |        value = self._seed >> (48 - bits)
      |        return value - (1 << 32) if value >= (1 << 31) else value
      |
      |    def next_double(self):
      |        return ((self._next(26) << 27) + self._next(27)) * (2.0 ** -53)
      |
      |    def next_int(self, bound):
      |        if bound & (-bound) == bound:
      |            return (bound * self._next(31)) >> 31
      |        while True:
      |            bits = self._next(31)
      |            value = bits % bound
      |            if bits - value + (bound - 1) >= 0:
      |                return value""".stripMargin
}
