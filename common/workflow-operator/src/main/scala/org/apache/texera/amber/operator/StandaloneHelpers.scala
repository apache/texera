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

  /**
    * A Python transcription of `AttributeTypeUtils`, for operators that cast a
    * column to a declared type.
    *
    * Python's own conversions answer differently on the values a spreadsheet
    * column actually holds. `bool("false")` is true, because every non-empty
    * string is; `int("6.7")` and `float("abc")` raise where a coercing cast
    * would have returned 6 and NaN. The engine reads "false" as false, "0" as
    * false, and refuses "6.7" as an integer, so the script has to do the same
    * rather than hand back a column the workflow never produced.
    *
    * Refusing is part of the contract: `parseField` raises on a value it cannot
    * read, and a script that quietly wrote NaN instead would report an answer
    * the run it was exported from never reached.
    */
  val AttributeCasts: String =
    """# AttributeTypeUtils, transcribed so a cast answers as the engine does.
      |def _texera_cast_boolean(x):
      |    # toBoolean first, then `toInt == 1`: "0" and "2" are both false.
      |    if isinstance(x, str):
      |        text = x.strip()
      |        lowered = text.lower()
      |        if lowered == "true":
      |            return True
      |        if lowered == "false":
      |            return False
      |        return int(text) == 1
      |    return x != 0
      |
      |
      |def _texera_cast_integral(x):
      |    # Scala's toInt/toLong take no decimal point, and truncate a Double
      |    # toward zero.
      |    if isinstance(x, str):
      |        return int(x.strip())
      |    return int(x)
      |
      |
      |def _texera_cast_double(x):
      |    if isinstance(x, str):
      |        return float(x.strip())
      |    return float(x)""".stripMargin
}
