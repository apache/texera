<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Audio test fixtures

Each `*.b64` file is a base64-encoded audio file, kept as text so no binary is
committed. All three hold the same original signal, made for these tests: a
1-second, 440 Hz sine at 22,050 Hz, mono, amplitude 0.5, with 10 ms fades.

| File | Encoding |
| --- | --- |
| `tone_440hz_1s.mp3.b64` | MP3, 32 kbit/s (LAME) |
| `tone_440hz_1s.flac.b64` | FLAC, 16-bit |
| `tone_440hz_1s.ogg.b64` | Ogg Vorbis (used to check that Ogg is rejected) |

WAV inputs are not stored here; the tests generate them with Python's `wave`
module.
