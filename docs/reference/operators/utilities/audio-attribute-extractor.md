<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "Audio Attribute Extractor"
description: "Measure waveform, loudness, noisiness, pitch, onsets or beats over time from audio files"
category: "Utilities"
operator_type: "AudioAttributeExtractor"
tags: [utilities]
---

[Home](../../) > [Utilities](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Audio Column | ✓ | String | - | Binary column holding the audio file (WAV, MP3 or<br>FLAC) |
| Attribute | ✓ | waveform, rms, zcr, pitch_hz, onsets, beats | rms | The property of the sound to measure |
| Sample Rate (Hz) | ✓ | Integer | 22050 | Audio is resampled to this rate before analysis |
| Frame Length (samples) | ✓ | Integer | 2048 | Window size for each measurement |
| Hop Length (samples) | ✓ | Integer | 512 | Step between measurements |
| Max Points | ✓ | Integer | 5000 | Upper bound on output rows per audio file |
| Max Duration (s) | ✓ | Integer | 600 | Longer files are not decoded |
| Retain Input Columns |  | Boolean | false | Copy the other input columns (not the audio) onto<br>every output row |
| Skip Unreadable Files |  | Boolean | false | Skip audio that cannot be read, with a console<br>message, instead of stopping with an error |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |

### Usage

1. Add a **File Scan** and set its **Attribute Type** to `binary` (`large binary`
   for files over the binary size limit). For a ZIP of audio files, also turn on
   **Extract** and **Include Filename**, so messages and retained rows name each file.
2. Connect the **Audio Attribute Extractor**, pick that column as **Audio Column**,
   and choose an **Attribute**.
3. Chart the output: `x` is the time in seconds, `y` the measured value and
   `series` the attribute's name.

| Attribute | `y` | Chart |
|-----------|-----|-------|
| `waveform` | The signal, as the loudest sample of each slice | Line Chart |
| `rms` | Loudness of each frame | Line Chart |
| `zcr` | Zero-crossing rate: high for noise and hiss, low for tones | Line Chart |
| `pitch_hz` | Pitch of one voice or instrument, 60 to 1000 Hz; empty when unpitched | Line Chart |
| `onsets` | `1.0` at each moment a new sound starts | Scatter Plot |
| `beats` | `1.0` at each estimated beat | Scatter Plot |

The defaults (Sample Rate 22,050 Hz, Frame Length 2048, Hop Length 512, a step of
about 23 ms) suit most music and speech. Frame Length and Hop Length apply to `rms`,
`zcr` and `pitch_hz` only; `onsets` and `beats` use a fixed step. For long files, a
lower Sample Rate (16,000 Hz is enough for speech) is faster, and a larger Hop Length
gives fewer points. With **Retain Input Columns**, a Scatter Plot colored by the
`filename` column compares several files; a Line Chart draws all rows as one line.
