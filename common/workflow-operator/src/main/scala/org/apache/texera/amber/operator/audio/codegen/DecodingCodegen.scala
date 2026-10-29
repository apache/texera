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

package org.apache.texera.amber.operator.audio.codegen

/**
  * Python that reads one audio cell and returns mono samples at the target sample rate, for
  * [[org.apache.texera.amber.operator.audio.AudioAttributeExtractorOpDesc]].
  *
  * `decode` recognises the format from the bytes, decodes WAV, MP3 or FLAC with miniaudio (PCM
  * WAV through the standard `wave` module without it) in blocks, mixes to mono and resamples
  * with SciPy's `resample_poly`, or a NumPy stand-in without SciPy. Large binary cells are read
  * with a size cap. Input that cannot be analysed raises `AudioInputError` naming the cause.
  *
  * Fixed module-level Python, spliced as is into the descriptor's template: settings reach it
  * as function arguments, never as text inside it.
  */
object DecodingCodegen {

  val python: String =
    """# ---------------------------------------------------------------- decoding
      |
      |SUPPORTED = "WAV, MP3 or FLAC" if miniaudio else "PCM WAV"
      |MIN_RATE, MAX_RATE = 1000, 384000  # a header outside this range is damaged, not exotic
      |MAX_LARGE_BINARY_BYTES = 256 * 1024 * 1024  # a large binary cell is read whole before decoding
      |MISSING_OBJECT_CODES = ("NoSuchKey", "NoSuchVersion", "NotFound", "404")
      |
      |
      |def read_capped(stream, limit=MAX_LARGE_BINARY_BYTES):
      |    # Reads a large binary stream, refusing one bigger than `limit` bytes.
      |    data = stream.read(limit + 1)
      |    if len(data) > limit:
      |        raise AudioInputError(f"the file is larger than the {limit // (1024 * 1024)} MB limit for one audio file")
      |    return data
      |
      |
      |def read_large_binary(value, open_stream, limit=MAX_LARGE_BINARY_BYTES):
      |    # Reads a large binary cell. A missing stored object is this file's problem and can be
      |    # skipped; any other storage failure (credentials, configuration, service) propagates.
      |    try:
      |        with open_stream(value) as stream:
      |            return read_capped(stream, limit)
      |    except StorageError as e:
      |        code = e.response.get("Error", {}).get("Code")
      |        if code not in MISSING_OBJECT_CODES:
      |            raise
      |        raise AudioInputError(f"the stored file is missing ({code})") from None
      |
      |
      |def _wav_frames(data):
      |    # (frames the data chunk declares, frames the bytes can hold) for RIFF/WAVE, else (0, 0).
      |    #
      |    # miniaudio reports what is actually present, so `declared` is what reveals a cut-off
      |    # file. A size of 0 or 0xFFFFFFFF means the writer did not know the length (a WAV
      |    # written to a pipe), so it declares nothing. `present` bounds any buffer sized from
      |    # a header, which may be damaged.
      |    if data[:4] != b"RIFF" or data[8:12] != b"WAVE":
      |        return 0, 0
      |    pos, align = 12, 0
      |    while pos + 8 <= len(data):
      |        cid, size = data[pos : pos + 4], int.from_bytes(data[pos + 4 : pos + 8], "little")
      |        if cid == b"fmt " and pos + 22 <= len(data):
      |            align = int.from_bytes(data[pos + 20 : pos + 22], "little")
      |        elif cid == b"data":
      |            if not align:
      |                return 0, 0
      |            present = (len(data) - pos - 8) // align
      |            return (0 if size in (0, 0xFFFFFFFF) else size // align), present
      |        pos += 8 + size + (size & 1)
      |    return 0, 0
      |
      |
      |def _probe(data, kind):
      |    # (duration s, sample rate, frames) from the header, without decoding, asking only
      |    # the decoder for the detected format. Ogg data never reaches miniaudio: fuzzing
      |    # found corrupted Ogg Vorbis files that abort the process inside stb_vorbis.
      |    if miniaudio is not None and kind in (None, "WAV", "MP3", "FLAC"):
      |        probe = {"WAV": miniaudio.wav_get_info, "FLAC": miniaudio.flac_get_info}.get(kind, miniaudio.mp3_get_info)
      |        try:
      |            info = probe(data)
      |            return info.duration, info.sample_rate, info.num_frames
      |        except Exception:
      |            pass
      |    try:
      |        with wave.open(io.BytesIO(data)) as w:
      |            # The stdlib takes the length from the header; never claim more than the bytes hold.
      |            n = min(w.getnframes(), _wav_frames(data)[1] or w.getnframes())
      |            return n / max(1, w.getframerate()), w.getframerate(), n
      |    except Exception:  # the stdlib parser raises bare RuntimeError on bad chunk sizes
      |        return None
      |
      |
      |def _sniff(data):
      |    # Names the container, so an error can say what it saw.
      |    head = data[:12]
      |    if head[4:8] == b"ftyp":
      |        return "M4A/MP4 (AAC)"
      |    if head[:4] == b"FORM":
      |        return "AIFF"
      |    if head[:4] == b"OggS":
      |        return "Ogg Opus" if b"OpusHead" in data[:64] else "Ogg Vorbis"
      |    if head[:4] == b"RF64":
      |        return "RF64 (64-bit WAV)"
      |    if head[:4] == b"RIFF" and head[8:12] == b"WAVE":
      |        return "WAV"
      |    if head[:4] == b"fLaC":
      |        return "FLAC"
      |    if head[:3] == b"ID3":
      |        return "MP3"
      |    if len(head) > 1 and head[0] == 0xFF and head[1] & 0xE0 == 0xE0:
      |        # MPEG audio and ADTS AAC share the frame sync; AAC's layer bits are zero.
      |        return "MP3" if head[1] & 0x06 else "AAC (ADTS)"
      |    return None
      |
      |
      |READABLE = {"WAV", "MP3", "FLAC"}
      |
      |
      |def _unsupported(data, detail):
      |    kind = _sniff(data)
      |    if kind == "WAV" and miniaudio is None:
      |        return AudioInputError(f"the WAV file could not be read ({detail})")
      |    if kind in READABLE and (miniaudio is not None or kind == "WAV"):
      |        return AudioInputError(f"the {kind} file could not be read; it may be damaged or cut off")
      |    if kind in READABLE:
      |        return AudioInputError(f"{kind} needs the miniaudio package (not installed); convert it to PCM WAV")
      |    if kind:
      |        return AudioInputError(f"{kind} audio is not supported; convert it to {SUPPORTED}")
      |    return AudioInputError(f"not a recognised audio file; expected {SUPPORTED}")
      |
      |
      |STREAM_FRAMES = 65536  # frames decoded per step; the file is never held whole at its own rate
      |# What a decoder raises on bytes it cannot read; anything else is a bug and propagates.
      |DECODE_ERRORS = (wave.Error, EOFError) + ((miniaudio.MiniaudioError,) if miniaudio else ())
      |
      |
      |def _pcm_to_float(raw, width):
      |    if width == 1:
      |        return (np.frombuffer(raw, np.uint8).astype(np.float32) - 128) / 128
      |    if width == 2:
      |        return np.frombuffer(raw, "<i2").astype(np.float32) / 32768
      |    if width == 3:
      |        b = np.frombuffer(raw, np.uint8).reshape(-1, 3)
      |        v = b[:, 0].astype(np.int32) | (b[:, 1].astype(np.int32) << 8) | (b[:, 2].astype(np.int32) << 16)
      |        v = np.where(v >= 1 << 23, v - (1 << 24), v)
      |        return v.astype(np.float32) / (1 << 23)
      |    if width == 4:
      |        return np.frombuffer(raw, "<i4").astype(np.float32) / 2147483648
      |    raise AudioInputError(f"unsupported WAV sample width: {width * 8} bits")
      |
      |
      |def _wave_chunks(data):
      |    # Stdlib PCM WAV reader: (sample rate, frames the header claims, mono float32 chunks).
      |    try:
      |        w = wave.open(io.BytesIO(data))
      |    except Exception as e:  # wave.Error, EOFError, and a bare RuntimeError on bad chunk sizes
      |        raise _unsupported(data, e) from None
      |    ch, width, sr, n = w.getnchannels(), w.getsampwidth(), w.getframerate(), w.getnframes()
      |    if width not in (1, 2, 3, 4):
      |        raise AudioInputError(f"unsupported WAV sample width: {width * 8} bits")
      |    if ch < 1 or not MIN_RATE <= sr <= MAX_RATE:
      |        raise AudioInputError(f"the WAV header is invalid ({ch} channels, {sr} Hz)")
      |
      |    def gen():
      |        with w:
      |            while True:
      |                raw = w.readframes(STREAM_FRAMES)
      |                usable = len(raw) - len(raw) % (width * ch)
      |                if usable <= 0:
      |                    return
      |                yield _pcm_to_float(raw[:usable], width).reshape(-1, ch).mean(axis=1)
      |
      |    return sr, n, gen()
      |
      |
      |class _Resampler:
      |    # Streams audio through scipy's polyphase filter (or its NumPy stand-in) in fixed blocks.
      |    #
      |    # Block edges sit on multiples of `down`, so output samples land on whole
      |    # indices, and each block is filtered with `pad` samples of context on both
      |    # sides (wider than the filter), which are then cut off. The result equals one
      |    # whole-array resample_poly call, without ever holding the input whole.
      |
      |    def __init__(self, sr, target_sr, expected_frames=0):
      |        self.identity = sr == target_sr
      |        # Without SciPy, an approximate stand-in streams through the same blocks.
      |        self.kernel = resample_poly or _numpy_resample
      |        self.sr, self.target_sr = sr, target_sr
      |        g = math.gcd(int(sr), int(target_sr))
      |        self.up, self.down = target_sr // g, sr // g
      |        self.pad = self.down * (math.ceil(10 * max(self.up, self.down) / self.up / self.down) + 1)
      |        # Bounded on both sides of the call: upsampling 1 kHz to 192 kHz grows a block 192x.
      |        self.block = self.down * max(1, 262144 // max(self.up, self.down))
      |        self.buf = np.zeros(0, np.float32)
      |        self.base = 0  # global index of buf[0]
      |        self.next = 0  # global index of the next block to emit
      |        self.total = 0
      |        # Output goes into one buffer sized from the header's length (grown if the
      |        # header understates it), so the result is never held twice while joining.
      |        ratio = 1.0 if self.identity else self.up / self.down
      |        self.out = np.empty(int(max(0, expected_frames) * ratio) + 1024, np.float32)
      |        self.filled = 0
      |
      |    def _put(self, y):
      |        if self.filled + len(y) > len(self.out):
      |            grown = np.empty(max(2 * len(self.out), self.filled + len(y)), np.float32)
      |            grown[: self.filled] = self.out[: self.filled]
      |            self.out = grown
      |        self.out[self.filled : self.filled + len(y)] = y
      |        self.filled += len(y)
      |
      |    def _emit(self, s0):
      |        a0 = max(0, s0 - self.pad)
      |        b0 = min(self.total, s0 + self.block + self.pad)
      |        y = self.kernel(self.buf[a0 - self.base : b0 - self.base], self.up, self.down)
      |        lo = (s0 - a0) * self.up // self.down
      |        hi = lo + (min(self.block, self.total - s0) * self.up + self.down - 1) // self.down
      |        self._put(y[lo:hi])
      |
      |    def feed(self, chunk):
      |        if self.identity:
      |            self._put(chunk)
      |            self.total += len(chunk)
      |            return
      |        self.buf = np.concatenate([self.buf, chunk])
      |        self.total += len(chunk)
      |        while self.next + self.block + self.pad <= self.total:
      |            self._emit(self.next)
      |            self.next += self.block
      |            keep = max(self.base, self.next - self.pad)
      |            self.buf = self.buf[keep - self.base :]
      |            self.base = keep
      |
      |    def finish(self):
      |        if not self.identity:
      |            while self.next < self.total:
      |                self._emit(self.next)
      |                self.next += self.block
      |        x = self.out[: self.filled]
      |        n = (self.total * self.up + self.down - 1) // self.down if not self.identity else len(x)
      |        return x[:n]
      |
      |
      |def _numpy_resample(x, up, down):
      |    # Stands in for scipy.signal.resample_poly when SciPy is missing: a box pre-filter
      |    # and linear interpolation, with resample_poly's output length and alignment, so it
      |    # streams block by block. It leaks some aliasing that SciPy's filter removes.
      |    if down > up:
      |        k = int(round(down / up))
      |        if k > 1:
      |            x = np.convolve(x, np.ones(k, np.float32) / k, mode="same")
      |    n = (len(x) * up + down - 1) // down
      |    return np.interp(np.arange(n) * (down / up), np.arange(len(x)), x).astype(np.float32)
      |
      |
      |def resample(x, sr, target_sr):
      |    # Whole-array entry point (tests, callers holding samples already).
      |    r = _Resampler(sr, target_sr, len(x))
      |    for i in range(0, len(x), STREAM_FRAMES):
      |        r.feed(np.asarray(x[i : i + STREAM_FRAMES], np.float32))
      |    return r.finish()
      |
      |
      |def _miniaudio_stream(data, kind, sr):
      |    # Decode with the decoder forced to the detected format (WAV, FLAC or MP3).
      |
      |    class Source(miniaudio.StreamableSource):
      |        def __init__(self):
      |            self.buf = io.BytesIO(data)
      |
      |        def read(self, num_bytes):
      |            return self.buf.read(num_bytes)
      |
      |        def seek(self, offset, origin):
      |            self.buf.seek(offset, {miniaudio.SeekOrigin.CURRENT: 1, miniaudio.SeekOrigin.END: 2}.get(origin, 0))
      |            return True
      |
      |    fmt = {"WAV": miniaudio.FileFormat.WAV, "FLAC": miniaudio.FileFormat.FLAC}.get(kind, miniaudio.FileFormat.MP3)
      |    return miniaudio.stream_any(
      |        Source(), source_format=fmt, output_format=miniaudio.SampleFormat.FLOAT32, nchannels=1, sample_rate=sr, frames_to_read=STREAM_FRAMES
      |    )
      |
      |
      |def _prepend(first, rest):
      |    yield first
      |    yield from rest
      |
      |
      |def _span(seconds):
      |    if seconds < 0.001:
      |        return "under 1 ms"  # one missing sample at 22,050 Hz is 0.05 ms, not "0 ms"
      |    return f"{seconds * 1000:.0f} ms" if seconds < 1 else f"{seconds:.1f} s"
      |
      |
      |def _length(seconds, limit):
      |    # Enough decimals that a file just over the limit does not read as the limit itself.
      |    for places in (1, 2, 3):
      |        text = f"{seconds:.{places}f}"
      |        if float(text) > limit:
      |            break
      |    return text
      |
      |
      |def decode(value, target_sr=22050, max_seconds=1800):
      |    # bytes -> (mono float32 samples at target_sr, target_sr, warnings).
      |    # Raises AudioInputError for input that cannot be analysed at all.
      |    if value is None:
      |        raise AudioInputError("the audio cell is empty (null)")
      |    if isinstance(value, (bytearray, memoryview)):
      |        value = bytes(value)
      |    if isinstance(value, str):
      |        raise AudioInputError("the audio column holds text, not binary; set File Scan's attribute type to binary")
      |    if not isinstance(value, bytes):
      |        raise AudioInputError(f"the audio column must be binary, got {type(value).__name__}")
      |    if len(value) == 0:
      |        raise AudioInputError("the audio file is empty (0 bytes)")
      |    kind = _sniff(value)
      |    if kind not in (None, "WAV", "MP3", "FLAC"):
      |        raise _unsupported(value, None)
      |    info = _probe(value, kind)
      |    if info is not None and not MIN_RATE <= info[1] <= MAX_RATE:
      |        raise AudioInputError(f"damaged header: sample rate {info[1]} Hz")
      |    if info is not None and info[0] > max_seconds:
      |        raise AudioInputError(f"audio is {_length(info[0], max_seconds)} s long; the limit is {max_seconds} s")
      |    # Frames the header promises, for the truncation warning: the RIFF data chunk for
      |    # WAV, STREAMINFO for FLAC. MP3 declares no length (its count comes from decoding).
      |    declared, present = _wav_frames(value) if kind == "WAV" else (0, 0)
      |    if kind == "FLAC" and info is not None:
      |        declared = info[2]
      |    chunks = None
      |    if miniaudio is not None and info is not None:
      |        sr, expected = info[1], max(info[2], declared)
      |        try:
      |            stream = _miniaudio_stream(value, kind, sr)
      |            first = next(stream, None)
      |            if first is not None:
      |                chunks = (np.frombuffer(c, dtype=np.float32) for c in _prepend(first, stream))
      |        except miniaudio.MiniaudioError:
      |            chunks = None
      |    if chunks is None:
      |        sr, expected, chunks = _wave_chunks(value)
      |    # The output buffer is sized from the header, so bound it by what the bytes can hold
      |    # and by the duration limit: a damaged size field must not reserve gigabytes.
      |    if present:
      |        expected = min(expected, present)
      |    resampler = _Resampler(sr, target_sr, min(expected or 0, max_seconds * sr))
      |    decoded = non_finite = 0
      |    warnings = []
      |    chunks = iter(chunks)
      |    while True:
      |        # Only the decoder is guarded: an error in the processing below is a bug and
      |        # must surface, not be mistaken for a damaged file.
      |        try:
      |            c = next(chunks, None)
      |        except DECODE_ERRORS as e:  # the decoder failing part-way through untrusted bytes
      |            if decoded == 0:
      |                raise _unsupported(value, e) from None
      |            warnings.append(f"decoding stopped after {_span(decoded / sr)}; the rest of the file was not read")
      |            break
      |        if c is None:
      |            break
      |        bad = ~np.isfinite(c)
      |        if bad.any():
      |            c = np.where(bad, np.float32(0), c)
      |            non_finite += int(bad.sum())
      |        decoded += len(c)
      |        # A header can understate the length; stop at the limit regardless.
      |        if decoded > max_seconds * sr:
      |            raise AudioInputError(f"audio is longer than the limit of {max_seconds} s")
      |        resampler.feed(c)
      |    x = resampler.finish()
      |    if decoded == 0 or len(x) == 0:
      |        raise AudioInputError("the audio file contains no samples")
      |    # Both decoders return what they could read from a cut-off file; say so once. A
      |    # complete WAV or FLAC file decodes to exactly the length its header declares.
      |    if declared and decoded < declared:
      |        warnings.append(f"cut off: {_span((declared - decoded) / sr)} of the {_span(declared / sr)} its header declares is missing")
      |    if non_finite:
      |        warnings.append(f"{non_finite} non-finite samples were replaced with silence")
      |    return x, target_sr, warnings""".stripMargin
}
