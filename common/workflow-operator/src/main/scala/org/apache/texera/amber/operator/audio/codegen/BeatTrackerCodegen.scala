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
  * Python for the beats attribute: librosa's onset strength, tempo estimation and
  * dynamic-programming beat tracker, ported to NumPy so librosa is not a dependency, for
  * [[org.apache.texera.amber.operator.audio.AudioAttributeExtractorOpDesc]].
  *
  * Derived from librosa 1.0.0 (ISC License, Copyright (c) 2013--2026, librosa development team)
  * and recorded in LICENSE; the Python's opening comment lists the files it adapts.
  *
  * Fixed module-level Python, spliced as is into the descriptor's template: settings reach it
  * as function arguments, never as text inside it.
  */
object BeatTrackerCodegen {

  val python: String =
    """# ---------------------------------------------------------------- beats
      |# Tempo estimation, onset strength and dynamic-programming beat tracking below are
      |# adapted from librosa 1.0.0 (librosa/feature/rhythm.py, librosa/beat.py,
      |# librosa/onset.py, librosa/filters.py, librosa/core/convert.py and
      |# librosa/util/utils.py), ISC License, Copyright (c) 2013--2026,
      |# librosa development team. Algorithm: D. Ellis, "Beat Tracking by Dynamic
      |# Programming", JNMR 2007. Rewritten in NumPy (no numba); static tempo only;
      |# spectra computed in blocks, with every step's kept for the global level, so the
      |# operator caps the steps per file (MaxBeatSteps). Output matches librosa.beat.beat_track,
      |# except that audio without note starts gets no beats (see beat_times).
      |
      |
      |def tempo_estimate(env, fps, start_bpm=120.0, std_bpm=1.0, ac_size=8.0, max_tempo=320.0):
      |    # Global tempo (BPM) from an onset envelope sampled at `fps` frames/s.
      |    win = max(int(np.floor(ac_size * fps)), 2)  # librosa.time_to_frames
      |    n = len(env)
      |    # Centre the autocorrelation windows (np.pad linear_ramp to 0, as librosa does).
      |    padded = np.pad(env.astype(np.float64), (win // 2, win // 2), mode="linear_ramp", end_values=(0, 0))
      |    hann = 0.5 - 0.5 * np.cos(2 * np.pi * np.arange(win) / win)  # periodic Hann (fftbins=True)
      |    acc = np.zeros(win)
      |    nfft = 1 << int(np.ceil(np.log2(2 * win - 1)))
      |    for s in range(0, n, 2048):
      |        e = min(n, s + 2048)
      |        frames = np.lib.stride_tricks.sliding_window_view(padded, win)[s:e] * hann
      |        ac = np.fft.irfft(np.abs(np.fft.rfft(frames, n=nfft, axis=1)) ** 2, n=nfft, axis=1)[:, :win]
      |        peak = np.max(np.abs(ac), axis=1, keepdims=True)
      |        ac = np.where(peak > np.finfo(float).tiny, ac / np.where(peak > 0, peak, 1), 0.0)
      |        acc += ac.sum(axis=0)
      |    tg = acc / n
      |    lags = np.arange(win)
      |    with np.errstate(divide="ignore"):
      |        bpms = 60.0 * fps / lags
      |    bpms[0] = np.inf
      |    logprior = -0.5 * ((np.log2(bpms) - np.log2(start_bpm)) / std_bpm) ** 2
      |    max_idx = int(np.argmax(bpms < max_tempo))
      |    logprior[:max_idx] = -np.inf
      |    best = int(np.argmax(np.log1p(1e6 * tg) + logprior))
      |    return float(bpms[best])
      |
      |
      |def _localmax(x):
      |    left = np.concatenate([[x[0]], x[:-1]])
      |    right = np.concatenate([x[1:], [x[-1]]])
      |    return (x > left) & (x >= right)
      |
      |
      |def beat_track(env, fps, bpm=None, tightness=100.0, trim=True):
      |    # -> (bpm, beat frame indices). Same decisions as librosa.beat.beat_track.
      |    env = np.asarray(env, dtype=np.float64)
      |    if not env.any():
      |        return 0.0, np.array([], dtype=int)
      |    if bpm is None:
      |        bpm = tempo_estimate(env, fps)
      |    fpb = float(np.round(fps * 60.0 / bpm))
      |    norm = env / (env.std(ddof=1) + np.finfo(env.dtype).tiny)
      |    # Local score: same-mode convolution with a Gaussian of width fpb/32.
      |    k = np.arange(-fpb, fpb + 1)
      |    window = np.exp(-0.5 * (k * 32.0 / fpb) ** 2)
      |    local = np.convolve(norm, window, mode="full")
      |    K = len(window)
      |    local = local[K // 2 : K // 2 + len(norm)]
      |    n = len(local)
      |    # Dynamic programme over predecessors in [i - 2 fpb, i - fpb/2].
      |    cum = np.empty(n)
      |    back = np.full(n, -1, dtype=np.int64)
      |    thresh = 0.01 * local.max()
      |    first_beat = True
      |    near = max(1, int(round(fpb / 2)))  # a predecessor is always an earlier frame
      |    logfpb = np.log(fpb)
      |    for i in range(n):
      |        hi = i - near
      |        lo = max(0, int(i - 2 * fpb))
      |        if hi >= 0 and hi >= lo:
      |            locs = np.arange(hi, lo - 1, -1)  # descending, as librosa's loop (ties -> later beat)
      |            scores = cum[locs] - tightness * (np.log(i - locs) - logfpb) ** 2
      |            j = int(np.argmax(scores))
      |            best, loc = scores[j], int(locs[j])
      |        else:
      |            best, loc = -np.inf, -1
      |        cum[i] = local[i] + best if loc >= 0 else local[i]
      |        if first_beat and local[i] < thresh:
      |            back[i] = -1
      |        else:
      |            back[i] = loc
      |            first_beat = False
      |    # Last beat: latest local maximum of the cumulative score above half its median.
      |    lm = _localmax(cum)
      |    thr = 0.5 * np.median(cum[lm]) if lm.any() else 0.0
      |    tail = n - 1
      |    for m in range(n - 1, -1, -1):
      |        if lm[m] and cum[m] >= thr:
      |            tail = m
      |            break
      |    beats = np.zeros(n, dtype=bool)
      |    m = tail
      |    while m >= 0:
      |        beats[m] = True
      |        m = back[m] if back[m] < m else -1
      |    # Trim weak leading/trailing frames, as librosa does.
      |    w = 0.5 - 0.5 * np.cos(2 * np.pi * np.arange(5) / 4)  # np.hanning(5)
      |    smooth = np.convolve(local[beats], w)[len(w) // 2 : len(local) + len(w) // 2]
      |    threshold = 0.5 * np.sqrt(np.mean(smooth**2)) if trim else 0.0
      |    m = 0
      |    while m < n and local[m] <= threshold:
      |        beats[m] = False
      |        m += 1
      |    m = n - 1
      |    while m >= 0 and local[m] <= threshold:
      |        beats[m] = False
      |        m -= 1
      |    return bpm, np.flatnonzero(beats)
      |
      |
      |# ---- onset strength (librosa.onset.onset_strength with aggregate=np.median, its defaults)
      |
      |
      |def _hz_to_mel(f):
      |    f = np.asarray(f, dtype=np.float64)
      |    f_sp, min_log_hz = 200.0 / 3, 1000.0
      |    min_log_mel, logstep = min_log_hz / f_sp, np.log(6.4) / 27.0
      |    return np.where(f >= min_log_hz, min_log_mel + np.log(np.maximum(f, 1e-12) / min_log_hz) / logstep, f / f_sp)
      |
      |
      |def _mel_to_hz(m):
      |    m = np.asarray(m, dtype=np.float64)
      |    f_sp, min_log_hz = 200.0 / 3, 1000.0
      |    min_log_mel, logstep = min_log_hz / f_sp, np.log(6.4) / 27.0
      |    return np.where(m >= min_log_mel, min_log_hz * np.exp(logstep * (m - min_log_mel)), f_sp * m)
      |
      |
      |def mel_filterbank(sr, n_fft, n_mels=128, fmin=0.0, fmax=None):
      |    # Slaney-style mel filters with Slaney area normalisation (librosa.filters.mel defaults).
      |    fmax = sr / 2 if fmax is None else fmax
      |    fftfreqs = np.fft.rfftfreq(n_fft, 1.0 / sr)
      |    mel_f = _mel_to_hz(np.linspace(_hz_to_mel(fmin), _hz_to_mel(fmax), n_mels + 2))
      |    fdiff = np.diff(mel_f)
      |    ramps = np.subtract.outer(mel_f, fftfreqs)
      |    weights = np.zeros((n_mels, len(fftfreqs)), dtype=np.float32)
      |    for i in range(n_mels):
      |        lower = -ramps[i] / fdiff[i]
      |        upper = ramps[i + 2] / fdiff[i + 1]
      |        weights[i] = np.maximum(0, np.minimum(lower, upper))
      |    weights *= (2.0 / (mel_f[2 : n_mels + 2] - mel_f[:n_mels]))[:, None]
      |    return weights
      |
      |
      |def onset_strength(x, sr, n_fft=2048, hop=512, n_mels=128):
      |    # Median over mel bands of the rectified frame-to-frame rise in log-mel power.
      |    # Frame i is centred on sample i * hop. Computed in blocks of about 2M samples.
      |    x = np.asarray(x, np.float32)
      |    block = max(16, (1 << 21) // n_fft)
      |    half = n_fft // 2
      |    n_frames = 1 + len(x) // hop  # center=True, pad_mode="constant"
      |    window = (0.5 - 0.5 * np.cos(2 * np.pi * np.arange(n_fft) / n_fft)).astype(np.float32)  # periodic Hann
      |    basis = mel_filterbank(sr, n_fft, n_mels)
      |    db = np.empty((n_frames, n_mels), np.float32)
      |    for s in range(0, n_frames, block):
      |        e = min(n_frames, s + block)
      |        # Frames s..e-1 cover samples [s*hop - half, (e-1)*hop - half + n_fft); zero-pad only this slice.
      |        a0, b0 = s * hop - half, (e - 1) * hop - half + n_fft
      |        seg = np.zeros(b0 - a0, np.float32)
      |        lo, hi = max(0, a0), min(len(x), b0)
      |        if hi > lo:
      |            seg[lo - a0 : hi - a0] = x[lo:hi]
      |        block_frames = np.lib.stride_tricks.sliding_window_view(seg, n_fft)[::hop][: e - s]
      |        spec = np.fft.rfft(block_frames * window, axis=1)
      |        power = (spec.real**2 + spec.imag**2).astype(np.float32)
      |        db[s:e] = 10.0 * np.log10(np.maximum(1e-10, power @ basis.T))
      |    np.maximum(db, db.max() - 80.0, out=db)  # top_db=80 relative to the global maximum, in place
      |    env = np.empty(max(0, n_frames - 1), np.float32)
      |    for s in range(1, n_frames, block):  # rectified rise, then the median across bands, per block
      |        e = min(n_frames, s + block)
      |        env[s - 1 : e - 1] = np.median(np.maximum(0.0, db[s:e] - db[s - 1 : e - 1]), axis=1)
      |    pad = 1 + n_fft // (2 * hop)  # lag + centring shift, as librosa pads
      |    return np.concatenate([np.zeros(pad, np.float32), env])[:n_frames]
      |
      |
      |def beat_times(x, sr):
      |    # librosa.beat.beat_track(y=x, sr=sr, units='time') without librosa, on the event
      |    # grid (librosa's own 2048/512 at 22,050 Hz). Audio with fewer than 4 note starts, or
      |    # fewer than one every 2 s, has no pulse to follow, so it gets no beats; librosa
      |    # would still place them on noise. (The sparsest drum loop tested has one every 1.7 s.)
      |    n_fft, hop = event_grid(sr)
      |    if len(onsets(x, sr)) < max(4, len(x) / sr / 2):
      |        return 0.0, np.array([])
      |    env = onset_strength(x, sr, n_fft=n_fft, hop=hop)
      |    bpm, fr = beat_track(env, sr / hop)
      |    return bpm, fr * hop / sr""".stripMargin
}
