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
  * Python that measures waveform, rms, zcr, pitch_hz and onsets on decoded samples, for
  * [[org.apache.texera.amber.operator.audio.AudioAttributeExtractorOpDesc]].
  *
  * rms, zcr and pitch_hz use Frame Length / Hop Length frames, taken 64 at a time so memory
  * stays bounded whatever the duration; pitch comes from normalized autocorrelation. Onsets use
  * a fixed ~23 ms grid and combine a rise in energy with spectral flux. `cap_points` and
  * `spread` keep each file within Max Points.
  *
  * Fixed module-level Python, spliced as is into the descriptor's template: settings reach it
  * as function arguments, never as text inside it.
  */
object FeaturesCodegen {

  val python: String =
    """# ---------------------------------------------------------------- framing
      |
      |
      |def frames(x, frame_length, hop):
      |    if len(x) < frame_length:
      |        x = np.pad(x, (0, frame_length - len(x)))
      |    return np.lib.stride_tricks.sliding_window_view(x, frame_length)[::hop]
      |
      |
      |def frame_times(n, frame_length, hop, sr):
      |    return (np.arange(n) * hop + frame_length / 2) / sr
      |
      |
      |CHUNK_FRAMES = 64  # frames processed at once; bounds memory whatever the duration
      |
      |
      |def chunked(f):
      |    # Fewer frames per step for long windows, so a step stays near 1M samples.
      |    step = max(4, min(CHUNK_FRAMES, (1 << 20) // max(1, f.shape[1])))
      |    for i in range(0, len(f), step):
      |        yield f[i : i + step]
      |
      |
      |def event_grid(sr):
      |    # (frame, hop) for onsets and beats: 2048/512 at 22,050 Hz, the same span in time
      |    # at other rates. Their thresholds are tuned for this step, so it is not a setting.
      |    hop = max(8, int(round(sr * 512 / 22050)))
      |    return 4 * hop, hop
      |
      |
      |def spread(t, max_points, what):
      |    # At most max_points event times, spread over the whole file; plus a warning if cut.
      |    if len(t) <= max_points:
      |        return t, []
      |    keep = np.unique(np.linspace(0, len(t) - 1, max_points).round().astype(int))
      |    return t[keep], [f"showing {len(keep)} of {len(t)} {what}, spread over the file; raise Max Points for all"]
      |
      |
      |def cap_points(t, y, max_points, how):
      |    # Bucket a series down to <= max_points rows (mean, nanmedian or max-abs).
      |    n = len(t)
      |    if n <= max_points:
      |        return t, y
      |    edges = np.linspace(0, n, max_points + 1).astype(int)
      |    tt = np.empty(max_points)
      |    yy = np.empty(max_points)
      |    for i in range(max_points):
      |        a, b = edges[i], edges[i + 1]
      |        seg = y[a:b]
      |        tt[i] = t[a:b].mean()
      |        if how == "maxabs":
      |            yy[i] = seg[np.argmax(np.abs(seg))]
      |        elif how == "nanmedian":
      |            yy[i] = np.nan if np.all(np.isnan(seg)) else np.nanmedian(seg)
      |        else:
      |            yy[i] = seg.mean()
      |    return tt, yy
      |
      |
      |# ---------------------------------------------------------------- features
      |
      |
      |def waveform(x, sr, max_points):
      |    # Keep the loudest sample of each slice (and its own time), so peaks survive.
      |    n = len(x)
      |    if n <= max_points:
      |        return np.arange(n) / sr, x.astype(np.float64)
      |    edges = np.linspace(0, n, max_points + 1).astype(np.int64)
      |    t = np.empty(max_points)
      |    y = np.empty(max_points)
      |    for i in range(max_points):
      |        a, b = edges[i], edges[i + 1]
      |        j, peak = a, -1.0
      |        # One slice can span the whole file (Max Points 1); scan it in bounded steps.
      |        for s in range(a, b, STREAM_FRAMES):
      |            k = s + int(np.argmax(np.abs(x[s : min(b, s + STREAM_FRAMES)])))
      |            if abs(float(x[k])) > peak:
      |                j, peak = k, abs(float(x[k]))
      |        t[i], y[i] = j / sr, x[j]
      |    return t, y
      |
      |
      |def rms(x, sr, frame_length, hop):
      |    f = frames(x, frame_length, hop)
      |    y = np.concatenate([np.sqrt((c.astype(np.float64) ** 2).mean(axis=1)) for c in chunked(f)])
      |    return frame_times(len(f), frame_length, hop, sr), y
      |
      |
      |def zcr(x, sr, frame_length, hop):
      |    f = frames(x, frame_length, hop)
      |    y = np.concatenate([(np.diff(np.signbit(c), axis=1) != 0).mean(axis=1) for c in chunked(f)])
      |    return frame_times(len(f), frame_length, hop, sr), y
      |
      |
      |def pitch(x, sr, frame_length, hop, fmin=60.0, fmax=1000.0, voicing=0.5, silence_db=-50.0):
      |    f = frames(x, frame_length, hop)
      |    y = np.concatenate([_pitch_block(c, sr, frame_length, fmin, fmax, voicing, silence_db) for c in chunked(f)])
      |    return frame_times(len(f), frame_length, hop, sr), y
      |
      |
      |def _pitch_block(f, sr, frame_length, fmin, fmax, voicing, silence_db):
      |    # Normalized autocorrelation F0 per frame; NaN where unvoiced or silent.
      |    #
      |    # Picks the FIRST lag whose normalized autocorrelation is within 90 % of the
      |    # best peak, which avoids the classic octave-down error, then refines the
      |    # lag by parabolic interpolation.
      |    f = f.astype(np.float64)
      |    f = f - f.mean(axis=1, keepdims=True)
      |    energy = (f**2).sum(axis=1)
      |    spec = np.fft.rfft(f, n=2 * frame_length, axis=1)
      |    ac = np.fft.irfft(np.abs(spec) ** 2, axis=1)[:, :frame_length]
      |    # Unbiased-ish normalisation: divide by the zero-lag value and by the overlap length.
      |    lags = np.arange(frame_length)
      |    with np.errstate(invalid="ignore", divide="ignore"):
      |        nac = ac / ac[:, :1] * (frame_length / (frame_length - lags))
      |    # Search a few lags beyond each end of the range, so notes at its limits are not
      |    # pinned to the edge; results well outside [fmin, fmax] are dropped further down.
      |    lo, hi = max(2, int(sr / fmax) - 1), min(frame_length - 2, int(sr / fmin) + 4)
      |    if hi - lo < 3:  # window too short for this rate (the operator rejects such settings)
      |        return np.full(len(f), np.nan)
      |    full = np.nan_to_num(nac, nan=-1.0)
      |    # Skip the zero-lag peak's own slope: candidates start after its first valley,
      |    # otherwise a low note's broad zero-lag peak still clears the 90 % bar at `lo`.
      |    rising = np.diff(full[:, lo - 1 : hi], axis=1) > 0
      |    valley = lo + np.argmax(rising, axis=1)
      |    valley[~rising.any(axis=1)] = hi
      |    lags_in = np.arange(lo, hi)
      |    win = np.where(lags_in[None, :] >= valley[:, None], full[:, lo:hi], -1.0)
      |    best = win.max(axis=1)
      |    good = win >= 0.9 * best[:, None]
      |    first = lo + np.argmax(good, axis=1)
      |    # Walk to the local maximum right of the first qualifying lag. Low notes have
      |    # broad peaks (about a period/10 wide), so walk until no frame moves.
      |    idx = np.arange(len(f))
      |    for _ in range(hi - lo):
      |        step = (nac[idx, np.minimum(first + 1, frame_length - 1)] > nac[idx, first]) & (first < hi - 1)
      |        if not step.any():
      |            break
      |        first = first + step.astype(int)
      |    a, b, c = nac[idx, first - 1], nac[idx, first], nac[idx, np.minimum(first + 1, frame_length - 1)]
      |    denom = a - 2 * b + c
      |    with np.errstate(invalid="ignore", divide="ignore"):
      |        shift = np.where(np.abs(denom) > 1e-12, 0.5 * (a - c) / denom, 0.0)
      |    f0 = sr / (first + np.clip(shift, -0.5, 0.5))
      |    level_db = 10 * np.log10(energy / frame_length + 1e-20)
      |    # A peak pinned to the search window's edge is the window talking, not the signal.
      |    at_edge = (first <= lo) | (first >= hi - 1)
      |    out_of_range = (f0 < fmin * 0.97) | (f0 > fmax * 1.03)
      |    # A note above fmax has its period below the window, so the search lands on a
      |    # multiple of it (1100 Hz reads as 550 Hz). Its autocorrelation is then just as
      |    # high at a fraction of the chosen lag, which an in-range note's never is.
      |    folded = np.zeros(len(f), dtype=bool)
      |    for k in (2, 3, 4, 5):
      |        sub = np.rint(first / k).astype(int)
      |        near = np.max([nac[idx, np.clip(sub + d, 1, frame_length - 1)] for d in (-1, 0, 1)], axis=0)
      |        folded |= (sub >= 2) & (near >= 0.9 * nac[idx, first])
      |    f0[(best < voicing) | (level_db < silence_db) | at_edge | out_of_range | folded] = np.nan
      |    # Estimates of a note at a limit stray a little past it (a 1000 Hz click reads up to
      |    # 1008 Hz), so the margin kept above is reported at the limit, never outside the range.
      |    return np.clip(f0, fmin, fmax)
      |
      |
      |def onset_envelope(x, sr, frame_length, hop):
      |    # Spectral flux on a log-magnitude spectrogram (half-wave rectified).
      |    # Returns (times, flux counted only while energy rises, raw flux).
      |    f = frames(x, frame_length, hop)
      |    win = np.hanning(frame_length).astype(np.float32)
      |    bins = frame_length // 2 + 1
      |    raws, gateds = [np.zeros(1)], [np.zeros(1)]
      |    prev_mag = prev_energy = None
      |    for c in chunked(f):
      |        spec = np.abs(np.fft.rfft(c * win, axis=1)).astype(np.float32)
      |        mag = np.log1p(100 * spec)
      |        # A fading note spreads into neighbouring bins and looks like new energy;
      |        # count flux only where the frame's total energy actually rises.
      |        energy = np.log10((spec.astype(np.float64) ** 2).sum(axis=1) + 1e-12)
      |        if prev_mag is not None:
      |            mag = np.vstack([prev_mag, mag])
      |            energy = np.concatenate([[prev_energy], energy])
      |        flux = np.maximum(np.diff(mag, axis=0), 0).sum(axis=1)
      |        raws.append(flux / bins)
      |        gateds.append(flux * (np.diff(energy) > 0) / bins)
      |        prev_mag, prev_energy = mag[-1:], energy[-1]
      |    raw, gated = np.concatenate(raws), np.concatenate(gateds)
      |    # Flux i compares frames i-1 and i. An attack raises the windowed energy fastest
      |    # where the Hann window is steepest, a quarter frame after the centre, so that is
      |    # where it is stamped (measured bias before this: about -27 ms at 2048/512).
      |    t = (np.arange(len(raw)) * hop + 3 * frame_length / 4 - hop / 2) / sr
      |    return t, gated, raw
      |
      |
      |ONSET_FLOOR = 0.05  # mean log-magnitude rise per bin; slow swells sit near 0.007, soft notes near 0.15
      |ONSET_MADS = 6.0  # above the background; at 3, steady noise alone gave about one onset a second
      |MIN_GAP_S = 0.05
      |
      |
      |def _pick(peak, strength, t):
      |    # Peak indices at least MIN_GAP_S apart, keeping the stronger of two close ones.
      |    picks = []
      |    for i in np.flatnonzero(peak):
      |        if not picks or t[i] - t[picks[-1]] >= MIN_GAP_S:
      |            picks.append(i)
      |        elif strength[i] > strength[picks[-1]]:
      |            picks[-1] = i
      |    return np.array(picks, dtype=int)
      |
      |
      |def flux_onsets(x, sr, frame_length, hop):
      |    # Note starts as peaks in spectral flux: finds a note that begins while others ring.
      |    t, env, raw = onset_envelope(x, sr, frame_length, hop)
      |    if env.max() <= 0:
      |        return np.array([])
      |    # Threshold from the ungated flux (the background level, noise included),
      |    # peaks from the gated flux (energy actually rising), plus an absolute floor.
      |    w = max(3, int(0.5 * sr / hop) | 1)
      |    windows = np.lib.stride_tricks.sliding_window_view(np.pad(raw, w // 2, mode="edge"), w)
      |    med = np.empty(len(raw))
      |    for s in range(0, len(raw), 2048):  # np.median copies its input; keep each copy small
      |        med[s : s + 2048] = np.median(windows[s : s + 2048], axis=1)
      |    mad = np.median(np.abs(raw - np.median(raw))) + 1e-9
      |    thr = np.maximum(med + ONSET_MADS * mad, ONSET_FLOOR)
      |    peak = (env > thr) & (env >= np.roll(env, 1)) & (env >= np.roll(env, -1))
      |    return t[_pick(peak, env, t)]
      |
      |
      |def energy_onsets(x, sr, frame_length, hop):
      |    # Note starts as jumps in frame RMS well above its own frame-to-frame jitter (median
      |    # plus 6 MADs of the change, and at least 0.005): finds a note that rises out of silence
      |    # or steady noise, slow attacks included, which flux can miss.
      |    f = frames(x, frame_length, hop)
      |    level = np.concatenate([np.sqrt((c.astype(np.float64) ** 2).mean(axis=1)) for c in chunked(f)])
      |    if len(level) < 3:
      |        return np.array([])
      |    rise = np.diff(level, prepend=level[0])
      |    med = np.median(rise)
      |    thr = max(0.005, med + 6.0 * np.median(np.abs(rise - med)))
      |    peak = (rise > thr) & (rise >= np.roll(rise, 1)) & (rise > np.roll(rise, -1))
      |    peak[0] = peak[-1] = False
      |    t = frame_times(len(level), frame_length, hop, sr)
      |    return t[_pick(peak, rise, t)]
      |
      |
      |def onsets(x, sr):
      |    # Both detectors on the fixed event grid. Where both fire within MIN_GAP_S, the
      |    # flux time is kept (it is the more precise: about -3 ms against -20 ms).
      |    fl, hop = event_grid(sr)
      |    flux, rise = flux_onsets(x, sr, fl, hop), energy_onsets(x, sr, fl, hop)
      |    if not len(flux) or not len(rise):
      |        return np.sort(np.concatenate([flux, rise]).astype(np.float64))
      |    # Distance from each energy onset to its nearest flux onset (both are sorted).
      |    i = np.searchsorted(flux, rise)
      |    nearest = np.minimum(np.abs(rise - flux[np.maximum(i - 1, 0)]), np.abs(rise - flux[np.minimum(i, len(flux) - 1)]))
      |    return np.sort(np.concatenate([flux, rise[nearest >= MIN_GAP_S]]).astype(np.float64))""".stripMargin
}
