/**
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

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { UserConfigService } from "../user/config/user-config.service";
import { UserService } from "../user/user.service";
import { ThemeService } from "../theme/theme.service";
import { BUILTIN_THEMES } from "../theme/themes";

const MOTION_KEY = "ui.motion";
const SOUND_KEY = "ui.sound";

/**
 * Owns the "delight layer" preferences (motion + sound) and the runtime
 * primitives that depend on them: confetti, success/fail chimes, and the
 * Konami easter egg.
 *
 * Defaults:
 *   - motion: on (unless prefers-reduced-motion is set)
 *   - sound:  off (don't startle people in a quiet office)
 *
 * Persistence mirrors ThemeService — user-config when logged in, otherwise
 * localStorage. Errors are swallowed; preferences are non-critical.
 */
@Injectable({ providedIn: "root" })
export class MotionService {
  private readonly motion$ = new BehaviorSubject<boolean>(this.systemMotionDefault());
  private readonly sound$ = new BehaviorSubject<boolean>(false);
  private audioCtx: AudioContext | null = null;

  constructor(
    private readonly userConfig: UserConfigService,
    private readonly userService: UserService,
    private readonly themeService: ThemeService
  ) {
    this.loadPersisted();
    this.userService.userChanged().subscribe(() => this.loadPersisted());
    this.installKonamiListener();
  }

  motionEnabled(): Observable<boolean> { return this.motion$.asObservable(); }
  soundEnabled(): Observable<boolean> { return this.sound$.asObservable(); }
  isMotionEnabled(): boolean { return this.motion$.value; }
  isSoundEnabled(): boolean { return this.sound$.value; }

  setMotionEnabled(v: boolean): void {
    if (v === this.motion$.value) return;
    this.motion$.next(v);
    this.persist(MOTION_KEY, v);
  }

  setSoundEnabled(v: boolean): void {
    if (v === this.sound$.value) return;
    this.sound$.next(v);
    this.persist(SOUND_KEY, v);
  }

  /**
   * Fire a quick confetti burst from the center of the viewport.
   * Cheap canvas implementation — no library dep, no DOM thrash.
   * No-op when motion is disabled.
   */
  confetti(opts?: { count?: number; durationMs?: number; origin?: { x: number; y: number } }): void {
    if (!this.motion$.value || typeof document === "undefined") return;
    const count = opts?.count ?? 120;
    const duration = opts?.durationMs ?? 1800;
    const origin = opts?.origin ?? { x: window.innerWidth / 2, y: window.innerHeight / 2 };

    const canvas = document.createElement("canvas");
    canvas.style.cssText =
      "position:fixed;inset:0;pointer-events:none;z-index:99999;";
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    document.body.appendChild(canvas);
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      canvas.remove();
      return;
    }

    // Sample colors from the active theme so confetti matches.
    const css = getComputedStyle(document.documentElement);
    const palette = [
      css.getPropertyValue("--tx-primary").trim() || "#1890ff",
      css.getPropertyValue("--tx-success").trim() || "#52c41a",
      css.getPropertyValue("--tx-warning").trim() || "#faad14",
      css.getPropertyValue("--tx-info").trim() || "#13c2c2",
      css.getPropertyValue("--tx-danger").trim() || "#ff4d4f",
    ];

    type Particle = {
      x: number; y: number;
      vx: number; vy: number;
      rot: number; vrot: number;
      size: number; color: string;
      shape: 0 | 1; // 0 = rect, 1 = circle
    };
    const particles: Particle[] = [];
    for (let i = 0; i < count; i++) {
      const angle = Math.random() * Math.PI * 2;
      const speed = 4 + Math.random() * 9;
      particles.push({
        x: origin.x,
        y: origin.y,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed - 4,
        rot: Math.random() * Math.PI * 2,
        vrot: (Math.random() - 0.5) * 0.4,
        size: 5 + Math.random() * 7,
        color: palette[Math.floor(Math.random() * palette.length)],
        shape: Math.random() < 0.5 ? 0 : 1,
      });
    }

    const start = performance.now();
    const gravity = 0.22;
    const drag = 0.992;
    const step = (now: number) => {
      const t = now - start;
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      for (const p of particles) {
        p.vy += gravity;
        p.vx *= drag;
        p.vy *= drag;
        p.x += p.vx;
        p.y += p.vy;
        p.rot += p.vrot;
        const fade = Math.max(0, 1 - t / duration);
        ctx.globalAlpha = fade;
        ctx.save();
        ctx.translate(p.x, p.y);
        ctx.rotate(p.rot);
        ctx.fillStyle = p.color;
        if (p.shape === 0) {
          ctx.fillRect(-p.size / 2, -p.size / 4, p.size, p.size / 2);
        } else {
          ctx.beginPath();
          ctx.arc(0, 0, p.size / 2, 0, Math.PI * 2);
          ctx.fill();
        }
        ctx.restore();
      }
      ctx.globalAlpha = 1;
      if (t < duration) {
        requestAnimationFrame(step);
      } else {
        canvas.remove();
      }
    };
    requestAnimationFrame(step);
  }

  /**
   * Play a short tonal chord. WebAudio-synthesized so no asset shipping.
   * No-op when sound is disabled.
   */
  chime(kind: "success" | "fail"): void {
    if (!this.sound$.value || typeof window === "undefined") return;
    try {
      const ctx = this.audioCtx ?? new (window.AudioContext || (window as any).webkitAudioContext)();
      this.audioCtx = ctx;
      // success = ascending C major triad. fail = descending tritone.
      const notes = kind === "success" ? [523.25, 659.25, 783.99] : [466.16, 329.63];
      const start = ctx.currentTime;
      notes.forEach((freq, i) => {
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = kind === "success" ? "triangle" : "sawtooth";
        osc.frequency.value = freq;
        gain.gain.setValueAtTime(0, start + i * 0.08);
        gain.gain.linearRampToValueAtTime(0.16, start + i * 0.08 + 0.02);
        gain.gain.exponentialRampToValueAtTime(0.0001, start + i * 0.08 + 0.55);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start(start + i * 0.08);
        osc.stop(start + i * 0.08 + 0.6);
      });
    } catch {
      /* WebAudio unavailable; silently skip */
    }
  }

  /**
   * Briefly shake the viewport — used for execution failures. No-op when
   * motion is off.
   */
  shake(): void {
    if (!this.motion$.value || typeof document === "undefined") return;
    const root = document.documentElement;
    root.animate(
      [
        { transform: "translate(0,0)" },
        { transform: "translate(-6px, 2px)" },
        { transform: "translate(5px, -3px)" },
        { transform: "translate(-3px, 1px)" },
        { transform: "translate(0,0)" },
      ],
      { duration: 320, easing: "ease-in-out" }
    );
  }

  /* ---- private ---------------------------------------------------------- */

  private systemMotionDefault(): boolean {
    if (typeof window === "undefined" || !window.matchMedia) return true;
    return !window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }

  private loadPersisted(): void {
    const readLocal = (k: string): boolean | null => {
      if (typeof localStorage === "undefined") return null;
      const v = localStorage.getItem(k);
      if (v === null) return null;
      return v === "1" || v === "true";
    };
    const local = { m: readLocal(MOTION_KEY), s: readLocal(SOUND_KEY) };
    if (local.m !== null) this.motion$.next(local.m);
    if (local.s !== null) this.sound$.next(local.s);

    if (this.userService.isLogin()) {
      try {
        this.userConfig.fetchKey(MOTION_KEY).subscribe({
          next: v => {
            if (v != null) this.motion$.next(v === "1" || v === "true");
          },
          error: () => {},
        });
        this.userConfig.fetchKey(SOUND_KEY).subscribe({
          next: v => {
            if (v != null) this.sound$.next(v === "1" || v === "true");
          },
          error: () => {},
        });
      } catch {
        /* logged-out race — local fallback already applied */
      }
    }
  }

  private persist(key: string, value: boolean): void {
    const str = value ? "1" : "0";
    if (typeof localStorage !== "undefined") localStorage.setItem(key, str);
    if (this.userService.isLogin()) {
      try {
        this.userConfig.set(key, str).subscribe({ error: () => {} });
      } catch {
        /* logged out by the time we got here; local already saved */
      }
    }
  }

  /* ---- Konami code easter egg ------------------------------------------- */
  // ↑ ↑ ↓ ↓ ← → ← → B A — when matched, briefly forces Synthwave + a
  // confetti volley + a chord. Reverts to the user's chosen theme after.
  private readonly konami = [
    "ArrowUp", "ArrowUp", "ArrowDown", "ArrowDown",
    "ArrowLeft", "ArrowRight", "ArrowLeft", "ArrowRight",
    "b", "a",
  ];
  private konamiIdx = 0;

  private installKonamiListener(): void {
    if (typeof window === "undefined") return;
    window.addEventListener("keydown", e => {
      const expected = this.konami[this.konamiIdx];
      if (e.key.toLowerCase() === expected.toLowerCase()) {
        this.konamiIdx++;
        if (this.konamiIdx === this.konami.length) {
          this.konamiIdx = 0;
          this.partyMode();
        }
      } else {
        this.konamiIdx = e.key === this.konami[0] ? 1 : 0;
      }
    });
  }

  private partyMode(): void {
    const previous = this.themeService.getCurrent();
    const synthwave = BUILTIN_THEMES.find(t => t.id === "synthwave");
    if (synthwave) this.themeService.setTheme(synthwave);
    // Two confetti volleys spaced apart for a sustained burst.
    const oldMotion = this.motion$.value;
    this.motion$.next(true); // override during easter egg
    this.confetti({ count: 180 });
    setTimeout(() => this.confetti({ count: 140 }), 600);
    setTimeout(() => this.confetti({ count: 100 }), 1200);
    setTimeout(() => {
      this.themeService.setTheme(previous);
      this.motion$.next(oldMotion);
    }, 4500);
    const oldSound = this.sound$.value;
    this.sound$.next(true);
    this.chime("success");
    setTimeout(() => this.sound$.next(oldSound), 800);
  }
}
