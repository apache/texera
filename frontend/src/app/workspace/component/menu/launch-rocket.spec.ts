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

import {
  launchRocket,
  ROCKET_CYCLES,
  ROCKET_DURATION_MS,
  ROCKET_ORBIT_FRACTION,
  ROCKET_SIZE_PX,
  ROCKET_STEPS_PER_CYCLE,
} from "./launch-rocket";

describe("launchRocket", () => {
  let button: HTMLElement;
  let animateSpy: jasmine.Spy;
  let fakeAnimation: { onfinish: (() => void) | null };

  beforeEach(() => {
    button = document.createElement("button");
    Object.assign(button.style, {
      position: "fixed",
      left: "1000px",
      top: "20px",
      width: "80px",
      height: "32px",
    });
    document.body.appendChild(button);

    fakeAnimation = { onfinish: null };
    animateSpy = spyOn(Element.prototype, "animate").and.returnValue(fakeAnimation as unknown as Animation);
  });

  afterEach(() => {
    document.querySelectorAll(".texera-rocket-launch").forEach(el => el.remove());
    button.remove();
  });

  function getRocket(): HTMLElement | null {
    return document.querySelector(".texera-rocket-launch");
  }

  it("returns null and creates nothing if button is null", () => {
    expect(launchRocket(null)).toBeNull();
    expect(getRocket()).toBeNull();
    expect(animateSpy).not.toHaveBeenCalled();
  });

  it("appends a 🚀 element to document.body when given a button", () => {
    const result = launchRocket(button);
    expect(result).not.toBeNull();
    const rocket = getRocket();
    expect(rocket).not.toBeNull();
    expect(rocket!.textContent).toBe("🚀");
    expect(rocket!.parentElement).toBe(document.body);
  });

  it("positions the rocket centered at the button's center", () => {
    const rect = button.getBoundingClientRect();
    launchRocket(button);
    const rocket = getRocket()!;
    expect(rocket.style.position).toBe("fixed");
    expect(parseFloat(rocket.style.left)).toBeCloseTo(rect.left + rect.width / 2 - ROCKET_SIZE_PX / 2, 1);
    expect(parseFloat(rocket.style.top)).toBeCloseTo(rect.top + rect.height / 2 - ROCKET_SIZE_PX / 2, 1);
    expect(rocket.style.fontSize).toBe(`${ROCKET_SIZE_PX}px`);
    expect(rocket.style.pointerEvents).toBe("none");
  });

  it("calls Element.prototype.animate with the configured duration", () => {
    launchRocket(button);
    expect(animateSpy).toHaveBeenCalledTimes(1);
    const [, options] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    expect(options.duration).toBe(ROCKET_DURATION_MS);
  });

  it("emits cycles*steps + 1 orbit keyframes plus one shoot-up keyframe", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    // totalSteps + 1 sampled orbit points (closed loop), plus 1 final shoot-up keyframe
    expect(keyframes.length).toBe(ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE + 1 + 1);
  });

  it("starts the orbit at translate(0px, 0px) (rocket at button center)", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    const first = keyframes[0].transform as string;
    expect(first).toContain("translate(0px, 0px)");
    expect(keyframes[0].offset).toBe(0);
    expect(keyframes[0].opacity).toBe(1);
  });

  it("ends the orbit at translate(~0, ~0) (closed counterclockwise loop)", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    const totalSteps = ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE;
    const lastOrbit = keyframes[totalSteps].transform as string;
    const m = /translate\(([^,]+)px,\s*([^)]+)px\)/.exec(lastOrbit);
    expect(m).toBeTruthy();
    expect(parseFloat(m![1])).toBeCloseTo(0, 5);
    expect(parseFloat(m![2])).toBeCloseTo(0, 5);
    expect(keyframes[totalSteps].offset).toBeCloseTo(ROCKET_ORBIT_FRACTION, 5);
  });

  it("rotates monotonically (counterclockwise) across orbit keyframes", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    const totalSteps = ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE;
    const rotations: number[] = [];
    for (let i = 0; i <= totalSteps; i++) {
      const t = keyframes[i].transform as string;
      const m = /rotate\((-?\d+(?:\.\d+)?)deg\)/.exec(t);
      expect(m).toBeTruthy();
      rotations.push(parseFloat(m![1]));
    }
    // strictly decreasing → CCW per our convention (R = -45 - θ*180/π)
    for (let i = 1; i < rotations.length; i++) {
      expect(rotations[i]).toBeLessThan(rotations[i - 1]);
    }
    // total angular distance is 360° * cycles
    expect(rotations[0] - rotations[rotations.length - 1]).toBeCloseTo(360 * ROCKET_CYCLES, 3);
  });

  it("traces a circle to the LEFT of the start point (dx is never positive)", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    const totalSteps = ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE;
    for (let i = 0; i <= totalSteps; i++) {
      const t = keyframes[i].transform as string;
      const m = /translate\(([^,]+)px,\s*([^)]+)px\)/.exec(t);
      expect(m).toBeTruthy();
      const dx = parseFloat(m![1]);
      // dx = radius*(cos θ - 1) is ≤ 0 for all θ (allow tiny floating-point slack)
      expect(dx).toBeLessThanOrEqual(1e-6);
    }
  });

  it("shoot-up keyframe ends opacity 0 above the viewport, with rocket pointing up", () => {
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    const last = keyframes[keyframes.length - 1];
    expect(last.opacity).toBe(0);
    expect(last.offset).toBe(1);
    const t = last.transform as string;
    const tm = /translate\(0px,\s*(-?\d+(?:\.\d+)?)px\)/.exec(t);
    expect(tm).toBeTruthy();
    expect(parseFloat(tm![1])).toBeLessThan(0); // upward (negative Y)
    // Final rotation equals -45 - 360*cycles, which is congruent to -45 mod 360 (pointing up)
    const rm = /rotate\((-?\d+(?:\.\d+)?)deg\)/.exec(t);
    expect(rm).toBeTruthy();
    expect(parseFloat(rm![1])).toBe(-45 - 360 * ROCKET_CYCLES);
  });

  it("removes the rocket element when the animation finishes", () => {
    launchRocket(button);
    expect(getRocket()).not.toBeNull();
    expect(typeof fakeAnimation.onfinish).toBe("function");
    fakeAnimation.onfinish!();
    expect(getRocket()).toBeNull();
  });

  it("creates an independent rocket per call (multiple clicks)", () => {
    launchRocket(button);
    launchRocket(button);
    launchRocket(button);
    expect(document.querySelectorAll(".texera-rocket-launch").length).toBe(3);
    expect(animateSpy).toHaveBeenCalledTimes(3);
  });

  it("shrinks orbit radius when the button is close to the left edge of the viewport", () => {
    // Replace button with one positioned near the left edge
    button.style.left = "100px";
    launchRocket(button);
    const [keyframes] = animateSpy.calls.mostRecent().args as [Keyframe[], KeyframeAnimationOptions];
    // Find max |dx| across keyframes — that's 2*radius
    let maxDx = 0;
    const totalSteps = ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE;
    for (let i = 0; i <= totalSteps; i++) {
      const t = keyframes[i].transform as string;
      const m = /translate\(([^,]+)px,/.exec(t);
      const dx = Math.abs(parseFloat(m![1]));
      if (dx > maxDx) maxDx = dx;
    }
    const radius = maxDx / 2;
    // Cap is 140; for a button close to the left edge we expect a smaller orbit.
    expect(radius).toBeLessThan(140);
    // But never smaller than the floor of 40.
    expect(radius).toBeGreaterThanOrEqual(40);
  });
});
