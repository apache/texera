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

export const ROCKET_SIZE_PX = 72;
export const ROCKET_CYCLES = 3;
export const ROCKET_STEPS_PER_CYCLE = 24;
export const ROCKET_DURATION_MS = 3500;
export const ROCKET_ORBIT_FRACTION = 0.75;

/**
 * Spawn a 🚀 emoji at the given button's center and animate it in 3
 * counterclockwise loops to the left of the button, then accelerate
 * straight up off the top of the viewport. Returns the underlying
 * Animation (or null if no button was supplied).
 */
export function launchRocket(button: HTMLElement | null): Animation | null {
  if (!button) return null;
  const rect = button.getBoundingClientRect();
  const startX = rect.left + rect.width / 2;
  const startY = rect.top + rect.height / 2;
  const margin = 40;
  const spaceLeft = startX - margin;
  const radius = Math.max(40, Math.min(140, spaceLeft / 2 - 20));
  const totalSteps = ROCKET_CYCLES * ROCKET_STEPS_PER_CYCLE;
  const keyframes: Keyframe[] = [];
  for (let i = 0; i <= totalSteps; i++) {
    const theta = (2 * Math.PI * i) / ROCKET_STEPS_PER_CYCLE;
    const dx = -radius + radius * Math.cos(theta);
    const dy = -radius * Math.sin(theta);
    const rot = -45 - (theta * 180) / Math.PI;
    keyframes.push({
      transform: `translate(${dx}px, ${dy}px) rotate(${rot}deg) scale(1.1)`,
      opacity: 1,
      offset: (i / totalSteps) * ROCKET_ORBIT_FRACTION,
      easing: "linear",
    });
  }
  const endRot = -45 - 360 * ROCKET_CYCLES;
  keyframes.push({
    transform: `translate(0px, ${-(startY + 200)}px) rotate(${endRot}deg) scale(0.8)`,
    opacity: 0,
    offset: 1,
  });
  keyframes[totalSteps].easing = "cubic-bezier(0.4, 0, 1, 1)";
  const rocket = document.createElement("div");
  rocket.textContent = "🚀";
  rocket.className = "texera-rocket-launch";
  Object.assign(rocket.style, {
    position: "fixed",
    left: `${startX - ROCKET_SIZE_PX / 2}px`,
    top: `${startY - ROCKET_SIZE_PX / 2}px`,
    fontSize: `${ROCKET_SIZE_PX}px`,
    lineHeight: "1",
    pointerEvents: "none",
    zIndex: "9999",
  });
  document.body.appendChild(rocket);
  const anim = rocket.animate(keyframes, { duration: ROCKET_DURATION_MS });
  anim.onfinish = anim.oncancel = () => rocket.remove();
  return anim;
}
