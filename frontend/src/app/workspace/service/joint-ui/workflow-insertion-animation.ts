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

import * as joint from "jointjs";

interface Entrance {
  element: SVGElement;
  effects: Animation[];
  halo?: SVGElement;
  cancel: () => void;
}

/** View-only feedback: never animate model coordinates, port geometry, or saved state. */
export class WorkflowInsertionAnimation {
  private frame: number | undefined;
  private pending = new Map<joint.dia.Cell.ID, joint.dia.Cell>();
  private active = new Map<joint.dia.Cell.ID, Entrance>();

  constructor(
    private readonly paper: joint.dia.Paper,
    private readonly isReloading: () => boolean,
    private readonly isOperator: (id: string) => boolean
  ) {
    paper.model.on("add", this.add, this);
    paper.model.on("remove", this.remove, this);
    paper.model.on("reset", this.clear, this);
    paper.on("render:done", this.rendered, this);
  }

  private add = (cell: joint.dia.Cell): void => {
    if (this.isReloading() || (!cell.isLink() && !this.isOperator(String(cell.id)))) return;
    if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;
    this.pending.set(cell.id, cell);
    this.schedule();
  };

  private schedule = (): void => {
    if (this.pending.size && this.frame === undefined) this.frame = requestAnimationFrame(this.flush);
  };

  private rendered = (): void => {
    if (!this.pending.size) return;
    if (this.frame !== undefined) cancelAnimationFrame(this.frame);
    // Start before the newly mounted views are painted, avoiding a full-opacity flash.
    this.flush();
  };

  private flush = (): void => {
    this.frame = undefined;
    if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) {
      this.clear();
      return;
    }
    let index = 0;
    for (const [id, cell] of this.pending) {
      if (!this.paper.model.getCell(id)) {
        this.pending.delete(id);
        continue;
      }
      const view = this.paper.findViewByModel(cell);
      // Async papers may not have mounted the view yet. render:done handles the next attempt.
      if (!view || !view.el.isConnected) continue;
      this.pending.delete(id);
      this.start(cell, view.el, Math.min(index++ * 25, 120));
    }
  };

  private start(cell: joint.dia.Cell, element: SVGElement, delay: number): void {
    if (typeof element.animate !== "function") return;
    this.cancel(cell.id);
    const effects = [
      element.animate([{ opacity: 0 }, { opacity: 1 }], {
        duration: 200,
        delay,
        easing: "ease-out",
        fill: "backwards",
      }),
    ];
    let halo: SVGElement | undefined;
    const body = element.querySelector(".body");
    if (cell.isElement() && body) {
      halo = body.cloneNode(false) as SVGElement;
      halo.removeAttribute("id");
      halo.setAttribute("class", "workflow-insertion-halo");
      halo.setAttribute("fill", "none");
      halo.setAttribute("stroke", "#5b8ff9");
      halo.setAttribute("stroke-width", "3");
      halo.setAttribute("pointer-events", "none");
      halo.style.transformBox = "fill-box";
      halo.style.transformOrigin = "center";
      body.parentNode!.appendChild(halo);
      effects.push(
        halo.animate(
          [
            { opacity: 0.7, transform: "scale(1)" },
            { opacity: 0, transform: "scale(1.08)" },
          ],
          {
            duration: 400,
            delay,
            easing: "ease-out",
            fill: "both",
          }
        )
      );
    }
    const cancel = () => this.cancel(cell.id);
    this.active.set(cell.id, { element, effects, halo, cancel });
    element.addEventListener("pointerdown", cancel, true);
    element.addEventListener("mousedown", cancel, true);
    // Use finish events rather than animation.finished promises, which reject on cancellation.
    let remaining = effects.length;
    effects.forEach(
      effect =>
        (effect.onfinish = () => {
          if (--remaining === 0) this.cancel(cell.id);
        })
    );
  }

  private remove = (cell: joint.dia.Cell): void => {
    this.pending.delete(cell.id);
    this.cancel(cell.id);
  };

  private cancel(id: joint.dia.Cell.ID): void {
    const entrance = this.active.get(id);
    if (!entrance) return;
    this.active.delete(id);
    entrance.element.removeEventListener("pointerdown", entrance.cancel, true);
    entrance.element.removeEventListener("mousedown", entrance.cancel, true);
    entrance.effects.forEach(effect => {
      effect.onfinish = null;
      effect.cancel();
    });
    entrance.halo?.remove();
  }

  private clear = (): void => {
    if (this.frame !== undefined) cancelAnimationFrame(this.frame);
    this.frame = undefined;
    this.pending.clear();
    for (const id of this.active.keys()) this.cancel(id);
  };

  dispose(): void {
    this.paper.model.off("add", this.add, this);
    this.paper.model.off("remove", this.remove, this);
    this.paper.model.off("reset", this.clear, this);
    this.paper.off("render:done", this.rendered, this);
    this.clear();
  }
}
