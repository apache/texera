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

export const MIN_OVERBOX_WIDTH = 120;
export const MIN_OVERBOX_HEIGHT = 80;

/** A JointJS tool that previews resizing locally and persists only the final dimensions. */
export function createOverboxResizeTool(onResizeEnd: (width: number, height: number) => void): joint.dia.ToolView {
  return new (class extends joint.elementTools.Control {
    private finalSize: { width: number; height: number } | undefined;

    protected override getPosition(view: joint.dia.ElementView): joint.dia.Point {
      const size = view.model.size();
      return { x: size.width, y: size.height };
    }

    protected override setPosition(view: joint.dia.ElementView, coordinates: joint.g.Point): void {
      this.finalSize = {
        width: Math.max(MIN_OVERBOX_WIDTH, Math.round(coordinates.x)),
        height: Math.max(MIN_OVERBOX_HEIGHT, Math.round(coordinates.y)),
      };
      view.model.resize(this.finalSize.width, this.finalSize.height);
    }

    protected override onPointerUp(event: joint.dia.Event): void {
      super.onPointerUp(event);
      if (this.finalSize) onResizeEnd(this.finalSize.width, this.finalSize.height);
      this.finalSize = undefined;
    }
  })({
    selector: "body",
    handleAttributes: {
      r: 7,
      fill: "#ffffff",
      stroke: "#1677ff",
      strokeWidth: 2,
      cursor: "nwse-resize",
    },
  });
}
