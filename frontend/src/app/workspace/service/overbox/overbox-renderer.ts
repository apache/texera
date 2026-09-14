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
import { CommentBox } from "../../types/workflow-common.interface";
import { MIN_OVERBOX_HEIGHT, MIN_OVERBOX_WIDTH } from "./overbox-resize-tool";
import { normalizeOverboxColor } from "./overbox-colors";

function positionTitle(element: joint.dia.Element, width: number): void {
  element.removeAttr("label/refX");
  element.removeAttr("label/refY");
  element.attr({
    label: {
      x: width / 2,
      y: -10,
      textAnchor: "middle",
      textVerticalAnchor: "bottom",
    },
  });
}

/** Uses the existing annotation model, so frames share persistence, undo and collaboration. */
export function renderOverbox(element: joint.dia.Element, box: CommentBox): void {
  const frame = box.overbox;
  if (!frame) return;
  const color = normalizeOverboxColor(frame.color);
  const width = Number.isFinite(frame.width) ? Math.max(MIN_OVERBOX_WIDTH, frame.width) : 400;
  const height = Number.isFinite(frame.height) ? Math.max(MIN_OVERBOX_HEIGHT, frame.height) : 240;
  element.resize(width, height);
  element.set("z", -1);
  element.attr({
    body: {
      class: "body",
      fill: color,
      fillOpacity: 0.12,
      stroke: color,
      strokeOpacity: 1,
      strokeWidth: 3,
      rx: 8,
      ry: 8,
      pointerEvents: "stroke",
    },
    label: {
      class: "label overbox-title",
      text: frame.name,
      fill: color,
      fillOpacity: 1,
      fontSize: 16,
      fontWeight: 600,
      pointerEvents: "visiblePainted",
    },
  });
  positionTitle(element, width);
}
