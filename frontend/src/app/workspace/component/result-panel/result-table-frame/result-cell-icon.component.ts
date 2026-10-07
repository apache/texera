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

import { ChangeDetectionStrategy, Component, Input } from "@angular/core";
import { CloudDownloadOutline, EyeOutline, PlayCircleOutline, SoundOutline } from "@ant-design/icons-angular/icons";

function iconMask(svg: string): string {
  // Library definitions are inline SVG; a standalone image needs the namespace.
  const image = svg.includes("xmlns=") ? svg : svg.replace("<svg", '<svg xmlns="http://www.w3.org/2000/svg"');
  return `url("data:image/svg+xml,${encodeURIComponent(image)}")`;
}

const CELL_ICON_MASKS = {
  "cloud-download": iconMask(CloudDownloadOutline.icon),
  "play-circle": iconMask(PlayCircleOutline.icon),
  sound: iconMask(SoundOutline.icon),
  eye: iconMask(EyeOutline.icon),
};

// Thousands of NzIcon load callbacks trigger quadratic change detection in a
// large result page. Reuse the same library SVG definitions as cached CSS masks:
// the browser loads/caches them without Angular callbacks, preserving currentColor.
@Component({
  selector: "texera-result-cell-icon",
  template: '<span [style.mask-image]="maskImage"></span>',
  host: { role: "img", "[attr.aria-label]": "type" },
  styles: [
    ":host { display: inline-flex; }",
    "span { display: inline-block; width: 1em; height: 1em; background-color: currentColor; mask-size: contain; mask-repeat: no-repeat; mask-position: center; }",
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ResultCellIconComponent {
  @Input({ required: true }) type!: keyof typeof CELL_ICON_MASKS;

  get maskImage(): string | null {
    return Object.hasOwn(CELL_ICON_MASKS, this.type) ? CELL_ICON_MASKS[this.type] : null;
  }
}
