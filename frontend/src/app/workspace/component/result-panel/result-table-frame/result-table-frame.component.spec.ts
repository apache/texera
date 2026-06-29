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

import { ComponentFixture, TestBed } from "@angular/core/testing";

import { ResultTableFrameComponent } from "./result-table-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";
import { NzTableModule } from "ng-zorro-antd/table";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { isAudioUrl, isImageUrl, isVideoUrl } from "../../../../common/util/media-type.util";

describe("ResultTableFrameComponent", () => {
  let component: ResultTableFrameComponent;
  let fixture: ComponentFixture<ResultTableFrameComponent>;

  const GUI_CONFIG_LIMIT = 15;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultTableFrameComponent, HttpClientTestingModule, NzModalModule, NzTableModule, NoopAnimationsModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        {
          provide: GuiConfigService,
          useValue: {
            env: {
              limitColumns: GUI_CONFIG_LIMIT,
            },
          },
        },
        ...commonTestProviders,
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("currentResult should not be modified if setupResultTable is called with empty (zero-length) execution result", () => {
    component.currentResult = [{ test: "property" }];
    (component as any).setupResultTable([], 0);

    expect(component.currentResult).toEqual([{ test: "property" }]);
  });

  it("should set columnLimit from gui-config", () => {
    expect(component.columnLimit).toEqual(GUI_CONFIG_LIMIT);
  });

  it("should detect media URLs for result cells", () => {
    expect(component.isVideoCell("https://example.com/clip.mp4")).toBe(true);
    expect(component.isAudioCell("https://example.com/sound.wav")).toBe(true);
    expect(component.isImageCell("data:image/png;base64,AAAA")).toBe(true);
  });

  it("should reject non-media values for result cells", () => {
    expect(component.isVideoCell("plain text")).toBe(false);
    expect(component.isAudioCell(123 as unknown)).toBe(false);
    expect(component.isImageCell(null as unknown)).toBe(false);
  });

  it("media-type util helpers should classify URLs consistently", () => {
    expect(isVideoUrl("clip.webm")).toBe(true);
    expect(isAudioUrl("track.flac")).toBe(true);
    expect(isImageUrl("image.webp")).toBe(true);
    expect(isVideoUrl("text")).toBe(false);
    expect(isAudioUrl("text")).toBe(false);
    expect(isImageUrl("text")).toBe(false);
  });

  describe("media cell rendering in table", () => {
    beforeEach(() => {
      component.operatorId = "test-op";
    });

    it("should render Play Video indicator for video URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/clip.mp4" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("Play Video");
    });

    it("should render Play Audio indicator for audio URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/clip.mp3" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("Play Audio");
    });

    it("should render View Image indicator for image URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/photo.jpg" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("View Image");
    });

    it("should render plain text for non-media cell values", () => {
      (component as any).setupResultTable([{ label: "just text" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("just text");
    });

    it("should render column headers matching the row keys", () => {
      (component as any).setupResultTable([{ score: "0.95", url: "https://example.com/a.png" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("score");
      expect(el.textContent).toContain("url");
    });
  });
});
