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
import { AboutComponent } from "./about.component";

describe("AboutComponent (browser)", () => {
  let fixture: ComponentFixture<AboutComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({ imports: [AboutComponent] });
    fixture = TestBed.createComponent(AboutComponent);
  });

  it.each([1280, 180])("fits the content and image in a %ipx column without horizontal scrolling", async width => {
    const host: HTMLElement = fixture.nativeElement;
    host.style.width = `${width}px`;
    fixture.detectChanges();
    await expect.poll(() => host.querySelector("h1")?.textContent?.trim()).toBe("About Texera");

    const bounds = host.getBoundingClientRect();
    const image = host.querySelector("img")!.getBoundingClientRect();

    expect(host.scrollWidth).toBeLessThanOrEqual(host.clientWidth + 1);
    expect(image.width).toBeGreaterThan(0);
    expect(image.left).toBeGreaterThanOrEqual(bounds.left);
    expect(image.right).toBeLessThanOrEqual(bounds.right + 1);
    expect(host.querySelector("h1")!.getBoundingClientRect().width).toBeGreaterThan(0);
  });
});
