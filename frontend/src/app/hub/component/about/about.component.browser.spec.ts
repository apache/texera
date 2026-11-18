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

  it.each([1280, 600])("preserves typography, spacing and grid layout in a %ipx host", async width => {
    const host: HTMLElement = fixture.nativeElement;
    host.style.display = "block";
    host.style.width = `${width}px`;
    host.style.height = "720px";
    fixture.detectChanges();
    await expect.poll(() => host.querySelector("h1")?.textContent?.trim()).toBe("About Texera");
    await fixture.whenStable();
    fixture.detectChanges();

    const container = host.querySelector<HTMLElement>(".about-page-container")!;
    const content = host.querySelector<HTMLElement>(".content")!;
    const heading = host.querySelector<HTMLElement>("h1")!;
    const paragraph = host.querySelector<HTMLElement>(".content > p")!;
    const row = host.querySelector<HTMLElement>(".features-row")!;
    const column = row.firstElementChild!;
    const list = host.querySelector<HTMLElement>("ul.text-content")!;
    const image = host.querySelector<HTMLImageElement>("img.workflow-gui-screenshot")!;
    const containerStyle = getComputedStyle(container);
    const contentStyle = getComputedStyle(content);
    const headingStyle = getComputedStyle(heading);
    const paragraphStyle = getComputedStyle(paragraph);
    const rowStyle = getComputedStyle(row);
    const columnStyle = getComputedStyle(column);
    const listStyle = getComputedStyle(list);

    expect(containerStyle.paddingLeft).toBe("133px");
    expect(containerStyle.paddingRight).toBe("133px");
    expect(containerStyle.overflowY).toBe("auto");
    expect(contentStyle.marginTop).toBe("40px");
    expect(contentStyle.maxWidth).toBe("1600px");
    expect(contentStyle.textAlign).toBe("left");
    expect(content.getBoundingClientRect().width).toBeCloseTo(width - 266, 1);
    expect(content.getBoundingClientRect().left - host.getBoundingClientRect().left).toBeCloseTo(133, 1);

    expect(headingStyle.fontSize).toBe("40px");
    expect(headingStyle.fontWeight).toBe("700");
    expect(headingStyle.marginTop).toBe("0px");
    expect(headingStyle.marginBottom).toBe("0px");
    expect(headingStyle.fontFamily).toBe(getComputedStyle(host).fontFamily);
    for (const subheading of Array.from(host.querySelectorAll("h2"))) {
      const style = getComputedStyle(subheading);
      expect(style.fontSize).toBe("28px");
      expect(style.fontWeight).toBe("700");
      expect(style.marginTop).toBe("10px");
    }
    expect(paragraphStyle.fontSize).toBe("16px");
    expect(paragraphStyle.fontWeight).toBe("400");
    expect(paragraphStyle.lineHeight).toBe("28.8px");
    expect(paragraphStyle.marginBottom).toBe("10px");
    expect(listStyle.fontSize).toBe("16px");
    expect(listStyle.lineHeight).toBe("28.8px");

    expect(rowStyle.display).toBe("flex");
    expect(rowStyle.flexWrap).toBe("wrap");
    expect(rowStyle.alignItems).toBe("flex-start");
    // The current string-valued nzGutter produces zero effective spacing in Chromium.
    expect(rowStyle.marginLeft).toBe("0px");
    expect(rowStyle.marginRight).toBe("0px");
    expect(columnStyle.flex).toBe("1 1 auto");
    expect(columnStyle.paddingLeft).toBe("0px");
    expect(columnStyle.paddingRight).toBe("0px");
    const columnContentWidth =
      column.getBoundingClientRect().width - parseFloat(columnStyle.paddingLeft) - parseFloat(columnStyle.paddingRight);
    expect(list.getBoundingClientRect().width).toBeCloseTo(columnContentWidth * 0.7, 1);
    expect(image.getBoundingClientRect().width).toBeCloseTo(content.getBoundingClientRect().width, 1);

    const learnMore = host.querySelector<HTMLElement>(".learn-more-section")!;
    expect(getComputedStyle(learnMore).marginTop).toBe("10px");
    const learnMoreParagraph = learnMore.querySelector("p")!;
    expect(getComputedStyle(learnMoreParagraph).fontSize).toBe("16px");
    expect(getComputedStyle(learnMoreParagraph).lineHeight).toBe("28.8px");
    for (const link of Array.from(learnMore.querySelectorAll("a"))) {
      expect(getComputedStyle(link).color).toBe("rgb(0, 122, 204)");
      expect(getComputedStyle(link).textDecorationLine).toBe("none");
    }
  });
});
