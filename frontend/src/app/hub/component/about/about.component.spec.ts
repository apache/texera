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

import { NgZone, provideZoneChangeDetection } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { flushSync } from "react-dom";

import { AboutComponent } from "./about.component";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("AboutComponent", () => {
  let component: AboutComponent;
  let fixture: ComponentFixture<AboutComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [AboutComponent, RouterTestingModule.withRoutes([])],
      providers: [provideZoneChangeDetection(), ...commonTestProviders],
    });
    fixture = TestBed.createComponent(AboutComponent);
    component = fixture.componentInstance;
    flushSync(() => fixture.detectChanges());
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("renders the workflow image and links to Texera and its source in the current tab", () => {
    const host: HTMLElement = fixture.nativeElement;
    const image = host.querySelector("img");
    expect(image?.getAttribute("src")).toBe("/assets/workflow_gui.png");
    expect(image?.alt.trim()).toBeTruthy();
    const links = Array.from(host.querySelectorAll<HTMLAnchorElement>("a"));
    expect(links.map(link => ({ href: link.href, target: link.target }))).toEqual([
      { href: "https://texera.apache.org/", target: "" },
      { href: "https://github.com/apache/texera", target: "" },
    ]);
  });

  it("keeps React DOM events outside Angular's zone", () => {
    const zone = TestBed.inject(NgZone);
    expect(zone.run(() => NgZone.isInAngularZone())).toBe(true);
    const onUnstable = vi.fn();
    const subscription = zone.onUnstable.subscribe(onUnstable);
    try {
      const heading: HTMLElement = fixture.nativeElement.querySelector("h1");
      zone.runOutsideAngular(() => heading.click());
      expect(onUnstable).not.toHaveBeenCalled();
    } finally {
      subscription.unsubscribe();
    }
  });

  it("clears the React subtree on destroy and renders it again on remount", () => {
    const previousHost: HTMLElement = fixture.nativeElement;
    fixture.destroy();
    expect(previousHost.childElementCount).toBe(0);

    fixture = TestBed.createComponent(AboutComponent);
    flushSync(() => fixture.detectChanges());
    expect(fixture.nativeElement.querySelectorAll("h1").length).toBe(1);
  });
});
