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
import { RouterTestingModule } from "@angular/router/testing";

import { AboutComponent } from "./about.component";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("AboutComponent", () => {
  let component: AboutComponent;
  let fixture: ComponentFixture<AboutComponent>;

  beforeEach(async () => {
    TestBed.configureTestingModule({
      imports: [AboutComponent, RouterTestingModule.withRoutes([])],
      providers: [...commonTestProviders],
    });
    fixture = TestBed.createComponent(AboutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    await expect.poll(() => fixture.nativeElement.querySelector("h1")).not.toBeNull();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // The login form moved to the dedicated /login page; this page must not render one.
  it("no longer embeds a login form", () => {
    expect(fixture.nativeElement.querySelector("texera-local-login")).toBeNull();
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

  it("clears the React subtree on destroy and renders it again on remount", async () => {
    const previousHost: HTMLElement = fixture.nativeElement;
    fixture.destroy();
    expect(previousHost.childElementCount).toBe(0);

    fixture = TestBed.createComponent(AboutComponent);
    fixture.detectChanges();
    await expect.poll(() => fixture.nativeElement.querySelectorAll("h1").length).toBe(1);
  });
});
