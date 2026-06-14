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
import { DomSanitizer } from "@angular/platform-browser";

import { FlarumComponent } from "./flarum.component";

describe("FlarumComponent", () => {
  let fixture: ComponentFixture<FlarumComponent>;
  let component: FlarumComponent;
  let bypassSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    TestBed.configureTestingModule({ imports: [FlarumComponent] });

    // Spy on the real sanitizer before the component is constructed, so the
    // constructor's call to bypassSecurityTrustResourceUrl is recorded.
    const sanitizer = TestBed.inject(DomSanitizer);
    bypassSpy = vi.spyOn(sanitizer, "bypassSecurityTrustResourceUrl");

    fixture = TestBed.createComponent(FlarumComponent);
    component = fixture.componentInstance;
  });

  it("asks DomSanitizer to trust the forum resource url at construction time", () => {
    expect(bypassSpy).toHaveBeenCalledExactlyOnceWith("forum");
  });

  it("exposes the sanitizer's SafeResourceUrl as flarumUrl", () => {
    // The spy returns the real SafeResourceUrl produced by Angular's sanitizer.
    expect(component.flarumUrl).toBe(bypassSpy.mock.results[0].value);
  });

  it("renders an iframe whose [src] binding is driven by flarumUrl", () => {
    fixture.detectChanges();
    const iframe = fixture.nativeElement.querySelector("iframe") as HTMLIFrameElement | null;

    expect(iframe).not.toBeNull();
    // We don't assert on the concrete URL string — DomSanitizer's serialised
    // output is an implementation detail. We just check that the binding fired.
    expect(iframe!.hasAttribute("src")).toBe(true);
  });
});
