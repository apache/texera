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
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

describe("AnnotationSuggestionComponent", () => {
  let component: AnnotationSuggestionComponent;
  let fixture: ComponentFixture<AnnotationSuggestionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AnnotationSuggestionComponent],
    }).compileComponents();
    fixture = TestBed.createComponent(AnnotationSuggestionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("creates", () => {
    expect(component).toBeTruthy();
  });

  it("defaults its inputs to empty / zero", () => {
    expect(component.code).toBe("");
    expect(component.suggestion).toBe("");
    expect(component.top).toBe(0);
    expect(component.left).toBe(0);
  });

  it("emits accept when onAccept is called", () => {
    const spy = vi.fn();
    component.accept.subscribe(spy);
    component.onAccept();
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("emits decline when onDecline is called", () => {
    const spy = vi.fn();
    component.decline.subscribe(spy);
    component.onDecline();
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("emits accept and decline independently — onAccept does not fire decline, and vice versa", () => {
    const acceptSpy = vi.fn();
    const declineSpy = vi.fn();
    component.accept.subscribe(acceptSpy);
    component.decline.subscribe(declineSpy);

    component.onAccept();
    expect(acceptSpy).toHaveBeenCalledTimes(1);
    expect(declineSpy).not.toHaveBeenCalled();

    component.onDecline();
    expect(acceptSpy).toHaveBeenCalledTimes(1);
    expect(declineSpy).toHaveBeenCalledTimes(1);
  });

  it("respects the latest values bound to its @Input fields", () => {
    component.code = "x = 1";
    component.suggestion = ": int";
    component.top = 50;
    component.left = 75;
    fixture.detectChanges();
    expect(component.code).toBe("x = 1");
    expect(component.suggestion).toBe(": int");
    expect(component.top).toBe(50);
    expect(component.left).toBe(75);
  });
});
