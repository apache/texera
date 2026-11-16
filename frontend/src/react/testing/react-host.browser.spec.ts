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

import { AfterViewInit, Component, ElementRef, OnDestroy, viewChild } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ReactCounter } from "./react-counter.fixture";

@Component({
  template: `<span>Angular host</span>
    <div #container></div>`,
})
class ReactHostComponent implements AfterViewInit, OnDestroy {
  private readonly container = viewChild.required<ElementRef<HTMLElement>>("container");
  private root?: Root;

  ngAfterViewInit(): void {
    this.root = createRoot(this.container().nativeElement);
    this.root.render(createElement(ReactCounter));
  }

  ngOnDestroy(): void {
    this.root?.unmount();
  }
}

it("renders interactive TSX inside Angular and unmounts it when the host is destroyed", async () => {
  const fixture = TestBed.createComponent(ReactHostComponent);
  fixture.detectChanges();

  const host: HTMLElement = fixture.nativeElement;
  const container = host.querySelector("div")!;
  expect(host.querySelector("span")?.textContent).toBe("Angular host");
  await expect.poll(() => container.querySelector("button")?.textContent).toBe("Count: 0");
  container.querySelector("button")!.click();
  await expect.poll(() => container.querySelector("button")?.textContent).toBe("Count: 1");

  fixture.destroy();
  expect(container.childElementCount).toBe(0);
});
