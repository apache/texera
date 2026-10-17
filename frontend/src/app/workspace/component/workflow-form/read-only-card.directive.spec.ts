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

import { Component } from "@angular/core";
import { NgIf } from "@angular/common";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ReadOnlyCardDirective } from "./read-only-card.directive";

/** A card as the Form View builds one: a bound control, and the controls a widget draws of its own. */
@Component({
  template: `
    <section [texeraReadOnlyCard]="locked">
      <input class="bound" />
      <button class="browse">Browse</button>
      <a
        class="help"
        href="https://example.invalid"
        >Help</a
      >
      <span class="label">Model</span>
      <div
        class="custom"
        tabindex="3">
        Pick
      </div>
      <button
        class="added"
        *ngIf="widgetRendered">
        Added later
      </button>
    </section>
  `,
  imports: [NgIf, ReadOnlyCardDirective],
})
class HostComponent {
  locked = true;
  widgetRendered = false;
}

/**
 * The read-only card is locked with `pointer-events: none`, which only stops the pointer. These
 * cover the keyboard half: nothing inside a reader's card is in the tab order, while the card stays
 * readable (it is not inert, so its content is still in the accessibility tree).
 */
describe("ReadOnlyCardDirective", () => {
  let fixture: ComponentFixture<HostComponent>;
  const host = () => fixture.nativeElement as HTMLElement;
  const attr = (selector: string, name: string) => host().querySelector(selector)!.getAttribute(name);
  const tabbable = () =>
    Array.from(host().querySelectorAll("input, button, a, [tabindex]")).filter(
      el => el.getAttribute("tabindex") !== "-1"
    ).length;

  beforeEach(async () => {
    await TestBed.configureTestingModule({ imports: [HostComponent] }).compileComponents();
    fixture = TestBed.createComponent(HostComponent);
  });

  it("takes every focusable control in a read-only card out of the tab order", () => {
    fixture.detectChanges();

    expect(tabbable()).toBe(0);
    expect(attr(".bound", "tabindex")).toBe("-1");
    expect(attr(".browse", "tabindex")).toBe("-1");
    expect(attr(".help", "tabindex")).toBe("-1");
  });

  it("says why, so assistive tech announces the controls as disabled rather than hiding them", () => {
    fixture.detectChanges();

    expect(attr(".browse", "aria-disabled")).toBe("true");
    // The card is not inert: its content stays in the accessibility tree for a reader to read.
    expect(host().querySelector("section")!.hasAttribute("inert")).toBe(false);
    expect(host().querySelector(".label")!.textContent).toContain("Model");
  });

  it("leaves a card alone while it is editable", () => {
    fixture.componentInstance.locked = false;
    fixture.detectChanges();

    expect(tabbable()).toBe(4);
    expect(attr(".bound", "tabindex")).toBeNull();
    expect(attr(".custom", "tabindex")).toBe("3");
  });

  it("gives a control its own tabindex back when the card becomes editable", () => {
    fixture.detectChanges();
    expect(attr(".custom", "tabindex")).toBe("-1");

    fixture.componentInstance.locked = false;
    fixture.detectChanges();

    expect(attr(".custom", "tabindex")).toBe("3");
    expect(attr(".bound", "tabindex")).toBeNull();
    expect(attr(".browse", "aria-disabled")).toBeNull();
  });

  // A widget renders its own controls after the card is built, and redraws them as it goes.
  it("locks a control a widget adds after the card was built", async () => {
    fixture.detectChanges();

    fixture.componentInstance.widgetRendered = true;
    fixture.detectChanges();
    // The observer runs as a microtask of its own, after the DOM change.
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(attr(".added", "tabindex")).toBe("-1");
    expect(attr(".added", "aria-disabled")).toBe("true");
    expect(tabbable()).toBe(0);
  });

  // ng-zorro and friends write their own tabindex when they enable a control; on a read-only card
  // that would put it back in the tab order behind this directive.
  it("locks a control again when its widget writes its tabindex back", async () => {
    fixture.detectChanges();
    const browse = host().querySelector(".browse") as HTMLElement;
    expect(browse.getAttribute("tabindex")).toBe("-1");

    browse.setAttribute("tabindex", "0");
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(browse.getAttribute("tabindex")).toBe("-1");
    expect(tabbable()).toBe(0);
  });

  it("stops watching when the card goes away", () => {
    fixture.detectChanges();
    const directive =
      (fixture.componentInstance as any) &&
      (fixture.debugElement.children[0].injector.get(ReadOnlyCardDirective) as any);
    const observer = directive.observer as MutationObserver;
    const disconnect = vi.spyOn(observer, "disconnect");

    fixture.destroy();

    expect(disconnect).toHaveBeenCalled();
  });
});
