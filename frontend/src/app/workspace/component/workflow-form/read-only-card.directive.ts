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

import { AfterViewInit, Directive, ElementRef, Input, NgZone, OnChanges, OnDestroy } from "@angular/core";

/** What the browser lets a user reach with Tab, before this directive takes it out of the order. */
const FOCUSABLE =
  "a[href], area[href], button, input, select, textarea, iframe, object, embed, summary, " +
  "audio[controls], video[controls], [contenteditable]:not([contenteditable='false']), [tabindex]";

/** Where an element's own tabindex is kept while it is locked, so unlocking gives it back. */
const ORIGINAL_TABINDEX = new WeakMap<Element, string | null>();

/**
 * Keeps the keyboard out of a read-only input card, without hiding what it shows.
 *
 * A read-only card is locked with `pointer-events: none`, which is hit testing only: a keyboard user
 * could Tab into the controls a custom widget draws of its own -- a file picker's Browse button, the
 * HuggingFace file input -- and activate them with Enter or Space, and those controls do not consult
 * the form's disabled state the way the bound formly controls do (#8943). This takes every focusable
 * descendant out of the tab order and marks it `aria-disabled`, so there is nothing to reach and
 * assistive tech says why.
 *
 * `inert` would be shorter, and the step panel uses it for its read-only mount, but it also removes
 * the subtree from the accessibility tree. A reader's card exists to be read -- the values are what
 * a reader runs the workflow with -- so hiding it from a screen reader would trade this bug for a
 * worse one. Disabled controls stay in the accessibility tree and keep being announced.
 *
 * Widgets render their controls after the card is built and may redraw them at any time, so the
 * locking is re-applied whenever the card's content changes -- a control added, or one whose own
 * tabindex is written back.
 */
@Directive({
  selector: "[texeraReadOnlyCard]",
})
export class ReadOnlyCardDirective implements OnChanges, AfterViewInit, OnDestroy {
  /** Whether this card is read-only; the directive does nothing while it is false. */
  @Input("texeraReadOnlyCard") locked = false;

  private observer?: MutationObserver;
  /** The elements this directive took out of the tab order, so it only gives back what it took. */
  private readonly lockedElements = new Set<Element>();

  constructor(
    private host: ElementRef<HTMLElement>,
    private zone: NgZone
  ) {}

  ngOnChanges(): void {
    this.apply();
  }

  ngAfterViewInit(): void {
    this.apply();
  }

  ngOnDestroy(): void {
    this.stopWatching();
  }

  private apply(): void {
    if (this.locked) {
      this.lockDescendants();
      this.startWatching();
    } else {
      this.stopWatching();
      this.unlockDescendants();
    }
  }

  private lockDescendants(): void {
    for (const element of Array.from(this.host.nativeElement.querySelectorAll(FOCUSABLE))) {
      if (!this.lockedElements.has(element)) {
        ORIGINAL_TABINDEX.set(element, element.getAttribute("tabindex"));
        this.lockedElements.add(element);
      }
      // Set only what is not already set: this runs again for every change inside the card, and a
      // write of its own would be one more change. A control already locked is skipped, so the
      // attribute observer below settles after one pass; one that a widget put back in the tab
      // order is locked again.
      if (element.getAttribute("tabindex") !== "-1") {
        element.setAttribute("tabindex", "-1");
      }
      if (element.getAttribute("aria-disabled") !== "true") {
        element.setAttribute("aria-disabled", "true");
      }
    }
  }

  private unlockDescendants(): void {
    for (const element of this.lockedElements) {
      const original = ORIGINAL_TABINDEX.get(element) ?? null;
      if (original === null) {
        element.removeAttribute("tabindex");
      } else {
        element.setAttribute("tabindex", original);
      }
      element.removeAttribute("aria-disabled");
    }
    this.lockedElements.clear();
  }

  /**
   * Re-lock whatever a widget adds later. Outside Angular: a widget's own redraw is not this card's
   * change detection to run, and the observer only sets attributes on elements it has just seen.
   */
  private startWatching(): void {
    if (this.observer) {
      return;
    }
    this.zone.runOutsideAngular(() => {
      this.observer = new MutationObserver(() => this.lockDescendants());
      // Attributes as well as children: a widget that enables a control of its own writes its
      // tabindex back, which would otherwise put it in the tab order again behind this directive.
      this.observer.observe(this.host.nativeElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["tabindex"],
      });
    });
  }

  private stopWatching(): void {
    this.observer?.disconnect();
    this.observer = undefined;
  }
}
