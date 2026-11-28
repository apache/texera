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

import { ElementRef, inject, NgZone, Provider } from "@angular/core";
import {
  CDK_CONNECTED_OVERLAY_DEFAULT_CONFIG,
  CdkConnectedOverlayConfig,
  ConnectedOverlayPositionChange,
  OverlayRef,
  PositionStrategy,
  ScrollStrategy,
} from "@angular/cdk/overlay";
import { CdkScrollable, ScrollDispatcher } from "@angular/cdk/scrolling";
import { Observable, Subscription } from "rxjs";

type ConnectedPositionStrategy = PositionStrategy &
  Readonly<{
    positionChanges: Observable<ConnectedOverlayPositionChange>;
    withScrollableContainers(scrollables: CdkScrollable[]): unknown;
  }>;

function isConnected(strategy: PositionStrategy | undefined): strategy is ConnectedPositionStrategy {
  return !!strategy && "positionChanges" in strategy && "withScrollableContainers" in strategy;
}

/**
 * The scroll strategy of every `cdkConnectedOverlay` in the app (ng-zorro's select, cascader and
 * pickers open their lists through it; nz-dropdown and nzAutocomplete do not -- they open their
 * overlays with the CDK's reposition strategy directly, so they follow a registered scroller but
 * never close this way). It is the CDK's `reposition()` -- the open list follows its control while
 * a `cdkScrollable` container scrolls -- plus one rule the CDK leaves to the app: once the control
 * has scrolled fully out of its container's view, the overlay is detached, and the component that
 * opened it closes (ng-zorro's components react to `detach`). Without that rule the list keeps
 * floating beyond the container's edge, attached to a control that is no longer in sight.
 *
 * The CDK computes "origin out of view" only against the scroll containers the position strategy is
 * given, and the directive gives it none in this version, so the strategy supplies them: the
 * `cdkScrollable` ancestors of the element the overlay's template lives in -- the same ancestors
 * the control has, since the template sits inside the component that owns the control. The CDK's
 * own `autoClose` option watches the viewport only, never the scroll containers.
 */
export class FollowOrCloseScrollStrategy implements ScrollStrategy {
  private overlayRef: OverlayRef | null = null;
  private subscriptions: Subscription | null = null;

  constructor(
    private readonly scrollDispatcher: ScrollDispatcher,
    private readonly ngZone: NgZone,
    /** The overlay template's anchor (a comment node inside the component that opens the overlay). */
    private readonly anchor: Node | null
  ) {}

  public attach(overlayRef: OverlayRef): void {
    this.overlayRef = overlayRef;
  }

  public enable(): void {
    const overlayRef = this.overlayRef;
    if (this.subscriptions || !overlayRef) {
      return;
    }
    this.subscriptions = this.scrollDispatcher.scrolled(0).subscribe(() => overlayRef.updatePosition());
    const positionStrategy = overlayRef.getConfig().positionStrategy;
    const near = this.anchor?.parentElement;
    if (!isConnected(positionStrategy) || !near) {
      return;
    }
    positionStrategy.withScrollableContainers(this.scrollDispatcher.getAncestorScrollContainers(near));
    this.subscriptions.add(
      positionStrategy.positionChanges.subscribe(change => {
        // Fully out of view, not merely clipped: a control half under the container's edge keeps its
        // list, or the list would close the moment the user scrolls the control to the edge.
        if (change.scrollableViewProperties.isOriginOutsideView) {
          this.disable();
          // The change arrives while the position strategy is still applying the new position, so
          // the detach waits for that to finish; in the zone, as the CDK's own strategies detach,
          // since the component's close has to be seen by change detection.
          this.ngZone.run(() =>
            Promise.resolve().then(() => {
              if (overlayRef.hasAttached()) {
                overlayRef.detach();
              }
            })
          );
        }
      })
    );
  }

  public disable(): void {
    this.subscriptions?.unsubscribe();
    this.subscriptions = null;
  }

  public detach(): void {
    this.disable();
    this.overlayRef = null;
  }
}

/**
 * Hands every `cdkConnectedOverlay` a strategy of its own. A scroll strategy belongs to one overlay
 * (the CDK's own throw on a second attach), and the CDK's per-overlay factory token is not public in
 * this version; its default-config token is, and the directive reads `scrollStrategy` off that config
 * once, in its constructor -- so a getter hands each directive a fresh strategy, and, read in the
 * directive's injection context, can take the directive's own element as the overlay's anchor.
 */
export const FOLLOW_OR_CLOSE_SCROLL_STRATEGY_PROVIDER: Provider = {
  provide: CDK_CONNECTED_OVERLAY_DEFAULT_CONFIG,
  useFactory: (scrollDispatcher: ScrollDispatcher, ngZone: NgZone): CdkConnectedOverlayConfig => ({
    get scrollStrategy(): ScrollStrategy {
      return new FollowOrCloseScrollStrategy(scrollDispatcher, ngZone, anchorOfCurrentDirective());
    },
    // The side a list opens on (below its control, or above when the viewport leaves no room below)
    // is chosen when it opens and kept while it follows the scroll. Without this the CDK re-chooses
    // on every reposition, and a list flips from below its control to above it, and back, as the
    // control nears the viewport's bottom -- a jump the user never asked for. A reopened list
    // chooses afresh.
    lockPosition: true,
  }),
  deps: [ScrollDispatcher, NgZone],
};

/**
 * The element of the directive being constructed, or null when the config is read anywhere else
 * (a `cdkConnectedOverlayConfig` input binding, a test): then the overlay only follows the scroll.
 */
function anchorOfCurrentDirective(): Node | null {
  try {
    return inject(ElementRef, { optional: true })?.nativeElement ?? null;
  } catch {
    return null;
  }
}
