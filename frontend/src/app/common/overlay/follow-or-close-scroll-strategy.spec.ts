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

import { NgZone } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import {
  CDK_CONNECTED_OVERLAY_DEFAULT_CONFIG,
  CdkConnectedOverlayConfig,
  ConnectedOverlayPositionChange,
  OverlayRef,
} from "@angular/cdk/overlay";
import { CdkScrollable, ScrollDispatcher } from "@angular/cdk/scrolling";
import { Subject } from "rxjs";
import {
  FOLLOW_OR_CLOSE_SCROLL_STRATEGY_PROVIDER,
  FollowOrCloseScrollStrategy,
} from "./follow-or-close-scroll-strategy";

describe("FollowOrCloseScrollStrategy", () => {
  const scrolled = new Subject<void>();
  const positionChanges = new Subject<ConnectedOverlayPositionChange>();
  const containers = [{} as CdkScrollable];
  const scrollDispatcher = {
    scrolled: () => scrolled,
    getAncestorScrollContainers: vi.fn(() => containers),
  } as unknown as ScrollDispatcher;
  const ngZone = { run: (fn: () => void) => fn() } as NgZone;
  // The overlay template's anchor sits in the component that owns the control.
  const owner = document.createElement("nz-select");
  const anchor = document.createComment("container");
  owner.appendChild(anchor);
  let positionStrategy: {
    positionChanges: Subject<ConnectedOverlayPositionChange>;
    withScrollableContainers: ReturnType<typeof vi.fn>;
  };
  let overlayRef: {
    updatePosition: ReturnType<typeof vi.fn>;
    detach: ReturnType<typeof vi.fn>;
    hasAttached: ReturnType<typeof vi.fn>;
    getConfig: () => { positionStrategy: unknown };
  };
  let strategy: FollowOrCloseScrollStrategy;

  const change = (
    isOriginOutsideView: boolean,
    isOriginClipped = isOriginOutsideView
  ): ConnectedOverlayPositionChange =>
    ({
      scrollableViewProperties: {
        isOriginClipped,
        isOriginOutsideView,
        isOverlayClipped: false,
        isOverlayOutsideView: false,
      },
    }) as ConnectedOverlayPositionChange;

  beforeEach(() => {
    positionStrategy = { positionChanges, withScrollableContainers: vi.fn() };
    overlayRef = {
      updatePosition: vi.fn(),
      detach: vi.fn(),
      hasAttached: vi.fn(() => true),
      getConfig: () => ({ positionStrategy }),
    };
    strategy = new FollowOrCloseScrollStrategy(scrollDispatcher, ngZone, anchor);
    strategy.attach(overlayRef as unknown as OverlayRef);
  });

  it("repositions the overlay on every scroll while enabled, and stops once disabled", () => {
    strategy.enable();
    scrolled.next();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(2);

    strategy.disable();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(2);
  });

  it("hands the position strategy the scroll containers around the overlay's owner, which the directive does not", () => {
    strategy.enable();
    expect(scrollDispatcher.getAncestorScrollContainers).toHaveBeenCalledWith(owner);
    expect(positionStrategy.withScrollableContainers).toHaveBeenCalledWith(containers);
  });

  it("detaches the overlay once its origin has scrolled fully out of view, after the position has been applied", async () => {
    strategy.enable();
    positionChanges.next(change(true));
    // Not yet: the change arrives mid-apply, so the detach is deferred past it.
    expect(overlayRef.detach).not.toHaveBeenCalled();
    await Promise.resolve();
    expect(overlayRef.detach).toHaveBeenCalledTimes(1);

    // Disabled by then: a later scroll neither repositions nor detaches again.
    scrolled.next();
    positionChanges.next(change(true));
    await Promise.resolve();
    expect(overlayRef.updatePosition).not.toHaveBeenCalled();
    expect(overlayRef.detach).toHaveBeenCalledTimes(1);
  });

  it("keeps an overlay whose origin is only clipped by the container's edge", async () => {
    strategy.enable();
    positionChanges.next(change(false, true));
    await Promise.resolve();
    expect(overlayRef.detach).not.toHaveBeenCalled();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(1);
  });

  it("leaves an overlay the user closed in the meantime alone", async () => {
    overlayRef.hasAttached.mockReturnValue(false);
    strategy.enable();
    positionChanges.next(change(true));
    await Promise.resolve();
    expect(overlayRef.detach).not.toHaveBeenCalled();
  });

  it("only follows when the position strategy is not a connected one, or the overlay has no anchor", async () => {
    overlayRef.getConfig = () => ({ positionStrategy: {} });
    strategy.enable();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(1);
    strategy.detach();

    const anchorless = new FollowOrCloseScrollStrategy(scrollDispatcher, ngZone, null);
    overlayRef.getConfig = () => ({ positionStrategy });
    anchorless.attach(overlayRef as unknown as OverlayRef);
    anchorless.enable();
    positionChanges.next(change(true));
    await Promise.resolve();
    expect(overlayRef.detach).not.toHaveBeenCalled();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(2);
    anchorless.detach();
  });

  it("enables once per attachment, and forgets the overlay on detach", () => {
    strategy.enable();
    strategy.enable();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(1);

    strategy.detach();
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(1);
    strategy.enable(); // nothing attached: a no-op
    scrolled.next();
    expect(overlayRef.updatePosition).toHaveBeenCalledTimes(1);
  });

  it("is what the app hands every connected overlay, one strategy each, anchored where it is read", () => {
    TestBed.configureTestingModule({ providers: [FOLLOW_OR_CLOSE_SCROLL_STRATEGY_PROVIDER] });
    const config = TestBed.inject(CDK_CONNECTED_OVERLAY_DEFAULT_CONFIG) as CdkConnectedOverlayConfig;
    // The directive reads the config's scrollStrategy once, in its constructor: every read is a
    // strategy of its own, since one strategy serves one overlay. Read outside any directive (here,
    // and for a config input binding) there is no element to anchor to, and the strategy only follows.
    const first = config.scrollStrategy;
    expect(first).toBeInstanceOf(FollowOrCloseScrollStrategy);
    expect(config.scrollStrategy).not.toBe(first);
    expect(TestBed.runInInjectionContext(() => config.scrollStrategy)).toBeInstanceOf(FollowOrCloseScrollStrategy);
  });

  it("keeps the side a list opened on while it follows the scroll", () => {
    TestBed.configureTestingModule({ providers: [FOLLOW_OR_CLOSE_SCROLL_STRATEGY_PROVIDER] });
    const config = TestBed.inject(CDK_CONNECTED_OVERLAY_DEFAULT_CONFIG) as CdkConnectedOverlayConfig;
    // The directive hands this to the position strategy as withLockedPosition: the placement chosen
    // on open (below, or above when the viewport leaves no room) is reused on every reposition.
    expect(config.lockPosition).toBe(true);
  });
});
