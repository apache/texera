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

import { ComponentFixture, TestBed, fakeAsync, tick, discardPeriodicTasks } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_ICONS } from "ng-zorro-antd/icon";
import {
  DeleteOutline,
  FileTextOutline,
  LoadingOutline,
  PlusOutline,
  ReloadOutline,
} from "@ant-design/icons-angular/icons";
import { AdminCuImageComponent } from "./admin-cu-image.component";
import { CuImage } from "../../../service/admin/cu-image/cu-image.service";

describe("AdminCuImageComponent", () => {
  let component: AdminCuImageComponent;
  let fixture: ComponentFixture<AdminCuImageComponent>;
  let httpTestingController: HttpTestingController;

  const CU_IMAGE_URL = "api/cu-image";

  const image = (over: Partial<CuImage> = {}): CuImage => ({
    iid: 1,
    name: "Python ML",
    sourceRef: "tagandhi19/texera-cu-sklearn:1.0",
    sourceDigest: "sha256:bdeadc3c",
    status: "READY",
    imageTag: "tagandhi19/texera-cu-sklearn@sha256:bdeadc3c",
    attempt: 1,
    creationTime: 0,
    updateTime: 0,
    ...over,
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AdminCuImageComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        // Registered so nz-icon renders inline instead of fetching each SVG over HTTP.
        {
          provide: NZ_ICONS,
          useValue: [PlusOutline, FileTextOutline, ReloadOutline, DeleteOutline, LoadingOutline],
        },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    httpTestingController = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(AdminCuImageComponent);
    component = fixture.componentInstance;
  });

  /** Answers the load() that ngOnInit fires, so each test starts from a known list. */
  const initWith = (images: CuImage[]) => {
    fixture.detectChanges();
    httpTestingController.expectOne(CU_IMAGE_URL).flush(images);
    fixture.detectChanges();
  };

  it("should create", () => {
    initWith([]);
    expect(component).toBeTruthy();
  });

  it("shows the digest a unit will actually run, not just the tag", () => {
    // A tag can be moved by its owner, so the digest is the only answer to "what runs?".
    initWith([image()]);
    const text = fixture.nativeElement.textContent;
    expect(text).toContain("tagandhi19/texera-cu-sklearn@sha256:bdeadc3c");
  });

  it("says a deployment has the feature switched off rather than raising an error", () => {
    // Shown as an error, an admin cannot tell an unset switch from a broken deployment.
    fixture.detectChanges();
    httpTestingController.expectOne(CU_IMAGE_URL).flush(null, { status: 503, statusText: "Service Unavailable" });
    fixture.detectChanges();

    expect(component.featureDisabled).toBe(true);
    expect(fixture.nativeElement.textContent).toContain("not enabled on this deployment");
  });

  // The API orders by name, which scatters a just-registered image.
  it("lists the newest image first, whatever its name", () => {
    initWith([
      image({ iid: 1, name: "Alpine", creationTime: 100 }),
      image({ iid: 2, name: "Zebra", creationTime: 300 }),
      image({ iid: 3, name: "Middle", creationTime: 200 }),
    ]);
    expect(component.images.map(i => i.iid)).toEqual([2, 3, 1]);
  });

  it("refuses to register an image with either field blank", () => {
    initWith([]);
    component.newName = "  ";
    component.newSourceRef = "owner/name:1";
    component.add();
    httpTestingController.expectNone({ method: "POST" });
  });

  // ngOnInit directly, not via detectChanges: a timer scheduled from the fixture's NgZone
  // lands on the real queue, where tick() cannot drive it.
  it("polls while a check is running and asks for nothing once it settles", fakeAsync(() => {
    // The regression this guards: an earlier version polled forever and ignored the answer.
    component.ngOnInit();
    httpTestingController
      .expectOne(CU_IMAGE_URL)
      .flush([image({ status: "VALIDATING", imageTag: null, sourceDigest: null })]);

    tick(3000);
    httpTestingController.expectOne(CU_IMAGE_URL).flush([image({ status: "READY" })]);
    expect(component.images[0].status).toBe("READY");

    // Settled now, so no further request is made at all.
    tick(3000);
    httpTestingController.expectNone(CU_IMAGE_URL);

    discardPeriodicTasks();
  }));

  it("keeps polling after a request fails", fakeAsync(() => {
    // An error reaching the outer stream ends the subscription for good, so one hiccup
    // while a check is running would leave the row VALIDATING forever -- and Refresh is
    // disabled in that state, so only a reload would recover it.
    component.ngOnInit();
    httpTestingController.expectOne(CU_IMAGE_URL).flush([image({ status: "VALIDATING" })]);

    tick(3000);
    httpTestingController.expectOne(CU_IMAGE_URL).flush(null, { status: 502, statusText: "Bad Gateway" });

    tick(3000);
    httpTestingController.expectOne(CU_IMAGE_URL).flush([image({ status: "READY" })]);
    expect(component.images[0].status).toBe("READY");

    discardPeriodicTasks();
  }));

  it("sends one request when Enter is pressed twice", () => {
    // Enter calls add() directly, where the Add button's disabled state does not apply.
    initWith([]);
    component.newName = "Python ML";
    component.newSourceRef = "owner/name:1";
    component.add();
    component.add();
    httpTestingController.expectOne(req => req.method === "POST");
  });

  it("does not poll a deployment that has the feature switched off", fakeAsync(() => {
    component.ngOnInit();
    httpTestingController.expectOne(CU_IMAGE_URL).flush(null, { status: 503, statusText: "Service Unavailable" });
    expect(component.featureDisabled).toBe(true);

    // Otherwise the page asks for a 503 every three seconds.
    tick(3000);
    httpTestingController.expectNone(CU_IMAGE_URL);

    discardPeriodicTasks();
  }));

  it("offers Refresh only when a check is not already running", () => {
    initWith([image({ status: "VALIDATING" })]);
    const refresh = Array.from(fixture.nativeElement.querySelectorAll("button") as NodeListOf<HTMLButtonElement>).find(
      b => b.textContent?.includes("Refresh")
    );
    expect(refresh?.disabled).toBe(true);
  });

  it("colours a failed check apart from a ready one", () => {
    expect(component.statusColor("READY")).toBe("green");
    expect(component.statusColor("FAILED")).toBe("red");
    expect(component.statusColor("VALIDATING")).toBe("blue");
  });

  afterEach(() => {
    httpTestingController.verify();
  });
});
