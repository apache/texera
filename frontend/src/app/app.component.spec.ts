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

import { HttpClientTestingModule } from "@angular/common/http/testing";
import { CommonModule } from "@angular/common";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { AppComponent } from "./app.component";
import { GuiConfigService } from "./common/service/gui-config.service";
import { DeploymentVersionService } from "./common/service/deployment-version/deployment-version.service";
import { NotificationService } from "./common/service/notification/notification.service";
import { Version } from "../environments/version";

// GuiConfigService stub whose env getter either returns a value or throws,
// mirroring "config loaded" vs "config failed to load by APP_INITIALIZER".
class StubGuiConfigService {
  shouldThrow = false;
  deploymentVersionCheckEnabled = true;
  get env(): unknown {
    if (this.shouldThrow) {
      throw new Error("config not loaded");
    }
    return { deploymentVersionCheckEnabled: this.deploymentVersionCheckEnabled };
  }
}

describe("AppComponent", () => {
  let config: StubGuiConfigService;
  // The real DeploymentVersionService, with its polling entry point spied so
  // the test asserts on the wiring without kicking off real HTTP polling.
  let startPollingSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    Version.buildNumber = "dev";
    config = new StubGuiConfigService();

    TestBed.configureTestingModule({
      imports: [CommonModule, RouterTestingModule, HttpClientTestingModule],
      declarations: [AppComponent],
      providers: [
        { provide: GuiConfigService, useValue: config },
        DeploymentVersionService,
        // NotificationService is a transitive dependency of DeploymentVersionService.
        { provide: NotificationService, useValue: { blank: vi.fn() } },
      ],
    });
    const deploymentVersionService = TestBed.inject(DeploymentVersionService);
    startPollingSpy = vi
      .spyOn(deploymentVersionService, "startPollingForUpdates")
      .mockReturnValue({ unsubscribe: () => undefined } as never);
  });

  // Version is a shared module singleton; restore the dev default so a test
  // that flips buildNumber cannot leak into other suites in the same worker.
  afterEach(() => {
    Version.buildNumber = "dev";
  });

  function create(): ComponentFixture<AppComponent> {
    return TestBed.createComponent(AppComponent);
  }

  describe("config-loaded detection", () => {
    it("marks config as loaded when env is accessible", () => {
      config.shouldThrow = false;
      const component = create().componentInstance;
      expect(component.configLoaded).toBe(true);
    });

    it("marks config as not loaded when accessing env throws", () => {
      config.shouldThrow = true;
      const component = create().componentInstance;
      expect(component.configLoaded).toBe(false);
    });

    it("renders the configuration-error panel when config is not loaded", () => {
      config.shouldThrow = true;
      const fixture = create();
      fixture.detectChanges();
      const el: HTMLElement = fixture.nativeElement;
      expect(el.querySelector("#config-error")).not.toBeNull();
    });

    it("does not render the configuration-error panel when config is loaded", () => {
      config.shouldThrow = false;
      const fixture = create();
      fixture.detectChanges();
      const el: HTMLElement = fixture.nativeElement;
      expect(el.querySelector("#config-error")).toBeNull();
    });
  });

  describe("deployment-version polling guard", () => {
    it("does not start polling for the 'dev' placeholder build", () => {
      config.deploymentVersionCheckEnabled = true;
      Version.buildNumber = "dev";
      create();
      expect(startPollingSpy).not.toHaveBeenCalled();
    });

    it("does not start polling when the config flag is disabled", () => {
      config.deploymentVersionCheckEnabled = false;
      Version.buildNumber = "prod-build-123";
      create();
      expect(startPollingSpy).not.toHaveBeenCalled();
    });

    it("does not start polling when config failed to load", () => {
      config.shouldThrow = true;
      Version.buildNumber = "prod-build-123";
      create();
      expect(startPollingSpy).not.toHaveBeenCalled();
    });

    it("starts polling for a real build when the config flag is enabled", () => {
      config.deploymentVersionCheckEnabled = true;
      Version.buildNumber = "prod-build-123";
      create();
      expect(startPollingSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("retry", () => {
    it("reloads the page", () => {
      const reload = vi.fn();
      // location.reload is a non-writable, non-configurable own property (and is
      // absent from Location.prototype) under this jsdom build, so it cannot be
      // spied or reassigned directly. window.location itself is configurable,
      // so swap the whole object for the test, then restore it.
      const original = window.location;
      Object.defineProperty(window, "location", {
        configurable: true,
        value: { ...original, reload },
      });
      try {
        create().componentInstance.retry();
        expect(reload).toHaveBeenCalledTimes(1);
      } finally {
        Object.defineProperty(window, "location", { configurable: true, value: original });
      }
    });
  });
});
