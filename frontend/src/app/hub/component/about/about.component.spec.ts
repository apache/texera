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
    await expect.poll(() => fixture.nativeElement.querySelector("h1")?.textContent?.trim()).toBe("About Texera");
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // The login form moved to the dedicated /login page; this page must not render one.
  it("no longer embeds a login form", () => {
    expect(fixture.nativeElement.querySelector("texera-local-login")).toBeNull();
  });

  it("preserves the introduction, headings and seven platform features in order", () => {
    const host: HTMLElement = fixture.nativeElement;
    expect(host.querySelectorAll("h1")).toHaveLength(1);
    expect(text(host.querySelector(".content > p"))).toBe(
      "Apache Texera (Incubating) is an open-source platform for human-AI collaborative data science using visual " +
        "workflows. It enables analysts to construct, execute, and refine data analysis tasks through an intuitive GUI, " +
        "assisted by AI agents that understand natural-language instructions. Texera is well-suited for a wide range of " +
        "applications, including “AI for Science,” by making advanced AI and data science capabilities accessible to a " +
        "broader community. It can run on a laptop for local use or be deployed in the cloud to support scalable processing " +
        "of large datasets."
    );
    expect(Array.from(host.querySelectorAll("h2"), text)).toEqual([
      "The platform has the following key features:",
      "Learn More",
    ]);
    expect(Array.from(host.querySelectorAll("ul > li"), text)).toEqual([
      "Natural-language data science through AI agents",
      "Intuitive GUI-based workflows for data science",
      "Real-time collaboration for workflow editing and execution",
      "Parallel backend engine for scalable big-data processing",
      "Language-agnostic workflow runtime, native support for Python and Java",
      "Separation of compute and storage for flexible cloud deployment",
      "Runtime debugging and interactive workflow execution",
    ]);
  });

  it("preserves the workflow screenshot and the Learn More links in the current browsing context", () => {
    const host: HTMLElement = fixture.nativeElement;
    const images = host.querySelectorAll("img");
    expect(images).toHaveLength(1);
    expect(images[0].getAttribute("src")).toBe("../../../../../assets/workflow_gui.png");
    expect(images[0].alt).toBe("workflow gui screenshot");
    expect(text(host.querySelector(".learn-more-section > p"))).toBe(
      "To learn more, please visit https://texera.apache.org/. The source code of the platform is available on GitHub."
    );
    const links = Array.from(host.querySelectorAll<HTMLAnchorElement>(".learn-more-section a"));
    expect(links.map(link => ({ href: link.getAttribute("href"), text: text(link), target: link.target }))).toEqual([
      { href: "https://texera.apache.org/", text: "https://texera.apache.org/", target: "" },
      { href: "https://github.com/apache/texera", text: "source code", target: "" },
    ]);
  });

  it("destroys the page and renders one complete page when the route host is recreated", async () => {
    const previousHost: HTMLElement = fixture.nativeElement;
    fixture.destroy();
    expect(fixture.componentRef.hostView.destroyed).toBe(true);

    fixture = TestBed.createComponent(AboutComponent);
    fixture.detectChanges();
    await expect.poll(() => fixture.nativeElement.querySelector("h1")?.textContent?.trim()).toBe("About Texera");
    const host: HTMLElement = fixture.nativeElement;
    expect(host).not.toBe(previousHost);
    expect(host.querySelectorAll(".about-page-container")).toHaveLength(1);
    expect(host.querySelectorAll("ul > li")).toHaveLength(7);
    expect(host.querySelectorAll(".learn-more-section a")).toHaveLength(2);
  });

  it("releases the rendered subtree when its Angular host is destroyed", () => {
    const container: HTMLElement = fixture.nativeElement;
    expect(container.querySelector("h1")?.textContent?.trim()).toBe("About Texera");
    fixture.destroy();
    expect(container.childElementCount).toBe(0);
  });
});

function text(element: Element | null): string {
  return element?.textContent?.replace(/\s+/g, " ").trim() ?? "";
}
