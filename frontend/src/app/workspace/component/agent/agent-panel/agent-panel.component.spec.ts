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

import { Component, EventEmitter, Input, Output } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { By } from "@angular/platform-browser";
import { Observable, Subject, of, throwError } from "rxjs";
import { AgentPanelComponent } from "./agent-panel.component";
import { AgentRegistrationComponent } from "./agent-registration/agent-registration.component";
import { AgentChatComponent } from "./agent-chat/agent-chat.component";
import { AgentInfo, AgentService } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const CURRENT_WID = 42;
const USER_INFO = { uid: 1, name: "user", email: "user@example.com", role: "REGULAR" };

/**
 * Subject-backed double of AgentService (panel surface only). The panel loads the
 * agent list in ngOnInit and re-loads it whenever agentChange$ fires, so tests set
 * `agentList` and, for refreshes, push through `agentChangeSubject.next()`.
 */
class MockAgentService {
  public agentChangeSubject = new Subject<void>();
  public agentChange$ = this.agentChangeSubject.asObservable();
  public agentList: AgentInfo[] = [];

  public getAllAgents = vi.fn((): Observable<AgentInfo[]> => of(this.agentList));
  public activateAgent = vi.fn((): boolean => true);
  public deactivateAgent = vi.fn();
  public deleteAgent = vi.fn((): Observable<boolean> => of(true));
}

/**
 * Child components have their own specs (agent-chat, agent-registration); the panel
 * spec shallow-renders them via Recipe F stubs so the panel's template still renders
 * every tab (nzForceRender) without pulling in markdown / computing-unit machinery.
 */
@Component({
  selector: "texera-agent-registration",
  standalone: true,
  template: "",
})
class StubAgentRegistrationComponent {
  @Output() agentCreated = new EventEmitter<string>();
}

@Component({
  selector: "texera-agent-chat",
  standalone: true,
  template: "",
})
class StubAgentChatComponent {
  @Input() agentInfo?: AgentInfo;
  @Input() isActive = false;
}

function makeAgent(id: string, overrides: Partial<AgentInfo> = {}): AgentInfo {
  return {
    id,
    name: `Agent ${id}`,
    modelType: "gpt-test",
    isBaselineMode: false,
    createdAt: new Date("2026-01-01T00:00:00Z"),
    ...overrides,
  };
}

function makeDelegateAgent(id: string, workflowId?: number): AgentInfo {
  return makeAgent(id, { delegate: { userInfo: USER_INFO, workflowId } });
}

describe("AgentPanelComponent", () => {
  let fixture: ComponentFixture<AgentPanelComponent>;
  let component: AgentPanelComponent;
  let agentService: MockAgentService;
  let workflowAction: { getWorkflowMetadata: ReturnType<typeof vi.fn> };
  let notification: Record<"success" | "error" | "warning" | "info", ReturnType<typeof vi.fn>>;
  // Element injected before the fixture so loadPanelSettings' getElementById("agent-container")
  // resolves to a style object the test controls (jsdom does not round-trip `transform`
  // through cssText, so the style is faked via defineProperty).
  let fakeContainer: HTMLDivElement | undefined;

  beforeEach(async () => {
    localStorage.clear();
    agentService = new MockAgentService();
    workflowAction = { getWorkflowMetadata: vi.fn().mockReturnValue({ wid: CURRENT_WID }) };
    notification = { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() };

    TestBed.overrideComponent(AgentPanelComponent, {
      remove: { imports: [AgentRegistrationComponent, AgentChatComponent] },
      add: { imports: [StubAgentRegistrationComponent, StubAgentChatComponent] },
    });

    await TestBed.configureTestingModule({
      imports: [AgentPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        { provide: AgentService, useValue: agentService },
        { provide: WorkflowActionService, useValue: workflowAction },
        { provide: NotificationService, useValue: notification },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  afterEach(() => {
    // Some tests destroy the fixture themselves; guard against a double-destroy.
    try {
      fixture?.destroy();
    } catch {
      // already destroyed
    }
    fixture = undefined as unknown as ComponentFixture<AgentPanelComponent>;
    fakeContainer?.remove();
    fakeContainer = undefined;
    // ngOnDestroy persists panel settings; wipe them so specs stay independent.
    localStorage.clear();
    vi.restoreAllMocks();
  });

  function createComponent(agentIdToActivate?: string): AgentPanelComponent {
    fixture = TestBed.createComponent(AgentPanelComponent);
    component = fixture.componentInstance;
    if (agentIdToActivate !== undefined) {
      fixture.componentRef.setInput("agentIdToActivate", agentIdToActivate);
    }
    fixture.detectChanges();
    return component;
  }

  function installFakeContainer(style: { cssText: string; transform: string }): void {
    fakeContainer = document.createElement("div");
    fakeContainer.id = "agent-container";
    Object.defineProperty(fakeContainer, "style", { value: style, configurable: true });
    // Insert at the front of <body> so getElementById resolves to this element,
    // not the panel's own #agent-container rendered later by the fixture.
    document.body.insertBefore(fakeContainer, document.body.firstChild);
  }

  describe("initialization and agent list wiring", () => {
    it("loads the agents once on init and renders one tab per agent plus the registration tab", () => {
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();

      expect(agentService.getAllAgents).toHaveBeenCalledTimes(1);
      expect(component.agents).toEqual(agentService.agentList);

      const text = fixture.nativeElement.textContent as string;
      expect(text).toContain("+ Agent");
      expect(text).toContain("2 agent(s)");
      const names = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll(".agent-tab-name")).map(
        el => el.textContent
      );
      expect(names).toEqual(["Agent a", "Agent b"]);
    });

    it("shows only the registration tab and a zero count when there are no agents", () => {
      createComponent();
      expect(fixture.nativeElement.querySelectorAll(".agent-tab-name").length).toBe(0);
      expect(fixture.nativeElement.textContent).toContain("0 agent(s)");
    });

    it("reloads the agent list when the service signals an agent change", () => {
      const a = makeAgent("a");
      agentService.agentList = [a];
      createComponent();
      expect(component.agents).toEqual([a]);

      const b = makeAgent("b");
      agentService.agentList = [a, b];
      agentService.agentChangeSubject.next();

      expect(agentService.getAllAgents).toHaveBeenCalledTimes(2);
      expect(component.agents).toEqual([a, b]);
      fixture.detectChanges();
      expect(fixture.nativeElement.textContent).toContain("2 agent(s)");
    });

    it("feeds each agent-chat tab its agent and whether it is the active one", () => {
      const a = makeAgent("a");
      const b = makeAgent("b");
      agentService.agentList = [a, b];
      createComponent();

      const chats = () =>
        fixture.debugElement.queryAll(By.directive(StubAgentChatComponent)).map(d => d.componentInstance);
      expect(chats().map(c => c.agentInfo)).toEqual([a, b]);
      expect(chats().map(c => c.isActive)).toEqual([false, false]);

      component.onTabSelectChange(1);
      fixture.detectChanges();
      expect(chats().map(c => c.isActive)).toEqual([true, false]);
    });
  });

  describe("agentIdToActivate input", () => {
    it("opens the panel, activates the agent and selects its tab once agents load", () => {
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent("b");

      expect(component.width).toBe(400); // panel opened from docked state
      expect(component.activeAgentId).toBe("b");
      expect(agentService.activateAgent).toHaveBeenCalledWith("b");
      expect(component.selectedTabIndex).toBe(2); // tab 0 is registration
      expect(component.agentIdToActivate).toBeUndefined(); // consumed
    });

    it("ignores an id that matches no agent and keeps the input for a later retry", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent("ghost");

      expect(component.width).toBe(0);
      expect(component.selectedTabIndex).toBe(0);
      expect(agentService.activateAgent).not.toHaveBeenCalled();
      expect(component.agentIdToActivate).toBe("ghost");
    });

    it("defers activation until the agent list arrives through agentChange$", () => {
      createComponent("b"); // no agents yet: both ngOnChanges and ngOnInit bail out
      expect(agentService.activateAgent).not.toHaveBeenCalled();

      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      agentService.agentChangeSubject.next();

      expect(component.activeAgentId).toBe("b");
      expect(component.selectedTabIndex).toBe(2);
      expect(component.width).toBe(400);
    });

    it("deactivates the previously active agent and keeps an already-open panel width", () => {
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();
      component.onTabSelectChange(1); // activate "a"
      component.width = 640; // panel already open at a custom size

      fixture.componentRef.setInput("agentIdToActivate", "b");
      fixture.detectChanges(); // triggers ngOnChanges

      expect(agentService.deactivateAgent).toHaveBeenCalledWith("a");
      expect(component.activeAgentId).toBe("b");
      expect(component.selectedTabIndex).toBe(2);
      expect(component.width).toBe(640); // not reset to the minimum width
    });

    it("does nothing when the input change clears the id", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent("a");
      agentService.activateAgent.mockClear();

      fixture.componentRef.setInput("agentIdToActivate", undefined);
      fixture.detectChanges();

      expect(agentService.activateAgent).not.toHaveBeenCalled();
    });
  });

  describe("openPanel and docked state", () => {
    it("the docked button opens the panel and the minimize button docks it again", () => {
      createComponent();
      const content = fixture.nativeElement.querySelector("#content") as HTMLElement;
      expect(content.hidden).toBe(true);
      expect(fixture.nativeElement.querySelector("#return-button")).toBeNull();

      const dockedButton = fixture.nativeElement.querySelector("#agent-docked-button") as HTMLButtonElement;
      dockedButton.click();
      fixture.detectChanges();

      expect(component.width).toBe(400);
      expect(content.hidden).toBe(false);
      expect(fixture.nativeElement.querySelector("#agent-docked-button")).toBeNull();

      const minimize = fixture.nativeElement.querySelector("#return-button li") as HTMLElement;
      minimize.click();
      fixture.detectChanges();

      expect(component.width).toBe(0);
      expect(component.isDocked).toBe(true);
      expect(fixture.nativeElement.querySelector("#agent-docked-button")).toBeTruthy();
    });
  });

  describe("onAgentCreated", () => {
    it("activates the new agent and switches to its tab after refreshing the list", () => {
      const a = makeAgent("a");
      const b = makeAgent("b");
      agentService.agentList = [a];
      createComponent();

      agentService.agentList = [a, b];
      component.onAgentCreated("b");

      expect(agentService.deactivateAgent).not.toHaveBeenCalled(); // nothing was active
      expect(agentService.activateAgent).toHaveBeenCalledWith("b");
      expect(component.activeAgentId).toBe("b");
      expect(component.agents).toEqual([a, b]);
      expect(component.selectedTabIndex).toBe(2);
    });

    it("deactivates the previously active agent before activating the created one", () => {
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();
      component.onTabSelectChange(1); // activate "a"

      component.onAgentCreated("b");

      expect(agentService.deactivateAgent).toHaveBeenCalledWith("a");
      expect(component.activeAgentId).toBe("b");
    });

    it("leaves the tab selection alone when the created agent is missing from the refreshed list", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();

      component.onAgentCreated("ghost");

      expect(component.activeAgentId).toBe("ghost");
      expect(agentService.activateAgent).toHaveBeenCalledWith("ghost");
      expect(component.selectedTabIndex).toBe(0);
    });

    it("is wired to the registration tab's agentCreated output", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();

      const registration = fixture.debugElement.query(By.directive(StubAgentRegistrationComponent))
        .componentInstance as StubAgentRegistrationComponent;
      registration.agentCreated.emit("a");

      expect(agentService.activateAgent).toHaveBeenCalledWith("a");
      expect(component.selectedTabIndex).toBe(1);
    });
  });

  describe("onTabSelectChange", () => {
    it("selecting the registration tab deactivates the current agent", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();

      component.onTabSelectChange(0); // nothing active yet: deactivation is a no-op
      expect(agentService.deactivateAgent).not.toHaveBeenCalled();

      component.onTabSelectChange(1); // activate "a"
      component.onTabSelectChange(0);

      expect(agentService.deactivateAgent).toHaveBeenCalledWith("a");
      expect(component.activeAgentId).toBeNull();
      expect(component.selectedTabIndex).toBe(0);
    });

    it("ignores a tab index beyond the agent list", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();

      component.onTabSelectChange(5);

      expect(component.selectedTabIndex).toBe(0);
      expect(agentService.activateAgent).not.toHaveBeenCalled();
    });

    it("activates the agent behind the selected tab", () => {
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();

      component.onTabSelectChange(2);

      expect(agentService.activateAgent).toHaveBeenCalledWith("b");
      expect(component.activeAgentId).toBe("b");
      expect(component.selectedTabIndex).toBe(2);
    });

    it("blocks switching to an agent bound to a different workflow and warns", () => {
      agentService.agentList = [makeDelegateAgent("foreign", 99)];
      createComponent();

      component.onTabSelectChange(1);

      expect(notification.warning).toHaveBeenCalledTimes(1);
      const message = notification.warning.mock.calls[0][0] as string;
      expect(message).toContain('Cannot switch to agent "Agent foreign"');
      expect(message).toContain("Open workflow #99");
      expect(agentService.activateAgent).not.toHaveBeenCalled();
      expect(component.selectedTabIndex).toBe(0);
    });

    it("allows switching when the agent's workflow matches the current one", () => {
      agentService.agentList = [makeDelegateAgent("bound", CURRENT_WID)];
      createComponent();

      component.onTabSelectChange(1);

      expect(notification.warning).not.toHaveBeenCalled();
      expect(agentService.activateAgent).toHaveBeenCalledWith("bound");
      expect(component.selectedTabIndex).toBe(1);
    });

    it("treats workflowId 0 as unbound and allows the switch", () => {
      agentService.agentList = [makeDelegateAgent("unbound", 0)];
      createComponent();

      component.onTabSelectChange(1);

      expect(agentService.activateAgent).toHaveBeenCalledWith("unbound");
      expect(component.selectedTabIndex).toBe(1);
    });

    it("re-selecting the already-active agent tab is a no-op", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();

      component.onTabSelectChange(1);
      component.onTabSelectChange(1);

      expect(agentService.activateAgent).toHaveBeenCalledTimes(1);
      expect(agentService.deactivateAgent).not.toHaveBeenCalled();
    });

    it("re-activates without deactivating when the same agent moved to a different tab", () => {
      const a = makeAgent("a");
      const b = makeAgent("b");
      agentService.agentList = [a, b];
      createComponent();
      component.onTabSelectChange(1); // activate "a" on tab 1

      agentService.agentList = [b, a]; // "a" is now behind tab 2
      agentService.agentChangeSubject.next();
      component.onTabSelectChange(2);

      expect(agentService.deactivateAgent).not.toHaveBeenCalled();
      expect(agentService.activateAgent).toHaveBeenCalledTimes(2);
      expect(agentService.activateAgent).toHaveBeenLastCalledWith("a");
      expect(component.selectedTabIndex).toBe(2);
    });
  });

  describe("canSwitchToAgent", () => {
    it("permits agents with no delegate, no workflow id, or a matching workflow id", () => {
      createComponent();
      expect(component.canSwitchToAgent(makeAgent("plain"))).toBe(true);
      expect(component.canSwitchToAgent(makeDelegateAgent("no-wid", undefined))).toBe(true);
      expect(component.canSwitchToAgent(makeDelegateAgent("zero", 0))).toBe(true);
      expect(component.canSwitchToAgent(makeDelegateAgent("same", CURRENT_WID))).toBe(true);
      expect(component.canSwitchToAgent(makeDelegateAgent("other", 99))).toBe(false);
    });

    it("renders a lock icon and mismatch style only on agents from another workflow", () => {
      agentService.agentList = [makeAgent("local"), makeDelegateAgent("foreign", 99)];
      createComponent();

      expect(fixture.nativeElement.querySelectorAll(".workflow-lock-icon").length).toBe(1);
      const titles = fixture.nativeElement.querySelectorAll(".agent-tab-title");
      expect(titles[0].classList.contains("workflow-mismatch")).toBe(false);
      expect(titles[1].classList.contains("workflow-mismatch")).toBe(true);
    });
  });

  describe("deleteAgent", () => {
    it("stops the event and does nothing when the deletion is not confirmed", () => {
      vi.spyOn(window, "confirm").mockReturnValue(false);
      agentService.agentList = [makeAgent("a")];
      createComponent();

      const event = new Event("click");
      const stopPropagation = vi.spyOn(event, "stopPropagation");
      component.deleteAgent("a", event);

      expect(stopPropagation).toHaveBeenCalled();
      expect(agentService.deleteAgent).not.toHaveBeenCalled();
    });

    it("deactivates and deletes the active agent, returning to the registration tab", () => {
      vi.spyOn(window, "confirm").mockReturnValue(true);
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();
      component.onTabSelectChange(1); // "a" active on its own tab

      component.deleteAgent("a", new Event("click"));

      expect(agentService.deactivateAgent).toHaveBeenCalledWith("a");
      expect(component.activeAgentId).toBeNull();
      expect(agentService.deleteAgent).toHaveBeenCalledWith("a");
      expect(component.selectedTabIndex).toBe(0);
    });

    it("shifts the selected index down when deleting a tab before the current one", () => {
      vi.spyOn(window, "confirm").mockReturnValue(true);
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();
      component.onTabSelectChange(2); // "b" active on tab 2

      component.deleteAgent("a", new Event("click"));

      expect(agentService.deactivateAgent).not.toHaveBeenCalled(); // "b" stays active
      expect(component.activeAgentId).toBe("b");
      expect(component.selectedTabIndex).toBe(1);
    });

    it("keeps the selected index when deleting a tab after the current one", () => {
      vi.spyOn(window, "confirm").mockReturnValue(true);
      agentService.agentList = [makeAgent("a"), makeAgent("b")];
      createComponent();
      component.onTabSelectChange(1); // "a" active on tab 1

      component.deleteAgent("b", new Event("click"));

      expect(agentService.deleteAgent).toHaveBeenCalledWith("b");
      expect(component.selectedTabIndex).toBe(1);
    });

    it("logs and leaves the tabs untouched when the backend delete fails", () => {
      vi.spyOn(window, "confirm").mockReturnValue(true);
      const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
      agentService.agentList = [makeAgent("a")];
      agentService.deleteAgent.mockReturnValueOnce(throwError(() => new Error("boom")));
      createComponent();
      component.onTabSelectChange(1);

      component.deleteAgent("a", new Event("click"));

      expect(consoleError).toHaveBeenCalledWith("Failed to delete agent:", expect.any(Error));
      expect(component.selectedTabIndex).toBe(1);
    });

    it("the close button in a tab title asks for confirmation and deletes through the service", () => {
      const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
      agentService.agentList = [makeAgent("a")];
      createComponent();

      (fixture.nativeElement.querySelector(".agent-tab-close") as HTMLButtonElement).click();

      expect(confirmSpy).toHaveBeenCalledWith("Are you sure you want to delete this agent?");
      expect(agentService.deleteAgent).toHaveBeenCalledWith("a");
    });
  });

  describe("panel geometry", () => {
    it("onResize coalesces updates through requestAnimationFrame", () => {
      createComponent();
      const raf = vi.spyOn(window, "requestAnimationFrame").mockImplementation(cb => {
        cb(0);
        return 123;
      });
      const caf = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});

      component.onResize({ width: 640, height: 520 });
      expect(caf).toHaveBeenCalledWith(-1); // initial frame id
      expect(raf).toHaveBeenCalledTimes(1);
      expect(component.width).toBe(640);
      expect(component.height).toBe(520);
      expect(component.id).toBe(123);

      component.onResize({ width: 700, height: 560 });
      expect(caf).toHaveBeenLastCalledWith(123); // cancels the previous frame
      expect(component.width).toBe(700);
      expect(component.height).toBe(560);
    });

    it("handleDragStart undocks the panel", () => {
      createComponent();
      component.isDocked = true;
      component.handleDragStart();
      expect(component.isDocked).toBe(false);
    });
  });

  describe("panel settings persistence", () => {
    it("restores a saved width only when the panel was left undocked", () => {
      localStorage.setItem("agent-panel-width", "512");
      localStorage.setItem("agent-panel-docked", "false");
      createComponent();
      expect(component.width).toBe(512);
    });

    it("stays docked when the panel was saved docked, regardless of the saved width", () => {
      localStorage.setItem("agent-panel-width", "512");
      localStorage.setItem("agent-panel-docked", "true");
      createComponent();
      expect(component.width).toBe(0);
    });

    it("rejects a saved width below the minimum", () => {
      localStorage.setItem("agent-panel-width", "100");
      localStorage.setItem("agent-panel-docked", "false");
      createComponent();
      expect(component.width).toBe(0);
    });

    it("rejects a non-numeric saved width", () => {
      localStorage.setItem("agent-panel-width", "not-a-number");
      localStorage.setItem("agent-panel-docked", "false");
      createComponent();
      expect(component.width).toBe(0);
    });

    it("restores a saved height at or above the minimum", () => {
      localStorage.setItem("agent-panel-height", "600");
      createComponent();
      expect(component.height).toBe(600);
    });

    it("keeps the default height when the saved height is below the minimum", () => {
      localStorage.setItem("agent-panel-height", "300");
      createComponent();
      expect(component.height).toBe(Math.max(450, window.innerHeight * 0.7));
    });

    it("re-applies the saved container style and derives the return position from its transform", () => {
      const style = { cssText: "", transform: "translate3d(10px, 20px, 0px) translate3d(5px, -5px, 0px)" };
      installFakeContainer(style);
      localStorage.setItem("agent-panel-style", "transform: translate3d(15px, 15px, 0px);");

      createComponent();

      expect(style.cssText).toBe("transform: translate3d(15px, 15px, 0px);");
      expect(component.returnPosition).toEqual({ x: -15, y: -15 });
      expect(component.isDocked).toBe(false); // drag position (0,0) differs from (-15,-15)
    });

    it("counts as docked when the saved transform nets out to the origin", () => {
      const style = { cssText: "", transform: "translate3d(5px, 5px, 0px) translate3d(-5px, -5px, 0px)" };
      installFakeContainer(style);
      localStorage.setItem("agent-panel-style", "transform: none;");

      createComponent();

      expect(component.returnPosition.x === 0).toBe(true);
      expect(component.returnPosition.y === 0).toBe(true);
      expect(component.isDocked).toBe(true);
    });

    it("persists dimensions, docked flag and container style, and deactivates the agent on destroy", () => {
      agentService.agentList = [makeAgent("a")];
      createComponent();
      component.onTabSelectChange(1); // "a" active
      component.openPanel(); // width 0 -> 400
      const height = component.height;

      fixture.destroy();

      expect(agentService.deactivateAgent).toHaveBeenCalledWith("a");
      expect(localStorage.getItem("agent-panel-width")).toBe("400");
      expect(localStorage.getItem("agent-panel-height")).toBe(String(height));
      expect(localStorage.getItem("agent-panel-docked")).toBe("false");
      // The panel's own #agent-container is still attached during ngOnDestroy.
      expect(localStorage.getItem("agent-panel-style")).not.toBeNull();
    });

    it("saves docked=true for a closed panel and skips the style when no container exists", () => {
      createComponent();
      vi.spyOn(document, "getElementById").mockReturnValue(null);

      fixture.destroy();

      expect(localStorage.getItem("agent-panel-width")).toBe("0");
      expect(localStorage.getItem("agent-panel-docked")).toBe("true");
      expect(localStorage.getItem("agent-panel-style")).toBeNull();
    });
  });
});
