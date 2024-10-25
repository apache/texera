import { ComponentFixture, TestBed } from "@angular/core/testing";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input.component";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { SimpleChanges } from "@angular/core";
import * as monaco from "monaco-editor";

describe("BreakpointConditionInputComponent", () => {
  let component: BreakpointConditionInputComponent;
  let fixture: ComponentFixture<BreakpointConditionInputComponent>;
  let mockUdfDebugService: jasmine.SpyObj<UdfDebugService>;
  let editorElement: HTMLElement;

  beforeEach(async () => {
    // Create a mock UdfDebugService
    mockUdfDebugService = jasmine.createSpyObj("UdfDebugService", ["getCondition", "doUpdateBreakpointCondition"]);

    await TestBed.configureTestingModule({
      declarations: [BreakpointConditionInputComponent],
      providers: [{ provide: UdfDebugService, useValue: mockUdfDebugService }],
    }).compileComponents();

    fixture = TestBed.createComponent(BreakpointConditionInputComponent);
    component = fixture.componentInstance;

    // Create and attach a <div> to host the Monaco editor
    editorElement = document.createElement("div");
    editorElement.style.width = "800px";
    editorElement.style.height = "600px";
    document.body.appendChild(editorElement); // Attach to the DOM

    // Initialize the Monaco editor
    component.monacoEditor = monaco.editor.create(editorElement, {
      value: "function hello() {\n\tconsole.log(\"Hello, world!\");\n}",
      language: "javascript",
    });

    // Set required inputs
    component.operatorId = "test-operator";
    component.lineNum = 1;

    fixture.detectChanges(); // Trigger Angular's change detection
  });

  afterEach(() => {
    // Clean up the editor and DOM element after each test
    component.monacoEditor.dispose();
    editorElement.remove();
    component.closeEmitter.emit();
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should update the condition when lineNum changes", () => {
    mockUdfDebugService.getCondition.and.returnValue("existing condition");

    const changes: SimpleChanges = {
      lineNum: {
        currentValue: 2,
        previousValue: 1,
        firstChange: false,
        isFirstChange: () => false,
      },
    };

    component.ngOnChanges(changes);

    expect(component.condition).toBe("existing condition");
  });

  it("should handle Enter key event and save the condition", () => {
    const emitSpy = spyOn(component.closeEmitter, "emit");
    const event = new KeyboardEvent("keydown", { key: "Enter" });

    component.condition = " new condition ";
    component.handleEvent(event);

    expect(mockUdfDebugService.doUpdateBreakpointCondition).toHaveBeenCalledWith("test-operator", 1, "new condition");
    expect(emitSpy).toHaveBeenCalled();
  });

  it("should not handle Enter key event if shift key is pressed", () => {
    const emitSpy = spyOn(component.closeEmitter, "emit");
    const event = new KeyboardEvent("keydown", { key: "Enter", shiftKey: true });

    component.handleEvent(event);

    expect(mockUdfDebugService.doUpdateBreakpointCondition).not.toHaveBeenCalled();
    expect(emitSpy).not.toHaveBeenCalled();
  });

  it("should emit close event on focusout", () => {
    const emitSpy = spyOn(component.closeEmitter, "emit");

    component.handleEvent(); // Simulate focusout

    expect(emitSpy).toHaveBeenCalled();
  });
});
