import { Component, Input, Output, EventEmitter } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";

// Different states the power button can be in
export enum PowerState {
  Off = "off",
  Initializing = "initializing",
  Running = "running",
  Stopping = "stopping",
}

@UntilDestroy()
@Component({
  selector: "texera-power-button",
  templateUrl: "./power-button.component.html",
  styleUrls: ["./power-button.component.scss"],
})
export class PowerButtonComponent {
  @Input() disabled = false;
  @Input() state: PowerState = PowerState.Off;
  @Output() stateChange = new EventEmitter<PowerState>();

  onClick() {
    let newState: PowerState;
    switch (this.state) {
      case PowerState.Off:
        newState = PowerState.Initializing;
        break;
      case PowerState.Initializing:
      case PowerState.Stopping:
        // Currently do not allow for state change by clicking
        return;
      case PowerState.Running:
        newState = PowerState.Stopping;
        break;
    }
    this.stateChange.emit(newState);
  }

  getButtonText(): string {
    switch (this.state) {
      case PowerState.Off:
        return "Start";
      case PowerState.Initializing:
        return "Initializing...";
      case PowerState.Running:
        return "Running";
      case PowerState.Stopping:
        return "Stopping...";
    }
  }

  getIconType(): string {
    switch (this.state) {
      case PowerState.Off:
        return "step-forward";
      case PowerState.Initializing:
        return "loading";
      case PowerState.Running:
        return "pause";
      case PowerState.Stopping:
        return "loading";
    }
  }

  isRunning(): boolean {
    return this.state === "running";
  }
}
