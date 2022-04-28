import {ChangeDetectorRef, Component, OnInit} from "@angular/core";

import {UntilDestroy} from "@ngneat/until-destroy";
import {DynamicComponentConfig} from "../../../common/type/dynamic-component-config";
import {OperatorMenuFrameComponent} from "./operator-menu-frame/operator-menu-frame.component";

export type LeftFrameComponent =
  | OperatorMenuFrameComponent;

export type LeftFrameComponentConfig = DynamicComponentConfig<LeftFrameComponent>;


@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "./left-panel.component.html",
  styleUrls: ["./left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  frameComponentConfig?: LeftFrameComponentConfig;

  currentOperatorId?: string;

  constructor(
    private changeDetectorRef: ChangeDetectorRef
  ) {
  }

  ngOnInit(): void {
    this.switchFrameComponent({
      component: OperatorMenuFrameComponent,
      componentInputs: {},
    });
  }

  switchFrameComponent(targetConfig?: LeftFrameComponentConfig) {
    if (
      this.frameComponentConfig?.component === targetConfig?.component &&
      this.frameComponentConfig?.componentInputs === targetConfig?.componentInputs
    ) {
      return;
    }

    this.frameComponentConfig = targetConfig;
  }
}
