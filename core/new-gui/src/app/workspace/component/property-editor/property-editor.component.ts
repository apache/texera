import { Component, OnDestroy, OnInit } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { FormlyFormFrameComponent } from './formly-form-frame/formly-form-frame/formly-form-frame.component';
import { BreakpointFrameComponent } from './breakpoint-frame/breakpoint-frame/breakpoint-frame.component';
import { Subscription, timer } from 'rxjs';

/**
 * PropertyEditorComponent is the panel that allows user to edit operator properties.
 *
 * Property Editor uses JSON Schema to automatically generate the form from the JSON Schema of an operator.
 * For example, the JSON Schema of Sentiment Analysis could be:
 *  'properties': {
 *    'attribute': { 'type': 'string' },
 *    'resultAttribute': { 'type': 'string' }
 *  }
 * The automatically generated form will show two input boxes, one titled 'attribute' and one titled 'resultAttribute'.
 * More examples of the operator JSON schema can be found in `mock-operator-metadata.data.ts`
 * More about JSON Schema: Understanding JSON Schema - https://spacetelescope.github.io/understanding-json-schema/
 *
 * OperatorMetadataService will fetch metadata about the operators, which includes the JSON Schema, from the backend.
 *
 * We use library `@ngx-formly` to generate form from json schema
 * https://github.com/ngx-formly/ngx-formly
 *
 * For more details of comparing different libraries, and the problems of the current library,
 *  see `json-schema-library.md`
 *
 * @author Zuozhi Wang
 */
@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss']
})
export class PropertyEditorComponent implements OnInit, OnDestroy {

  frameComponent: any | undefined = undefined;

  subscriptions = new Subscription();

  // operatorID if the component is displaying operator property editor
  public currentOperatorID: string | undefined;

  constructor(public workflowActionService: WorkflowActionService) {}

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  ngOnInit(): void {
    this.registerHighlightEventsHandler();
  }

  switchFrameComponent(targetComponent: any) {
    if (this.frameComponent === targetComponent) {
      return;
    }
    this.frameComponent = targetComponent;
  }

  /**
   * This method changes the property editor according to how operators are highlighted on the workflow editor.
   *
   * Displays the form of the highlighted operator if only one operator is highlighted;
   * Displays the form of the link breakpoint if only one link is highlighted;
   * hides the form if no operator/link is highlighted or multiple operators and/or groups and/or links are highlighted.
   */
  registerHighlightEventsHandler() {
    this.subscriptions.add(Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream()
    ).subscribe(() => {
      const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
      const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
      const highlightLinks = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
      this.switchFrameComponent(undefined);

      timer(0).subscribe(() => {
        if (highlightedOperators.length === 1 && highlightedGroups.length === 0 && highlightLinks.length === 0) {
          this.switchFrameComponent(FormlyFormFrameComponent);
        } else if (highlightLinks.length === 1 && highlightedGroups.length === 0 && highlightedOperators.length === 0) {
          this.switchFrameComponent(BreakpointFrameComponent);
        }
      });

    }));
  }


}
