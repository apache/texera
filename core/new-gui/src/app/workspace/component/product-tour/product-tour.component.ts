import { Component, OnInit } from "@angular/core";
import { TourService, IStepOption } from "ngx-tour-ng-bootstrap";

/**
 * ProductTourComponent is the product tour that shows basic product tutorial.
 *
 * Product tour library built with Angular (2+).
 * ngx-tour-ngx-bootstrap is an implementation of the tour ui that uses ngx-bootstrap popovers to display tour steps.
 *
 * The component has a step list in this.tourService.initialize that can add, edit or delete steps.
 * Define anchor points for the tour steps by adding the tourAnchor directive throughout components.
 *
 * <div tourAnchor="some.anchor.id">...</div>
 *
 * Define your tour steps using tourService.initialize(steps).
 *
 * For the full text of the library, go to https://github.com/isaacplmann/ngx-tour
 *
 * The screenshots were done by GIPHY Capture
 *
 *
 * @author Bolin Chen
 */
/* eslint-disable max-len */
@Component({
  selector: "texera-product-tour",
  templateUrl: "./product-tour.component.html",
  styleUrls: ["./product-tour.component.scss"],
})
export class ProductTourComponent {
  private steps: IStepOption[] = [
    {
      anchorId: "texera-navigation-grid-container",
      content: `
    <div class="intro">
    <center>
      <h3>Welcome to Texera!</h3>
    </center>
    <br>
    <p>
    Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows. Use "<b>« Prev</b>"/"
    <b>Next »</b>" or "Left"/"Right" arrow keys to navigate through the tutorial.
    </p>
    <br>
    <center>
    <img src="../../../assets/Tutor_Intro_Sample.jpeg" alt="intro img">
    </center>
    <br><br>
    </div>
    `,
      placement: "bottom",
      title: "Welcome",
      preventScrolling: true,
    },
    {
      anchorId: "texera-operator-panel",
      content: `
    <p>This <b>Operator Panel</b> contains all the operators you need to construct a workflow. </p>
    <p>Let's create a basic twitter text analysis workflow to demonstrate the steps involved in creation of a workflow.</p>
    <p>Open the first operator group named <b>Source</b>. This group contains all the operators needed to import a source dataset into a workflow.</p>
    <center><img src="../../../assets/Tutor_OpenSection_Sample.gif" height="400" width="590"></center>
    <br><br>
    `,
      placement: "right",
      title: "Operator Panel",
      preventScrolling: true,
    },
    {
      anchorId: "texera-operator-panel",
      content: `
    <p>Drag the <b>Source: Scan</b> operator and drop it on the workflow panel. </p>
    <p><b>Source: Scan</b> operator reads records one by one from a table.</p>
    <center><img src="../../../assets/Tutor_Intro_Drag_Srouce.gif" height="450" width="600"></center>
    <br><br>
    `,
      title: "Select Operator",
      placement: "right",
      preventScrolling: true,
    },
    {
      anchorId: "texera-property-editor-grid-container",
      content: `
    <p>This is the <b>property editor</b> panel where users can set the properties of an operator. </p>
    <p>Now, we want to edit the <i>input table</i> property of <b>Source: Scan</b> operator and set it to <b>twitter_sample</b> table.
    Please type <b>twitter_sample</b> in the space for the <i>input table</i> property.</p>
    <center><img src="../../../assets/Tutor_Property_Sample.gif" height="260" width="500"></center>
    <br><br>
    `,
      placement: "left",
      title: "Property Editor",
      preventScrolling: true,
    },
    {
      anchorId: "texera-operator-panel",
      content: `<p>To view the output of a workflow, you need to end the workflow with a <b>View Results</b> operator. Please open the <b>View Results</b> section.</p>
    <center><img src="../../../assets/Tutor_OpenResult_Sample.gif" height="270" width="500"></center>
    <br><br>
    `,
      placement: "right",
      title: "Operator Panel",
      preventScrolling: true,
    },
    {
      anchorId: "texera-operator-panel",
      content: `
    <p>Drag <b>View Results</b> operator and drop it on the workflow panel.</p>
    <center><img src="../../../assets/Tutor_Intro_Drag_Result.gif" height="400" width="590"></center>
    <br><br>
    `,
      placement: "right",
      title: "Select Operator",
      preventScrolling: true,
    },
    {
      anchorId: "texera-property-editor-grid-container",
      content: `
    <p>Connect the <b>Source:Scan</b> and the <b>View Results</b> operators.</p><center>
    <img src="../../../assets/Tutor_JointJS_Sample.gif" height="150" width="500"></center>
    <br><br>
    `,
      placement: "left",
      title: "Connecting operators",
      preventScrolling: true,
    },
    {
      anchorId: "texera-workspace-navigation-run",
      content: `
    <p>Click the <b>Run</b> button to execute the workflow.</p>
    `,
      title: "Running the workflow",
      placement: "bottom",
      preventScrolling: true,
    },
    {
      anchorId: "texera-result-view-grid-container",
      content: `
    <p>You can view the results in the <b>Results Panel</b> here.</p>
    `,
      placement: "top",
      title: "Viewing the results",
      preventScrolling: true,
    },
    {
      anchorId: "texera-navigation-grid-container",
      content: `
    <center><h3>Congratulations!</h3></center>
    <p>You have finished the basic tutorial. </p>
    <p>There are many other operators that you can use to form a workflow.</p>
    <center><img src="../../../assets/Tutor_End_Sample.gif" height="335" width="640"></center>
    <br><br>
    `,
      placement: "bottom",
      title: "Ending of tutorial",
      preventScrolling: true,
    },
  ];

  constructor(public tourService: TourService) {
    this.tourService.initialize(this.steps);
  }
}
