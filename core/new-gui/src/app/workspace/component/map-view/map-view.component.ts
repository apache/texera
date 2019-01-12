import { Component, OnInit } from '@angular/core'; // AfterViewInit
import * as joint from 'jointjs';
// import { fill } from 'lodash-es';
import { WorkflowEditorComponent } from './../workflow-editor/workflow-editor.component';

@Component({
  selector: 'texera-map-view',
  templateUrl: './map-view.component.html',
  styleUrls: ['./map-view.component.scss']
})
export class MapViewComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }
}

// you can call `JointJSGraph.getCell(id)` to get the cell, then call `cell.get(position)`
