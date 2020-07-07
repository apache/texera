import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA} from '@angular/material/dialog';
import * as c3 from 'c3';
import {PrimitiveArray} from 'c3';

@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit {
  table: object[];
  columns: string[] = [];
  map: Map<string, string[]>;
 

  constructor(@Inject(MAT_DIALOG_DATA) public data: any) {
    this.table = data.table;
    this.map = new Map<string, string[]>();  
  }

  getKeys(map: Map<string, string[]>): string[] {
    return Array.from(map.keys());
}
  ngOnInit() {
 
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
   
    for (let column of this.columns) {
      
      let rows: string[] = [];
     
      for (let row of this.table) {
        rows.push(String((row as any)[column]));
      }
     
      this.map.set(column, rows);
      
    }

   
  }

  ngAfterViewInit() {
    this.onClickGenerateChart();
  }

  async onClickGenerateChart() {

    let dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    let count: number = 0;
    let category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns![i]);
    }

    for (let name of this.map.get(this.columns![0])!) {
     
      let items:[string, ...PrimitiveArray] = [String(name)];
      for (let i = 1; i < this.columns?.length; i++)
        items.push(Number(this.map.get(this.columns![1])![count++]));
      dataToDisplay.push(items);
     
    }
   
    c3.generate({
      size: {
        height: 1080,
        width: 1920
    },
      data: {
          columns: dataToDisplay,
          type: this.data.chartType
      },
      axis: {
        x: {
          type: 'category',
          categories: category
        }
      },
      bindto: "#Chart"
    });
  }

 
 
}
