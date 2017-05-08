import { Component } from '@angular/core';

import { MockDataService } from '../services/mock-data-service';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

@Component({
  moduleId: module.id,
  selector: '[operator-bar]',
  templateUrl: './operator-bar.component.html',
  styleUrls: ['../style.css']
})
export class OperatorBarComponent {

  constructor(private mockDataService: MockDataService, private currentDataService: CurrentDataService) { }

  initialize() {
    var container = jQuery('#the-flowchart').parent();

    this.initializePanzoom(container);
    this.initializeOperators(container);
  }

  initializePanzoom(container: any) {
    // Panzoom initialization...
    jQuery('#the-flowchart').panzoom({
      disablePan: true, // disable the pan
      // contain : true, // if pan, only can pan within flowchart div

    });
    var possibleZooms = [0.7, 0.8, 0.9, 1];
    var currentZoom = 2;
    container.on('mousewheel.focal', function(e) {
      e.preventDefault();
      var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
      var zoomOut = delta;
      // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
      currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut / 40 - 1))));
      jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoom]);
      jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoom], {
        animate: false,
        focal: e
      });
      var ZoomRatio = possibleZooms[currentZoom];
      // enlarge the div ratio so there's more space for the operators
      if (ZoomRatio < 0.8) {
        jQuery('#the-flowchart').css({
          "left": "-354px",
          "top": "-172px",
          "width": "143%",
          "height": "143%",
        });
      } else {
        jQuery('#the-flowchart').css({
          "left": "0px",
          "width": "100%",
          "top": "0px",
          "height": "100%",
        });
      }

    });
    // panzoom end
  }


  initializeOperators(container: any) {
    var findOperatorData = function(opeartorId: number, opeatorList: [any]): any {
      for (let operator of opeatorList) {
        if (operator.id === opeartorId) {
          return operator.jsonData;
        }
      }
      return null;
    }


    let operatorList;
    this.mockDataService.getOperatorList().then(
      data => {
        operatorList = data;
      },
      error => {
        console.log(error);
      }
    );

    var draggableOperators = jQuery('.draggable_operator');
    draggableOperators.draggable({
      cursor: "move",
      opacity: 0.7,

      appendTo: 'body',
      zIndex: 1000,

      helper: function(e) {
        var dragged = jQuery(this);
        var operatorId = parseInt(dragged.data('matcher-type'));
        var operatorData = findOperatorData(operatorId, operatorList);

        return jQuery('#the-flowchart').flowchart('getOperatorElement', operatorData);
      },

      stop: function(e, ui) {
        var dragged = jQuery(this);

        var operatorId = parseInt(dragged.data('matcher-type'));
        var operatorData = findOperatorData(operatorId, operatorList);

        var newData = {
          top: 0,
          left: 20,
          properties: operatorData.properties
        }

        var elOffset = ui.offset;
        var containerOffset = container.offset();

        var positionRatio = jQuery('#the-flowchart').flowchart('getPositionRatio');

        console.log("CONTAINoffset left " + containerOffset.left);
        console.log("CONTAINoffset top " + containerOffset.top);
        console.log("elOffset left = " + elOffset.left * positionRatio);
        console.log("elOffset top = " + elOffset.top * positionRatio);

        if (elOffset.left  > containerOffset.left &&
          elOffset.top > containerOffset.top &&
          elOffset.left  < containerOffset.left + container.width() &&
          elOffset.top < containerOffset.top + container.height()) {

          var flowchartOffset = jQuery('#the-flowchart').offset();

          var relativeLeft = elOffset.left - flowchartOffset.left;
          var relativeTop = elOffset.top - flowchartOffset.top;

          relativeLeft /= positionRatio;
          relativeTop /= positionRatio;

          newData.left = relativeLeft;
          newData.top = relativeTop;

          var operatorNum = jQuery('#the-flowchart').flowchart('addOperator', newData);

          jQuery('#the-flowchart').flowchart('selectOperator', operatorNum); // select the created operator
        }
      }
    });
  }
}
