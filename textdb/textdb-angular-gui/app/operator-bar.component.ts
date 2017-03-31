import { Component } from '@angular/core';

import { MockDataService } from './mock-data-service';
import { CurrentDataService } from './current-data-service';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: '[operator-bar]',
    templateUrl: './operator-bar.component.html',
    styleUrls: ['style.css']
})
export class OperatorBarComponent {

    constructor(private mockDataService: MockDataService, private currentDataService: CurrentDataService) { }

    initialize(){
        var container = jQuery('#the-flowchart').parent();
        var draggableOperators = jQuery('.draggable_operator');


        // // panzoom begin

        // jQuery('#the-flowchart').panzoom({
        //   contain: true,
        //   animate: false,
        //   disablePan: true,
        //   $zoomIn: jQuery('.zoom-in'),
        //   $zoomOut: jQuery('.zoom-out'),
        //   $reset: jQuery('.reset'),
        // });
        // jQuery('#the-flowchart').panzoom('pan',-cx + container.width() /2, -cy + container.height()/2);

        var cx = jQuery('#the-flowchart').width() / 2;
        var cy = jQuery('#the-flowchart').height() / 2;
        // Panzoom initialization...
        jQuery('#the-flowchart').panzoom({
          disablePan: true, // disable the pan
          // contain : true, // if pan, only can pan within flowchart div

        });
        var possibleZooms = [0.7, 0.8, 0.9, 1];
        var currentZoom = 2;
        container.on('mousewheel.focal', function( e ) {
            e.preventDefault();
            var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
            var zoomOut = delta;
            // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
            currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut/40 - 1))));
            jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoom]);
            jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoom], {
                animate: false,
                focal: e
            });
            var ZoomRatio = possibleZooms[currentZoom];
            // enlarge the div ratio so there's more space for the operators
            if (ZoomRatio < 0.8){
              jQuery('#the-flowchart').css({
                "left" : "-140px",
                "top" : "-90px",
                "width" : "120%",
                "height" : "120%",
              });
            } else {
              jQuery('#the-flowchart').css({
                "left" : "0px",
                "width" : "100%",
                "top" : "0px",
                "height" : "100%",
              });
            }

        });
        // // panzoom end

        var current = this;
        var operatorI = 0;

        var default_matchers;
        current.mockDataService.getMatchers().then(
            matchers => {
                default_matchers = matchers;
            },
            error => {
                console.log(error);
            }
        );

        draggableOperators.draggable({
            cursor: "move",
            opacity: 0.7,

            appendTo: 'body',
            zIndex: 1000,

            helper: function(e) {
                var dragged = jQuery(this);
                var matcherId = parseInt(dragged.data('matcher-type'));
                var data = default_matchers[matcherId].jsonData;
                return jQuery('#the-flowchart').flowchart('getOperatorElement', data);
            },
            stop: function(e, ui) {
                var dragged = jQuery(this);
                var matcherId = parseInt(dragged.data('matcher-type'));
                var data = default_matchers[matcherId].jsonData;

                var new_data = {
                  top : 0,
                  left : 20,
                  properties : data.properties
                }

                var elOffset = ui.offset;
                var containerOffset = container.offset();
                if (elOffset.left > containerOffset.left &&
                    elOffset.top > containerOffset.top &&
                    elOffset.left < containerOffset.left + container.width() &&
                    elOffset.top < containerOffset.top + container.height()) {

                    var flowchartOffset = jQuery('#the-flowchart').offset();

                    var relativeLeft = elOffset.left - flowchartOffset.left;
                    var relativeTop = elOffset.top - flowchartOffset.top;

                    var positionRatio = jQuery('#the-flowchart').flowchart('getPositionRatio');
                    relativeLeft /= positionRatio;
                    relativeTop /= positionRatio;

                    new_data.left = relativeLeft;
                    new_data.top = relativeTop;

                    var operatorNum = jQuery('#the-flowchart').flowchart('addOperator', new_data);
                    // current.currentDataService.selectData(operatorNum);
                    // current.currentDataService.addData(new_data, operatorNum, jQuery('#the-flowchart').flowchart('getData'));
                    jQuery('#the-flowchart').flowchart('selectOperator', operatorNum); // select the created operator
                    console.log("Created Operator id = " + operatorNum);
                }
            }
        });
    }
}
