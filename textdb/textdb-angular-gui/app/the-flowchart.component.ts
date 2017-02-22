import { Component } from '@angular/core';
declare var jQuery: any;

@Component({
	moduleId: module.id,
  selector: 'flowchart-container',
	template: `
		<div id="the-flowchart"></div>
	`,
	styleUrls: ['style.css']
})
export class TheFlowchartComponent {

	initialize() {
		var data = {
			operators: {
				operator1: {
					top: 20,
					left: 20,
					properties: {
						title: 'Operator 1',
						inputs: {},
						outputs: {
							output_1: {
								label: 'Output 1',
							}
						}
					}
				},
				operator2: {
					top: 80,
					left: 300,
					properties: {
						title: 'Operator 2',
						inputs: {
							input_1: {
								label: 'Input 1',
							},
							input_2: {
								label: 'Input 2',
							},
						},
						outputs: {}
					}
				},
			},
			links: {
				link_1: {
					fromOperator: 'operator1',
					fromConnector: 'output_1',
					toOperator: 'operator2',
					toConnector: 'input_2',
				},
			}
		};
		
		var container = jQuery('#the-flowchart').parent();
		
		jQuery('#the-flowchart').flowchart({
			data: data
		});
		
		var draggableOperators = jQuery('.draggable_operator');
    
    var operatorId = 0;
        
    draggableOperators.draggable({
			cursor: "move",
			opacity: 0.7,

			appendTo: 'body',
			zIndex: 1000,
			
			helper: function(e) {
				var dragged = jQuery(this);
				var nbInputs = parseInt(dragged.data('nb-inputs'));
				var nbOutputs = parseInt(dragged.data('nb-outputs'));
				var data = {
					properties: {
						title: dragged.text(),
						inputs: {},
						outputs: {}
					} 
				};
				
				var i = 0;
				for (i = 0; i < nbInputs; i++) {
					data.properties.inputs['input_' + i] = {
						label: 'Input ' + (i + 1)
					};
				}
				for (i = 0; i < nbOutputs; i++) {
					data.properties.outputs['output_' + i] = {
						label: 'Output ' + (i + 1)
					};
				}
		
				return jQuery('#the-flowchart').flowchart('getOperatorElement', data);
			},
			stop: function(e, ui) {
				var dragged = jQuery(this);
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
					
					var nbInputs = parseInt(dragged.data('nb-inputs'));
					var nbOutputs = parseInt(dragged.data('nb-outputs'));
					var data = {
						left: relativeLeft,
						top: relativeTop,
						properties: {
							title: dragged.text(),
							inputs: {},
							outputs: {}
						} 
					};
					
					var i = 0;
					for (i = 0; i < nbInputs; i++) {
						data.properties.inputs['input_' + i] = {
							label: 'Input ' + (i + 1)
						};
					}
					for (i = 0; i < nbOutputs; i++) {
						data.properties.outputs['output_' + i] = {
							label: 'Output ' + (i + 1)
						};
					}

					
					jQuery('#the-flowchart').flowchart('addOperator', data);
				}
			}
    });
	}
}
