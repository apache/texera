/*
	Gui <---> Flowchart.js Communication
	GUIJSON <---> TEXTDBJSON Conversion
	
	@Author: Jimmy Wang
*/

$(document).ready(function() {
    var data = {};

    // Apply the plugin on a standard, empty div...
    $('#the-flowchart').flowchart({
      data: data
    });
	
	var operatorI = 0;
	
	var defaultRegex = "zika\s*(virus|fever)";
	var defaultAttributes = "first name, last name";
	var defaultLimit = 10;
	var defaultOffset = 5;

	
/*
	Create Operator Helper Functions
*/


	function getExtraOperators(userInput, panel){
	  var extraOperators = {};
	  
	  if (panel == 'regex-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultRegex;
		}
	    extraOperators['regex'] = userInput;
	  }
	  return extraOperators;
	}
	
	function getAttr(panel, keyword){
		var result = $('#' + panel + keyword).val();
		if(keyword == ' .limit'){
			if (result == null || result == ''){
				result = defaultLimit;
			}
		}
		else if(keyword == ' .offset'){
			if (result == null || result == ''){
				result = defaultOffset;
			}
		}
		else if(keyword == ' .attributes'){
			if (result == null || result == ''){
				result = defaultAttributes;
			}
		}
		return result;
	}

	
/*
	Create Operator Button
*/


    $('.create-operator').click(function() {
	  var panel = $(this).attr('rel');

	  var userInput = $('#' + panel + ' .value').val();	  
	  var extraOperators = getExtraOperators(userInput,panel);
	  
	  var userLimit = getAttr(panel, ' .limit');
	  var userOffset = getAttr(panel, ' .offset');
	  var userAttributes = getAttr(panel, ' .attributes');
	  var operatorName = $('#' + panel + ' button').attr('id');
      var operatorId = operatorName + '_' + operatorI;
      var operatorData = {
        top: 60,
        left: 500,
        properties: {
          title: (operatorName),
          inputs: {
            input_1: {
              label: 'Input 1',
            }
          },
          outputs: {
            output_1: {
              label: 'Output 1',
            }
          },
		  attributes: {
			operator_id: (operatorId),
			operator_type: (operatorName),
		  }
        }
      };
      
	  for(var extraOperator in extraOperators){
		  operatorData.properties.attributes[extraOperator] = extraOperators[extraOperator];
	  }
	  operatorData.properties.attributes['attributes'] = userAttributes;
	  operatorData.properties.attributes['limit'] = userLimit;
	  operatorData.properties.attributes['offset'] = userOffset;
	  
      operatorI++;
      
      $('#the-flowchart').flowchart('createOperator', operatorId, operatorData);
	  
	  data = $('#the-flowchart').flowchart('getData'); 
    });

		
/*
	Delete Operator Button
*/

	
	$('.nav').on('click', '.delete-operator', function() {		
		$('#the-flowchart').flowchart('deleteSelected');
		data = $('#the-flowchart').flowchart('getData');
    });
});