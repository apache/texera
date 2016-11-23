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
	var selectedOperator = '';
	var editOperators = [];
	
	var defaultRegex = "zika\s*(virus|fever)";
	var defaultKeyword = "Zika";
	var defaultDict = "SampleDict1.txt";
	var defaultFuzzy = "FuzzyWuzzy";
	var thresholdRatio = 0.8;
	var nlpArray = ["noun", "verb", "adjective", "adverb", "ne_all", "number", "location", "person", "organization", "money", "percent", "date", "time"];
	var defaultNlp = "ne_all";
	var defaultDataSource = "collection name";
	var defaultFileSink = "output.txt";
	var defaultAttributeID = "John";
	var defaultPredicateType = "CharacterDistance";
	var defaultDistance = 10;
	var defaultAttributes = "first name, last name";
	var defaultLimit = 10;
	var defaultOffset = 5;


/*
	Process Operators to Server (GUIJSON --> TEXTDBJSON --> Server)
*/


	$('.process-query').on('click', function() {
		
		var GUIJSON = $('#the-flowchart').flowchart('getData');
		
		var TEXTDBJSON = {};
		var operators = [];
		var links = [];
		
		for(var operatorIndex in GUIJSON.operators){
			if (GUIJSON.operators.hasOwnProperty(operatorIndex)){
				var attributes = {};
				
				for(var attribute in GUIJSON['operators'][operatorIndex]['properties']['attributes']){
					if (GUIJSON['operators'][operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)){
						attributes[attribute] = GUIJSON['operators'][operatorIndex]['properties']['attributes'][attribute];
					}
				}
				operators.push(attributes);
			}
		}	
		
		for(var link in GUIJSON.links){
			var destination = {};
			if (GUIJSON['links'][link].hasOwnProperty("fromOperator")){
				destination["from"] = GUIJSON['links'][link]['fromOperator'];
				destination["to"] = GUIJSON['links'][link]['toOperator'];
				links.push(destination);
			}
		}
		TEXTDBJSON.operators = operators;
		TEXTDBJSON.links = links;
		
		// console.log(operators);
		// console.log(links)
		// console.log(data);
		// console.log(JSON.stringify(data));
		// console.log(JSON.stringify(TEXTDBJSON));
		
		$.ajax({
            url: "http://localhost:8080/queryplan/execute",
            type: "POST",
            data: JSON.stringify(TEXTDBJSON),
            dataType: "text",
            contentType: "application/json",
            success: function(returnedData){
                console.log("SUCCESS\n");
                console.log(JSON.stringify(returnedData));
            },
            error: function(xhr, status, err){
                console.log("ERROR");
				console.log(xhr.status);
				console.log(JSON.stringify(xhr));
				console.log(JSON.stringify(status));
				console.log(JSON.stringify(err));
            }
        });
	});

	
/*
	Attribute Pop-Up Box Helper Function
*/	


	function getPopupText(output){
		var result = "";
		for(var attr in output){
			if(attr == 'operator_id'){
				continue;
			}
			if(output.hasOwnProperty(attr)){
				var splitString = attr.split("_");
				for(var splitPart in splitString){
					result += ' ' + splitString[splitPart].charAt(0).toUpperCase() + splitString[splitPart].slice(1);
				}
				result += ": ";
				result += '<b>' + output[attr] + '</b>' + '<br />';
			}
		}
		result += '<br />';
		return result;
	}

/*
	Attribute Pop-Up Box
*/

	
	$('#the-flowchart').on('click', '.flowchart-operators-layer.unselectable div .flowchart-operator-title', function() {
		selectedOperator = $('#the-flowchart').flowchart('getSelectedOperatorId');
		var output = data['operators'][selectedOperator]['properties']['attributes'];
		var title = data['operators'][selectedOperator]['properties']['title'];
		// console.log(data['operators'][selectedOperator]['attributes']);
		// console.log(JSON.stringify(data['operators'][selectedOperator]['attributes']));
		// console.log(output);
		// console.log(JSON.stringify(output));
		
		$('.popup').animate({
            'bottom': '0'
        }, 200);
		
		$('#attributes').css({
			'visibility': 'visible'
		});
		
		$('#attributes').text(getPopupText(output));
		$('#attributes').html($('#attributes').text());
		
		var editButton = $('<button class="edit-operator">Edit</button>');
        $('#attributes').append(editButton);
		
		var deleteButton = $('<button class="delete-operator">Delete</button>');
        $('#attributes').append(deleteButton);
		
		$('#attributes button').css({
			'margin': '5px',
			'border': '1px solid black'
		});
		
		$('.band').html('Attributes for <em>' + title + '</em>');
	});

	
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
	  else if (panel == 'keyword-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultKeyword;
		}
		extraOperators['keyword'] = userInput;
		extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
	  }
	  else if (panel == 'dictionary-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultDict;
		}
		extraOperators['dictionary'] = userInput;
		extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
	  }
	  else if (panel == 'fuzzy-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultFuzzy;
		}
		extraOperators['query'] = userInput;
		extraOperators['threshold_ratio'] = thresholdRatio;
	  }
	  else if (panel == 'nlp-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultNlp;
		}
		else if(nlpArray.indexOf(userInput.toLowerCase()) == -1){
			alert('Please choose an NLP from the following: ["noun", "verb", "adjective", "adverb", "ne_all", "number", "location", "person", "organization", "money", "percent", "date", "time"]');
			return;
		}
		extraOperators['nlp_type'] = userInput;
	  }
	  else if (panel == 'keyword-source-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultKeyword;
		}
		extraOperators['keyword'] = userInput;
		
		var dataSource = $('#' + panel + ' .data-source').val();
		if (dataSource == null || dataSource == ''){
			dataSource = defaultDataSource;
		}
		extraOperators['data_source'] = dataSource;
		
		extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
	  }
	  else if (panel == 'file-sink-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultFileSink;
		}
		extraOperators['file_path'] = userInput;
	  }
	  else if (panel == 'join-panel'){
		if (userInput == null || userInput == ''){
			userInput = defaultAttributeID;
		}
		extraOperators['id_attribute'] = userInput;
		
		var predicateType = $('#' + panel + ' .predicate-type').val();
		if (predicateType == null || predicateType == ''){
			predicateType = defaultPredicateType;
		}
		extraOperators['predicate_type'] = predicateType;
		
		var distance = $('#' + panel + ' .distance').val();
		if (distance == null || distance == ''){
			distance = defaultDistance;
		}
		extraOperators['distance'] = distance;
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
	Edit Operator Button Helper Function
*/
	
	
	function getHtml(attr, attrValue){
		var resultString = '';
		var classString = attr.replace(/_/g, '-');
		if(attr == 'matching_type'){
			resultString += '<select class="matching-type"><option value="conjunction"';
			if(attrValue == 'conjunction'){
				resultString += ' selected';
			}
			resultString += '>Conjunction</option><option value="phrase"';
			if(attrValue == 'phrase'){
				resultString += ' selected';
			}
			resultString += '>Phrase</option><option value="substring"';
			if(attrValue == 'substring'){
				resultString += ' selected';
			}
			resultString += '>Substring</option></select>';
		}
		else if(attr == 'dictionary'){
			resultString += '<input type="file" class="dictionary" placeholder="Enter File">';
		}
		else{
			resultString += '<input type="text" class="' + classString + '" value="' + attrValue + '">';
		}
		editOperators.push(classString);
		return resultString;		
	}
	
	
/*
	Edit Operator Button
*/

	
	$('#attributes').on('click', '.edit-operator', function() {
		editOperators = [];
		var output = data['operators'][selectedOperator]['properties']['attributes'];
		
		$('#attributes').text(function(){
			var result = "";
			for(var attr in output){
				if(attr == 'operator_id'){
					continue;
				}
				else if(output.hasOwnProperty(attr)){
					var splitString = attr.split("_");
					for(var splitPart in splitString){
						result += ' ' + splitString[splitPart].charAt(0).toUpperCase() + splitString[splitPart].slice(1);
					}
					result += ": ";
					if(attr == 'operator_type'){
						result += output[attr] + "\n";					
					}
					else{
						inputHtml = getHtml(attr,output[attr]);
						result += "\t" + "\n";
						result = result.replace(/\t/g, inputHtml);
					}
				}
			}
			result += "\n";
			result.replace(/\n/g, '<br />');
			return result;
		});
		
		$('#attributes').html($('#attributes').text());
		
		var confirmChangesButton = $('<button class="confirm-button">Confirm Changes</button>');
        $('#attributes').append(confirmChangesButton);
		
		var cancelButton = $('<button class="cancel-button">Cancel</button>');
        $('#attributes').append(cancelButton);
		
		$('#attributes button').css({
			'margin': '5px',
			'border': '1px solid black'
		});
	});


/*
	Confirm Changes Button
*/	
	
	
	$('#attributes').on('click', '.confirm-button', function() {
	  data = $('#the-flowchart').flowchart('getData');
	  var output = data['operators'][selectedOperator]['properties']['attributes'];
	  var panel = $(this).parent().attr('class');
	  
	  var operatorTop = data['operators'][selectedOperator]['top'];
	  var operatorLeft = data['operators'][selectedOperator]['left'];
	  var operatorId = output['operator_id'];
	  var operatorName = output['operator_type'];
	  
	  var operatorData = {
        top: (operatorTop),
        left: (operatorLeft),
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
	  
	  for(var otherOperator in editOperators){
		var attr = editOperators[otherOperator].replace(/-/, '_');
		var result = $('.' + panel + ' .' + editOperators[otherOperator]).val();
		if(((result == '') || (result == null)) && (attr == 'dictionary')){
			result = defaultDict;
		}
		operatorData.properties.attributes[attr] = result;
	  }
      
	  $('#the-flowchart').flowchart('deleteSelected');
      $('#the-flowchart').flowchart('createOperator', operatorId, operatorData);
	  $('#the-flowchart').flowchart('selectOperator', operatorId);
	  selectedOperator = $('#the-flowchart').flowchart('getSelectedOperatorId');
	  
	  data = $('#the-flowchart').flowchart('getData');
	  output = data['operators'][selectedOperator]['properties']['attributes'];
	  
	  var title = data['operators'][selectedOperator]['properties']['title'];
	  $('.band').html('Attributes for <em>' + title + '</em>');
	  
	  $('#attributes').text(getPopupText(output));
	  $('#attributes').html($('#attributes').text());
	  
	  var editButton = $('<button class="edit-operator">Edit</button>');
	  $('#attributes').append(editButton);
		
	  var deleteButton = $('<button class="delete-operator">Delete</button>');
	  $('#attributes').append(deleteButton);
		
	  $('#attributes button').css({
			'margin': '5px',
			'border': '1px solid black'
	  });
	});
	
	
/*
	Cancel Edit Button
*/	
	
	
	$('#attributes').on('click', '.cancel-button', function() {
		var output = data['operators'][selectedOperator]['properties']['attributes'];
		$('#attributes').text(getPopupText(output));
		$('#attributes').html($('#attributes').text());
		
		var editButton = $('<button class="edit-operator">Edit</button>');
        $('#attributes').append(editButton);
		
		var deleteButton = $('<button class="delete-operator">Delete</button>');
        $('#attributes').append(deleteButton);
		
		$('#attributes button').css({
			'margin': '5px',
			'border': '1px solid black'
		});
	});
		
		
/*
	Delete Operator Button
*/

	
	$('#attributes, .nav').on('click', '.delete-operator', function() {
		data = $('#the-flowchart').flowchart('getData');
		$('#attributes').css({
			'visibility': 'hidden'
		});
		
		$('.band').text('Attributes');
		
		$('#the-flowchart').flowchart('deleteSelected');
		data = $('#the-flowchart').flowchart('getData');
    });
});