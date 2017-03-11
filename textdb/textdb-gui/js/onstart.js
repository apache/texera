/*
	Gui <---> Flowchart.js Communication
	GUIJSON <---> TEXTDBJSON Conversion

	@Author: Jimmy Wang
*/

var debug = null;

// main operation (holds all the methods for the buttons and holds the buttons itself)
var setup = function(){
	var data = {};

	//
	// Panzoom initialization...
	// $('#the-flowchart').panzoom({
	// 	minScale : 0,
	// 	$zoomRange : $("input[type = 'range']"),
	// 	disablePan : true,
	// 	disableZoom: true
	// });

	// Apply the plugin on a standard, empty div...
	$('#the-flowchart').flowchart({
		data: data,
		multipleLinksOnOutput: true
	});


	var operatorI = 0;
	var selectedOperator = '';
	var editOperators = [];

	var DEFAULT_REGEX = "zika\s*(virus|fever)";
	var DEFAULT_KEYWORD = "Zika";
	var DEFAULT_DICT = "SampleDict1.txt";
	var DEFAULT_FUZZY = "FuzzyWuzzy";
	var THRESHOLD_RATIO = 0.8;
	var DEFAULT_NLP = "ne_all";
	var DEFAULT_DATA_SOURCE = "collection name";
	var DEFAULT_FILE_SINK = "output.txt";
	var DEFAULT_ATTRIBUTE_ID = "_id";
	var DEFAULT_PREDICATE_TYPE = "Characterthreshold";
	var DEFAULT_THRESHOLD = 100; //threshold
	var DEFAULT_ATTRIBUTES = "first name, last name";
	var DEFAULT_LIMIT = null;
	var DEFAULT_OFFSET = null;

	var DEFAULT_IN_ATTRIBUTE = "name1";
	var DEFAULT_OUT_ATTRIBUTE = "name2"

	/*
		Helper Functions
	*/
	//Helper Function for Process Queries that displays the results after hitting "Process Query"
	function createResultFrame(message){
		var resultJSON = JSON.parse(message['message']);

		var resultFrame = $('<div class="result-frame"><div class="result-box"><div class="result-box-band">Return Result<div class="result-frame-close"><img src="img/close-icon.png"></div></div><div class="return-result"></div></div></div>');
		$('body').append(resultFrame);
		
		var node = new PrettyJSON.view.Node({
			el:$('.return-result'),
			data:resultJSON
		});
	}

	//Create Operator Helper Function
	function getExtraOperators(userInput, panel){
		var extraOperators = {};

		if (panel == 'regex-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_REGEX;
			}
			extraOperators['regex'] = userInput;
		}
		else if (panel == 'keyword-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_KEYWORD;
			}
			extraOperators['keyword'] = userInput;
			extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
		}
		else if (panel == 'dictionary-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_DICT;
			}
			extraOperators['dictionary'] = userInput;
			extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
		}
		else if (panel == 'fuzzy-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_FUZZY;
			}
			extraOperators['query'] = userInput;
			extraOperators['threshold_ratio'] = THRESHOLD_RATIO;
		}
		else if (panel == 'nlp-panel'){
			extraOperators['nlp_type'] =  $('#' + panel + ' .nlp-type').val();;
		}
		else if (panel == 'keyword-source-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_KEYWORD;
			}
			extraOperators['keyword'] = userInput;

			var dataSource = $('#' + panel + ' .data-source').val();
			if (dataSource == null || dataSource == ''){
				dataSource = DEFAULT_DATA_SOURCE;
			}
			extraOperators['data_source'] = dataSource;

			extraOperators['matching_type'] = $('#' + panel + ' .matching-type').val();
		}
		else if (panel == 'file-sink-panel'){
			if (userInput == null || userInput == ''){
				userInput = DEFAULT_FILE_SINK;
			}
			extraOperators['file_path'] = userInput;
		}
		else if (panel == 'join-panel'){
			// if (userInput == null || userInput == ''){
			// 	userInput = DEFAULT_ATTRIBUTE_ID;
			// }
			// extraOperators['id_attribute'] = userInput;

			var predicateType = $('#' + panel + ' .predicate-type').val();
			if (predicateType == null || predicateType == ''){
				predicateType = DEFAULT_PREDICATE_TYPE;
			}
			extraOperators['predicate_type'] = predicateType;

			var threshold = $('#' + panel + ' .threshold').val();
			if (threshold == null || threshold == ''){
				threshold = DEFAULT_THRESHOLD;
			}
			extraOperators['threshold'] = threshold;



		}
		return extraOperators;
	};

	//Create Operator Helper Function
	function getAttr(panel, keyword){
		var result = $('#' + panel + keyword).val();
		if(keyword == ' .limit'){
			if (result == null || result == ''){
				result = DEFAULT_LIMIT;
			}
		}
		else if(keyword == ' .offset'){
			if (result == null || result == ''){
				result = DEFAULT_OFFSET;
			}
		}
		else if(keyword == ' .attributes'){
			if (result == null || result == ''){
				result = DEFAULT_ATTRIBUTES;
			}
		}
		// HENRY change begin
		else if (keyword == ' .outer-attribute'){
			if (result == null || result == ''){
				result = DEFAULT_OUT_ATTRIBUTE;
			}
		}
		else if (keyword == ' .inner-attribute'){
			if (result == null || result == ''){
				result = DEFAULT_IN_ATTRIBUTE;
			}
		}
		// Henry change end;
		return result;
	};

	//Edit Operator Button Helper Function
		//getEditInputHtml Helper Function (get Html for a specific operator)
	function getHtml(attr, attrValue){
		var resultString = '';
		var classString = attr.replace(/_/g, '-');
		if(attr == 'matching_type'){
			var matchingTypeArray = ["conjunction", "phrase", "substring"];
			resultString += '<select class="matching-type"><option value="' + attrValue + '" selected>' + attrValue + '</option>';

			//indexOf is not supported in IE6,7,8
			matchingTypeArray.splice(matchingTypeArray.indexOf(attrValue),1);

			for(var index in matchingTypeArray){
				resultString += '<option value="' + matchingTypeArray[index] + '">' + matchingTypeArray[index] + '</option>';
			}
			resultString += '</select>';
		}
		else if(attr == 'nlp_type'){
			var nlpArray = ["noun", "verb", "adjective", "adverb", "ne_all", "number", "location", "person", "organization", "money", "percent", "date", "time"];
			resultString += '<select class="nlp-type"><option value="' + attrValue + '" selected>' + attrValue + '</option>';

			//indexOf is not supported in IE6,7,8
			nlpArray.splice(nlpArray.indexOf(attrValue),1);

			for(var index in nlpArray){
				resultString += '<option value="' + nlpArray[index] + '">' + nlpArray[index] + '</option>';
			}
			resultString += '</select>';
		}
		else if(attr == 'dictionary'){
			resultString += '<input type="text" class="dictionary" placeholder="Enter Dictionary">';
		}
		else{
			resultString += '<input type="text" class="' + classString + '" value="' + attrValue + '">';
		}
		editOperators.push(classString);
		return resultString;
	}

	//Edit Operator Help Function (creates the html code for the input boxes to edit an operator)
	var getEditInputHtml = function(output){
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
					var inputHtml = getHtml(attr,output[attr]);
					result += "\t" + "\n";
					result = result.replace(/\t/g, inputHtml);
				}
			}
		}
		result += "\n";
		result.replace(/\n/g, '<br />');
		return result;
	};

	//Attribute Pop-Up Box Helper Function
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
	};

	/*
		Button functions
	*/

	//Process Operators to Server (GUIJSON --> TEXTDBJSON --> Server)
	var processQuery = function(){
		var GUIJSON = $('#the-flowchart').flowchart('getData');

		var TEXTDBJSON = {};
		var operators = [];
		var links = [];

		var DUMMYJSON = {
			glossary: {
				title: 'example glossary',
				GlossDiv: {
					title: 'S',
					GlossList: {
						GlossEntry: {
							ID: 'SGML',
							SortAs: 'SGML',
							GlossTerm: 'Standard Generalized Markup Language',
							Acronym: 'SGML',
							Abbrev: 'ISO 8879:1986',
							GlossDef: {
								para: 'A meta-markup language, used to create markup languages such as DocBook.',
								GlossSeeAlso: ['GML', 'XML']
							},
							GlossSee: 'markup'
						}
					}
				}
			},
			glossary2: {
				title: 'example glossary',
				GlossDiv: {
					title: 'S',
					GlossList: {
						GlossEntry: {
							ID: 'SGML',
							SortAs: 'SGML',
							GlossTerm: 'Standard Generalized Markup Language',
							Acronym: 'SGML',
							Abbrev: 'ISO 8879:1986',
							GlossDef: {
								para: 'A meta-markup language, used to create markup languages such as DocBook.',
								GlossSeeAlso: ['GML', 'XML']
							},
							GlossSee: 'markup'
						}
					}
				}
			}
		}

		for(var operatorIndex in GUIJSON.operators){
			var currentOperator = GUIJSON['operators']
			if (currentOperator.hasOwnProperty(operatorIndex)){
				var attributes = {};
				for(var attribute in currentOperator[operatorIndex]['properties']['attributes']){
					if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)){
						attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
					}
				}
				operators.push(attributes);
			}
		}

		for(var link in GUIJSON.links){
			var destination = {};
			var currentLink = GUIJSON['links']
			if (currentLink[link].hasOwnProperty("fromOperator")){
				destination["from"] = currentLink[link]['fromOperator'];
				destination["to"] = currentLink[link]['toOperator'];
				links.push(destination);
			}
		}
		TEXTDBJSON.operators = operators;
		TEXTDBJSON.links = links;

		debug = TEXTDBJSON;
		console.log(JSON.stringify(debug));

		$.ajax({
			url: "http://localhost:8080/queryplan/execute",
			type: "POST",
			data: JSON.stringify(TEXTDBJSON),
			dataType: "text",
			contentType: "application/json",
			success: function(returnedData){
				console.log("SUCCESS\n");
				createResultFrame(JSON.parse(returnedData));
			},
			error: function(xhr, status, err){
				console.log(JSON.stringify(xhr));
				console.log(JSON.stringify(err));
				createResultFrame(DUMMYJSON);
			}
		});
	};

	//Attribute Pop-Up Box displays attributes in the popup box for selected operator
	var displayPopupBox = function(){
		selectedOperator = $('#the-flowchart').flowchart('getSelectedOperatorId');
		var output = data['operators'][selectedOperator]['properties']['attributes'];
		var title = data['operators'][selectedOperator]['properties']['title'];

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

		$('.attributes-band').html('Attributes for <em>' + title + '</em>');

		$('#switch').html('<i class="fa fa-minus-circle" aria-hidden="true"></i>Hide Attributes');
	};

	//Create Operator to send to flowchart.js and display on flowchart
	var createOperator = function(buttonPanel){
		var panel = $(buttonPanel).attr('rel');

		var userInput = $('#' + panel + ' .value').val();
		var extraOperators = getExtraOperators(userInput,panel);

		var userLimit = getAttr(panel, ' .limit');
		var userOffset = getAttr(panel, ' .offset');
		var userAttributes = getAttr(panel, ' .attributes');

		//
		var userInAttributes = getAttr(panel, ' .inner-attribute');
		var userOutAttributes = getAttr(panel, ' .outer-attribute');
		//

		var operatorName = $('#' + panel + ' button').attr('id');
		var operatorId = operatorName + '_' + operatorI;
		var operatorData = {
			top: 60,
			left: 500,
			properties: {
				title: (operatorName),
				inputs: {
					input_1: {
						label: 'Input (:i)',
					}
				},
				outputs: {
					output_1: {
						label: 'Output (:i)',
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
		if(operatorName == "Join"){
			operatorData.properties.inputs['input_2'] = {label: 'Input 2'};
			operatorData.properties.attributes['inner_attribute'] = userInAttributes;
			operatorData.properties.attributes['outer_attribute'] = userOutAttributes;
		} else {
			operatorData.properties.attributes['attributes'] = userAttributes;
		}
		// operatorData.properties.attributes['attributes'] = userAttributes;
		operatorData.properties.attributes['limit'] = userLimit;
		operatorData.properties.attributes['offset'] = userOffset;



		operatorI++;

		$('#the-flowchart').flowchart('createOperator', operatorId, operatorData);

		data = $('#the-flowchart').flowchart('getData');
	};

	//Edit Operator to send to flowchart.js and display on flowchart
	var editOperator = function(){
		editOperators = [];
		var output = data['operators'][selectedOperator]['properties']['attributes'];

		$('#attributes').text(getEditInputHtml(output));

		$('#attributes').html($('#attributes').text());

		var confirmChangesButton = $('<button class="confirm-button">Confirm Changes</button>');
        $('#attributes').append(confirmChangesButton);

		var cancelButton = $('<button class="cancel-button">Cancel</button>');
        $('#attributes').append(cancelButton);
	};

	//Confirms changes made to selected operator which are passed to flowchart.js and recreated.
	var confirmChanges = function(){
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
						label: 'Input (:i)',
					}
				},
				outputs: {
					output_1: {
						label: 'Output (:i)',
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
				result = DEFAULT_DICT;
			}
			if (result == 'null'){
				result = null;
			}
			operatorData.properties.attributes[attr] = result;
		}
		console.log(JSON.stringify(operatorData));

		if(operatorName == "Join"){
			operatorData.properties.inputs['input_2'] = {label: 'Input 2'};
		}
		$('#the-flowchart').flowchart('setOperatorData', operatorId, operatorData);

		$('#the-flowchart').flowchart('selectOperator', operatorId);
		selectedOperator = $('#the-flowchart').flowchart('getSelectedOperatorId');




		data = $('#the-flowchart').flowchart('getData');
		output = data['operators'][selectedOperator]['properties']['attributes'];

		var title = data['operators'][selectedOperator]['properties']['title'];
		$('.attributes-band').html('Attributes for <em>' + title + '</em>');

		$('#attributes').text(getPopupText(output));
		$('#attributes').html($('#attributes').text());

		var editButton = $('<button class="edit-operator">Edit</button>');
		$('#attributes').append(editButton);

		var deleteButton = $('<button class="delete-operator">Delete</button>');
		$('#attributes').append(deleteButton);
	};

	//Deletes the selected operator from flowchart.js and GUI
	var deleteOperator = function(){
		data = $('#the-flowchart').flowchart('getData');
		$('#attributes').css({
			'visibility': 'hidden'
		});

		$('.attributes-band').text('Attributes');

		$('#the-flowchart').flowchart('deleteSelected');
		data = $('#the-flowchart').flowchart('getData');

		$('#switch').html('<i class="fa fa-minus-circle" aria-hidden="true"></i>Show Attributes');
		closeBand();
	};

	/*
		Buttons
	*/

	//Process Operator Button. Calls processQuery function
	$('.process-query').on('click', processQuery);

	//Upon Operator Selection (when operator is clicked/selected) Calls displayPopupBox function
	$('#the-flowchart').on('click', '.flowchart-operators-layer.unselectable div .flowchart-operator-title', displayPopupBox);

	//Create Operator button. Calls createOperator function
	$('.create-operator').click(function() {
		createOperator(this);
	});

	//Edit Operator Button calls editOperator function
	$('#attributes').on('click', '.edit-operator', editOperator);

	//Confirm Changes Button. Calls confirmChanges function
	$('#attributes').on('click', '.confirm-button', confirmChanges);

	//Cancel Edit Button (as if selecting the operator again) calls displayPopupBox function
	$('#attributes').on('click', '.cancel-button', displayPopupBox);

	//Delete Operator Button. Calls deleteOperator function
	$('#attributes, .nav').on('click', '.delete-operator', deleteOperator);
};

$(document).ready(setup);
