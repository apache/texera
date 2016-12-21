var openMenu = function(){
	$('.menu').animate({
		'left': '0px'
	}, 200);
	
	$('.icon-menu').css({
		'visibility': 'hidden',
		'pointer': 'default'
	});
	
	$('.process-query').animate({
		'margin-right': '295px'
	}, 200);
		
	$('body').animate({
		'left': '285px'
	}, 200);
};

var closeMenu = function(){
	$('.menu').animate({
		'left': '-285px'
	}, 200);
	
	$('.process-query').animate({'margin-right': '10px'}, 200, function(){
		$('.icon-menu').css({'visibility': 'visible', 'pointer': 'pointer'});
	});
	
	$('body').animate({
		'left': '0px'
	}, 200);
};

var closeResultFrame = function(closeButton){
	$(closeButton).parent().parent().parent().remove();
};

var closeBand = function(){
	$('.popup').animate({
		'bottom': '-570px'
	}, 200);
};

var selectPanel = function(clickedPanel){
	var panelToShow = $(clickedPanel).attr('rel');
	var oldPanel = $('.panel.active').attr('id');
	
	$('li.active').removeClass('active');
	$('.panel.active').removeClass('active');
	
	if (oldPanel != panelToShow){
		$(clickedPanel).addClass('active');
		$('#' + panelToShow).addClass('active');
	}	
}

var main = function(){
		
	$('.icon-menu').click(openMenu);
	
	$('.icon-close').click(closeMenu);
	
	$('body').on('click', '.result-frame .result-box .result-box-band .result-frame-close', function() {
		closeResultFrame(this);
	});
	
	$('.attributes-band').on('click', closeBand);
	
	$('.menu ul li').on('click', function(){
		selectPanel(this);
	});
};

$(document).ready(main);