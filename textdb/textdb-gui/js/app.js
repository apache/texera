var openMenu = function(){
	$('.menu').animate({
		'left': '0px'
	}, 200);

	$('.icon-menu').css({
		'visibility': 'hidden',
		'pointer': 'default'
	});

	$('.Wiki').animate({
		'margin-right': '285px'
	}, 200);

	$('.attribute-show').animate({
		'margin-left': '0px',
	}, 200);

	$('body').animate({
		'left': '285px'
	}, 200);



};

var closeMenu = function(){

	$('.menu').animate({
		'left': '-285px'
	}, 200);

	$('.Wiki').animate({'margin-right': '10px'}, 200, function(){
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

var openBand = function(){
	$('.popup').animate({
		'bottom': '10px'
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

var beginhover = function(){
	$(this).addClass('activating');
}
var endhover = function(){
	$(this).removeClass('activating');
}

var beginpanelhover = function(){
	$(this).addClass('activate-panel');
}

var endpanelhover = function(){
	$(this).removeClass('activate-panel');
}

var main = function(){


	// $('.icon-menu').click(openMenu);
	$('.process-query').hover(beginhover,endhover);
	$('.delete-operator').hover(beginhover,endhover);
	$('.icon-close').hover(beginhover, endhover);

	$('li').hover(beginpanelhover,endpanelhover);

	$('.attribute-show').hover(beginhover,endhover);
	$('.attribute-hide').hover(beginhover,endhover);
	$('.Wiki').hover(beginhover,endhover);

	$('.create-operator').hover(beginpanelhover,endpanelhover);


	// $('.icon-close').click(closeMenu);

	$("#switch").click(function () {
		if (($("#switch").text() == 'Show Attributes')){
			openBand();
			$('#switch').html('<i class="fa fa-minus-circle" aria-hidden="true"></i>Hide Attributes');
			;
		}else{
			closeBand();
			$('#switch').html('<i class="fa fa-plus-circle"></i>Show Attributes');
		}
})

	$('body').on('click', '.result-frame .result-box .result-box-band .result-frame-close', function() {
		closeResultFrame(this);
	});


	$('.attributes-band').on('click', function(){
		closeBand();
		$('#switch').html('<i class="fa fa-plus-circle"></i>Show Attributes');
	});

	$('.menu ul li').on('click', function(){
		selectPanel(this);
	});
};



$(document).ready(main);
