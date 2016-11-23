var main = function(){
		
    $('.icon-menu').click(function(){
        $('.menu').animate({
            'left': '0px'
        }, 200);
        
		// $('#delete-operator').animate({
			// 'padding-right': '310px'
		// }, 200);
		
		$('.icon-menu').css({
			'visibility': 'hidden',
			'pointer': 'default'
		});
		
		$('.process-query').animate({
            'margin-right': '295px'
        }, 200);
		
		// $('.popup').animate({
			// 'padding-right': '285px'
		// }, 200);
		
        $('body').animate({
            'left': '285px'
        }, 200);
    });
	
    $('.icon-close').click(function(){
        $('.menu').animate({
            'left': '-285px'
        }, 200);
        
		$('.process-query').animate({'margin-right': '10px'}, 200, function(){
			$('.icon-menu').css({'visibility': 'visible', 'pointer': 'pointer'});
		});
		
		// $('.process-query').animate({
            // 'left': '0px'
        // }, 200);
		
		// $('.popup').animate({
			// 'padding-right': '0px'
		// }, 200);
		
        $('body').animate({
            'left': '0px'
        }, 200);
    });
	
	$('.band').on('click', function() {
		$('.popup').animate({
            'bottom': '-570px'
        }, 200);
	});
	
	$('.menu ul li').on('click', function() {
		
		var panelToShow = $(this).attr('rel');
		var oldPanel = $('.panel.active').attr('id');
		
		$('li.active').removeClass('active');
		$('.panel.active').removeClass('active');
		
		if (oldPanel != panelToShow){
			$(this).addClass('active');
			$('#' + panelToShow).addClass('active');
		}		
	});
};

$(document).ready(main);
