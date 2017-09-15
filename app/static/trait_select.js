$('ul').each( function () {
	$(this).before("<input id='select_all_" + $(this).attr('id') + "' type='checkbox'>");
})

$('ul').prev().change(function () {
	if (this.checked) { 
		$(this).next().find("input").prop("checked", true);
	}
	else {
		$(this).next().find("input").prop("checked", false);
	}
})

$('#submit_traits').click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	wait_message = "Please wait for file to be generated"
	flash_wait = "<div id='traits_flash' class='flash'>" + wait_message + "</div>"
	$("#submit_traits").after(flash_wait)
	trait_count = $("form input[type=checkbox]:checked").length;
	if (trait_count > 64) {
		console.log(trait_count);
		$("#traits_flash").after("<div id='traits_flash' \
			class='flash'>Caution: Field Book can only export up to 64 traits at a time\
			- consider selecting less (you have currently selected\
			" + trait_count + ")</div>")
	};
	var submit_traits = $.ajax({
		url: "/create_trt",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.submitted) {
				flash = "<div id='traits_flash' class='flash'> " + response.submitted + " </div>"		
			} else {
				flash = "<div id='traits_flash' class='flash'>Please select traits to include</div>"
			}
			$("#traits_flash").replaceWith(flash);
		},
		error: function(error) {
			console.log(error);
		}
	});
})