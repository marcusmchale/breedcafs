//Add select all checkboxes to the form
$('dl').each( function () {
	//get ID from ul element
	id = $(this).find('dd > ul').attr('id'); 
	//expand collapse trait details on click label
	$(this).find('dt > label').click(function () {
		$(this).parent().next().toggle();
	});
	//and start collapsed
	$(this).find('dd').hide()
	//add checkbox
	$(this).find('dt > label').before("<input id='select_all_" + id + "' type='checkbox'>");
	//on checkbox change toggle children true/false
	$('#select_all_' + id).change(function () {
		if (this.checked) { 
			$(this).parent().next().find("input").prop("checked", true);
		}
		else {
			$(this).parent().next().find("input").prop("checked", false);
		}
	})
})

//expand the general tab as defaults are set (and as example)
$('#select_all_general').parent().next().show();

//generate traits.csv
$('#submit_traits').click( function(e) {
	e.preventDefault();
	level = String($("form").attr('action').split('/').slice(-2,-1));
	$(".flash").remove();
	wait_message = "Please wait for file to be generated"
	flash_wait = "<div id='traits_flash' class='flash'>" + wait_message + "</div>"
	$("form").append(flash_wait)
	trait_count = $("form input[type=checkbox]:checked").length - $('[id^=select_all_]:checked').length;
	if ($("#email_checkbox").prop('checked') == true) {trait_count = trait_count -1};
	if (trait_count > 64) {
		$("#traits_flash").after("<div id='traits_flash' \
			class='flash'>Caution: Field Book can only export up to 64 traits at a time\
			- consider selecting less (you have currently selected\
			" + trait_count + ")</div>")
	};
	if (trait_count === 0) {
		select_message = "Please select traits to include in traits.trt"
		flash_select = "<div id='traits_flash' class='flash'>" + select_message + "</div>"
		$("#traits_flash").replaceWith(flash_select)
	} else {
		console.log($("form").serialize())
		var submit_traits = $.ajax({
			url: "/traits/" + level + "/create_trt",
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
	}
})