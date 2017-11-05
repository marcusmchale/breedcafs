remove_flash = function() {
	$(".flash").remove();
}

//generate traits.csv
$('#upload_submit').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for data to be submitted"
	flash_wait = "<div id='upload_submit_flash' class='flash'>" + wait_message + "</div>"
	$("#upload_submit").after(flash_wait);
	$("#upload_submit").hide();
	var submit = $.ajax({
		url: "/upload_submit",
		data: new FormData($('form')[0]),
		type: 'POST',
		processData: false,
		contentType: false,
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='upload_submit_flash' class='flash'> " + response.submitted + " </div>";
				$("#upload_submit_flash").replaceWith(flash_submitted);
				$('svg').empty();
				load_graph();
			} else {
				$("#upload_submit_flash").remove();
				for (var key in response){
					if (response.hasOwnProperty(key)) {
						flash = "<div id='flash_" + key + "' class='flash'>" + response[key][0] + "</div>";
						$('#' + key).after(flash);
					}
				}
			}
		},
		error: function(error) {
		}
	})
	$("#upload_submit").show();
})
