

//get custom trees.csv
$("#custom_trees_csv").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for file to be generated";
	flash_wait = "<div id='fields_flash' class='flash'>" + wait_message + "</div>";
	$(this).parent().after(flash_wait)
	var custom_trees_csv = $.ajax({
		url: "/custom_trees_csv",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='fields_flash' class='flash'>" + response.submitted + "</div>";
				$("#fields_flash").replaceWith(flash_submitted);
			} else {
				$("#fields_flash").remove();
				for (var key in response[0]){
					if (response[0].hasOwnProperty(key)) {
						flash = "<div id='flash_" + key + "' class='flash'>" + response[0][key][0] + "</div>";
						$('#' + key).after(flash);
					}
				}
				//this response is an array from two forms so need two of these (alternatively could iterate over these...)
				for (var key in response[1]){
					if (response[1].hasOwnProperty(key)) {
						flash = "<div id='flash_" + key + "' class='flash'>" + response[1][key][0] + "</div>";
						$('#' + key).after(flash);
					}
				}
			}
		},
		error: function(error) {
			console.log(error);
		}
	});
})

