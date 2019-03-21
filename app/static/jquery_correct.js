correct_submit = $('#correct_submit');

remove_flash = function() {
	$(".flash").remove();
};

correct_submit.click( function(e) {
	e.preventDefault();
	correct_submit.prop('disabled', true);
	remove_flash();
	const wait_message = "Please wait for records to be submitted";
	const flash_wait = "<div id='correct_submit_flash' class='flash'>" + wait_message + "</div>"
	correct_submit.after(flash_wait);
	const correct_submit_flash = $('#correct_submit_flash');
	correct_submit.hide();
	$.ajax({
		url: "/correct_submit",
		data: new FormData($('form')[0]),
		type: 'POST',
		processData: false,
		contentType: false,
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				correct_submit_flash.html(response.submitted);
				if (response.hasOwnProperty('task_id')) {
				    poll(response.task_id);
				}
				else {
					correct_submit.prop('disabled', false);
				}
			} else {
				correct_submit_flash.remove();
				correct_submit.prop('disabled', false);
				for (var key in response){
					if (response.hasOwnProperty(key)) {
						const flash = "<div id='flash_" + key + "' class='flash'>" + response[key][0] + "</div>";
						$('#' + key).after(flash);
					}
				}
			}
		},
		error: function(error) {
			console.log(error);
			correct_submit.attr("enabled", "enabled")
		}
	})
	//button was hidden, reveal again.
	correct_submit.show();
	//poll for result of submission
	function poll(task_id) {
		setTimeout( function () {
			$.ajax({
				type: 'GET',
				url: "/status/" + task_id +"/",
				success: function(response) {
					console.log(response);
					if (response.hasOwnProperty('status')) {
						//flash_status = "<div id='upload_submit_flash' class='flash'> " + response.status + "</div>";
						//$("#upload_submit_flash").replaceWith(flash_status);
						if (response.status === 'PENDING') {
							poll(task_id)
						}
						if (response.status === 'RETRY') {
							correct_submit_flash.html(
							   "<p>Your file will be processed as soon as the database becomes available</p>"
                            );
							correct_submit.prop('disabled', false);
							poll(task_id);
						}
						if (response.status === 'FAILURE') {
							correct_submit_flash.css({
                                    'background': '#f0b7e1'
                                });
							correct_submit_flash.html(
							   "<p>Something unexpected happened. " +
								"Please try again but if the issue persists then please contact an administrator</p>"
                            );
							correct_submit.prop('disabled', false);
						}
						if (response.status === 'ERRORS') {
							correct_submit_flash.css({
                                    'background': '#f0b7e1'
                                });
							correct_submit_flash.html("<p>Errors were found in the uploaded file:</p>");
							correct_submit_flash.append('<div>' + response.result + '</div>');
							correct_submit.prop('disabled', false);
							}
					    if (response.status === 'SUCCESS') {
					    	correct_submit_flash.html(response.result.result);
					    	correct_submit.prop('disabled', false);
					    	}
					}
				}
			});
		}, 1000);
	}
});
