
remove_flash = function() {
	$(".flash").remove();
}

$('#upload_submit').click( function(e) {
	e.preventDefault();
	$('#upload_submit').prop('disabled', true);
	remove_flash();
	$('#error_table_div').empty()
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
				if (response.hasOwnProperty('task_id')) {
				    poll(response.task_id);
				}
				else {
					$('#upload_submit').prop('disabled', false);
				}
			} else {
				$("#upload_submit_flash").remove();
				$('#upload_submit').prop('disabled', false);
				for (var key in response){
					if (response.hasOwnProperty(key)) {
						flash = "<div id='flash_" + key + "' class='flash'>" + response[key][0] + "</div>";
						$('#' + key).after(flash);
					}
				}
			}
		},
		error: function(error) {
			$('#upload_submit').attr("enabled", "enabled")
		}
	});
	//button was hidden, reveal again.
	$("#upload_submit").show();
	//poll for result of submission
	function poll(task_id) {
		setTimeout( function () {
			$.ajax({
				type: 'GET',
				url: "/status/" + task_id +"/",
				success: function(response) {
					if (response.hasOwnProperty('status')) {
						//flash_status = "<div id='upload_submit_flash' class='flash'> " + response.status + "</div>";
						//$("#upload_submit_flash").replaceWith(flash_status);
						if (response.status === 'PENDING') { poll(task_id) };
						if (response.status === 'RETRY') {
							$('#upload_submit_flash').replaceWith("<div id='upload_submit_flash' class='flash'></div>");
							message = "<p>Your file will be processed as soon as the database becomes available</p>"
							$('#upload_submit_flash').append(message);
							$('#upload_submit').prop('disabled', false);
							poll(task_id);
						}
						if (response.status === 'ERRORS') {
							$('#upload_submit_flash').replaceWith("<div id='response' class='flash'></div>");
							$('#response').append("<p>Errors were found in the uploaded file:</p>");
							$('#response').append('<div>' + response.result + '</div>');
							$('#upload_submit').prop('disabled', false);
							};
					    if (response.status === 'SUCCESS') {
					    	$('#upload_submit_flash').replaceWith("<div id='response' class='flash'></div>")
					    	$('#response').append(response.result.result);
					    	$('#upload_submit').prop('disabled', false);
							load_graph("/json_submissions");
					    	};
					}
					//if (response.hasOwnProperty('result')) {
					//	if (typeof response.result['new_data'] !== "undefined") {
					//		result_text = String(response.result['new_data']) + " new records submitted, "
					//		result_text += String(response.result['resubmissions']) + " records already found"
					//		result_text += String()
					//		flash_result = "<div id='upload_submit_flash' class='flash'> " + result_text +  "</div>";
					//		$("#upload_submit_flash").replaceWith(flash_result);
					//		$('#upload_submit').prop('disabled', false);
					//	}
					//}
				}
			});
		}, 1000);
	}
});
