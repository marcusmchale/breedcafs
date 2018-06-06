remove_flash = function() {
	$(".flash").remove();
}

$('#submission_type').val('FB');

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
				if (response.hasOwnProperty('task_id')) {
				    $('svg').empty();
				    poll(response.task_id);
				}
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
						flash_status = "<div id='upload_submit_flash' class='flash'> " + response.status + "</div>";
						$("#upload_submit_flash").replaceWith(flash_status);
						if (response.status === 'PENDING') { poll(task_id) };
					    if (response.status === 'SUCCESS') { load_graph() };
					}
					if (response.hasOwnProperty('result')) {
						if (typeof response.result['new_data'] !== "undefined") {
							result_text = String(response.result['new_data']) + " new records submitted, "
							result_text += String(response.result['resubmissions']) + " records already found"
							result_text += String()
							flash_result = "<div id='upload_submit_flash' class='flash'> " + result_text +  "</div>";
							$("#upload_submit_flash").replaceWith(flash_result);
						}
					}
				}
			});
		}, 1000);
	}
});

//Render a calendar in jquery-ui for date selection
$("#date").datepicker({ dateFormat: 'yy-mm-dd'});
$('#time').timepicker({ dateFormat: 'hh-mm'});