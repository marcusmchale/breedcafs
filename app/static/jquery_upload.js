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
				task_id = response.task_id;
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
	function poll() {
		setTimeout( function () {
			$.ajax({
				type: 'GET',
				url: "/status/" + task_id +"/",
				success: function(response) { //check if available
					if (response.hasOwnProperty('status')) {
						flash_status = "<div id='upload_submit_flash' class='flash'> " + response.status + "</div>";
						$("#upload_submit_flash").replaceWith(flash_status);
					}
					if (response.hasOwnProperty('result')) {
						if (typeof response.result !== "undefined") {
							result_text = String(response.result[0]) + " new records submitted, " + String(response.result[1]) + " records already found"
							flash_result = "<div id='upload_submit_flash' class='flash'> " + result_text +  "</div>";
							$("#upload_submit_flash").replaceWith(flash_result);
						}
					}
					if (response.status !== 'SUCCESS') { poll() };
					if (response.status === 'SUCCESS') { load_graph() };
				}
			});
		}, 1000);
	}
	submit.done(poll())
})