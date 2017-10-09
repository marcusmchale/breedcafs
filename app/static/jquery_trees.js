//update blockx (not in location form as different rendering on different pages (list dropbox))
update_blocks = function() {
	var sel_plot = $("#plot").find(":selected").val();
	if (sel_plot !== "") {
		var request = $.ajax({
			type: 'GET',
			url: "/location/blocks/" + sel_plot + '/',
		});
		request.done(function(data){
			var blocks = [["","Select Block"]].concat(data).sort();
			$("#block").empty();
			for (var i = 0; i < blocks.length; i++) {
				$("#block").append(
					$("<option></option>").attr(
						"value", blocks[i][0]).text(blocks[i][1])
				);
			}
		});
	}
};

$('#plot').change(update_blocks);
$('#farm').change(update_blocks);
$('#region').change(update_blocks);
$('#country').change(update_blocks);

//register new trees
$("#submit_trees").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for trees to be registered and files generated"
	flash_wait = "<div id='trees_flash' class='flash'>" + wait_message + "</div>"
	$("#submit_trees").after(flash_wait)
	var submit_trees = $.ajax({
		url: "/add_trees",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='trees_flash' class='flash'>" + response.submitted + "</div>";
				$("#trees_flash").replaceWith(flash_submitted);
			} else {
				$("#trees_flash").remove();
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
	submit_trees.done(load_chart);
})

//get custom trees.csv
$("#custom_trees_csv").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for file to be generated";
	flash_wait = "<div id='fields_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
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

