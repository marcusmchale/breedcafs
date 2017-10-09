//update blockx (not in location form as different rendering on different pages (list dropbox))
update_blocks = function () {
	var sel_plotID = $("#plot").find(":selected").val();
	if (sel_plotID !== "") {
		var request = $.ajax({
			type: 'GET',
			url: "/location/blocks/" + sel_plotID +"/",
		});
		request.done(function(data){
			$('#blocks_list_div').remove();
			$("#submit_plot").after("<div id=blocks_list_div><a>Blocks in selected plot:</a><ul id=blocks_list></ul></div>");
			$("#blocks_list").empty();
			var blocks = [].concat(data).sort();
			for (var i=0; i < blocks.length; i++) {
				$("#blocks_list").append('<li>' + blocks[i][1] + '</li>')
			}			
		})
	}
}

$('#plot').change(update_blocks);
$('#farm').change(update_blocks);
$('#region').change(update_blocks);
$('#country').change(update_blocks);

//generate blocks.csv
$("#generate_blocks_csv").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for file to be generated";
	flash_wait = "<div id='generate_blocks_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var generate_blocks_csv = $.ajax({
		url: "/generate_blocks_csv",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='generate_blocks_flash' class='flash'>" + response.submitted + "</div>";
				$("#generate_blocks_flash").replaceWith(flash_submitted);
			} else {
				$("#generate_blocks_flash").remove();
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

//Add new treatment block to the plot
$("#submit_block").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for block to be submitted";
	flash_wait = "<div id='submit_block_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var submit_block = $.ajax({
		url: "/add_block",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			console.log(response);
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='submit_block_flash' class='flash'>" + response.submitted + "</div>";
				$("#submit_block_flash").replaceWith(flash_submitted);
				update_blocks();
			} else {
				$("#submit_block_flash").remove();
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
		error: function (error) {
			console.log(error);
		}
	});
})
