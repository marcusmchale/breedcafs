update_soil_types = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/location_fields/soil_types/",
	});
	request.done(function(data){
		var soil_types = [["","Select Soil Type"]].concat(data).sort();
		$("#soil").empty();
		for (var i = 0; i < soil_types.length; i++) {
			$("#soil").append(
				$("<option></option>").attr(
					"value", soil_types[i][0]).text(soil_types[i][1])
			);
		}
	});
}

update_shade_trees = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/location_fields/shade_trees/",
	});
	request.done(function(data){
		var shade_trees = [].concat(data).sort();
		//("#shade_trees").empty();
		for (var i = 0; i < shade_trees.length; i++) {
			$("#shade_tree").append(
				$("<option></option>").attr(
					"value", shade_trees[i][0]).text(shade_trees[i][1])
			);
		}
	});
}

$( window ).load(update_soil_types).load(update_shade_trees)

//Submit soil types
$("#submit_soil").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_soil = $.ajax({
			url: "/add_soil",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='soil_flash' class='flash'> " + response + " </div>"
				$("#submit_soil").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_soil.done(update_soil_types);
})

//Submit shade trees
$("#submit_shade_tree").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_shade_tree = $.ajax({
			url: "/add_shade_tree",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='shade_tree_flash' class='flash'> " + response + " </div>"
				$("#submit_shade_tree").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_shade_tree.done(update_shade_trees);
	$("#shade_trees").load(location.href+" #shade_trees>*");
})


//register field details
$("#submit_field_details").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	wait_message = "Please wait for field details to be registered"
	flash_wait = "<div id='fields_flash' class='flash'>" + wait_message + "</div>"
	$("#submit_field_details").after(flash_wait)
	var submit_field_details = $.ajax({
		url: "/add_field_details",
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
	submit_field_details.done(load_chart);
})

