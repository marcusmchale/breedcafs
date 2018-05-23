//update the drop down boxes

update_countries = function(set_country = "") {
	var request = $.ajax({
		type: 'GET',
		url: "/location/countries/",
		success: function(response) {
			var countries = [["","Select Country"]].concat(response).sort();
			$("#country").empty();
			for (var i = 0; i < countries.length; i++) {
				$("#country").append(
					$("<option></option>").attr(
						"value", countries[i][0]).text(countries[i][1])
				);
			$('#country').val(set_country);
			}
			update_regions();
		},
		error: function(error) {
			console.log(error);
		}
	});
}

update_regions = function(set_region = "") {
	$("#region").empty();
	var sel_country = $("#country").find(":selected").val();
	if (sel_country === "") {
		$("#region").append($("<option></option>").attr("value", "").text("Select Region"))
	}
	else {
		$("#region").prop( "disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/" + sel_country +'/',
			success: function(response) {
				var regions = [["","Select Region"]].concat(response).sort();
				for (var i = 0; i < regions.length; i++) {
					$("#region").append(
						$("<option></option>").attr(
							"value", regions[i][0]).text(regions[i][1])
					);
				$("#region").prop( "disabled", false);
				}
				$('#region').val(set_region);
				update_farms();
			},
			error: function(error) {
				console.log(error);
			}
		});
	}
}

update_farms = function(set_farm = "") {
	var sel_country= $("#country").find(":selected").val();
	var sel_region= $("#region").find(":selected").val();
	if (sel_region === "") {
		$("#farm").empty();
		$("#farm").append($("<option></option>").attr("value", "").text("Select Farm"))
	}
	else {
		$("#farm").prop( "disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/" + sel_country +'/' + sel_region + '/',
			success: function(response){
				var farms = [["","Select Farm"]].concat(response).sort();
				$("#farm").empty();
				for (var i = 0; i < farms.length; i++) {
					$("#farm").append(
						$("<option></option>").attr(
							"value", farms[i][0]).text(farms[i][1])
					);
				$("#farm").prop( "disabled", false);
				}
				$('#farm').val(set_farm);
				update_plots();
			},
			error: function (error) {
				console.log(error);
			}
		});
	}
};

update_plots = function(set_plot = "") {
	var sel_country = $("#country").find(":selected").val();
	var sel_region = $("#region").find(":selected").val();
	var sel_farm = $("#farm").find(":selected").val();
	if (sel_farm === "") {
		$("#plot").empty();
		$("#plot").append($("<option></option>").attr("value", "").text("Select Plot"))
	}
	else {
		$("#plot").prop( "disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/" + sel_country + '/' + sel_region + '/' + sel_farm + '/',
			success: function(response){
				var plots = [["","Select Plot"]].concat(response).sort();
				$("#plot").empty();
				for (var i = 0; i < plots.length; i++) {
					$("#plot").append(
						$("<option></option>").attr(
							"value", plots[i][0]).text(plots[i][1])
					);
				$("#plot").prop( "disabled", false);
				}
				$('#plot').val(set_plot);
				update_blocks();
			},
			error: function (error) {
				console.log(error);
			}
		});
	}
};

update_blocks = function(set_block = "") {
	var sel_plot = $("#plot").find(":selected").val();
	if (sel_plot === "")  {
		$("#block").empty();
		$("#block").append($("<option></option>").attr("value", "").text("Select Block"))
	}
	else {
		$("#block").prop( "disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/blocks/" + sel_plot + '/',
			success: function(response){
				var blocks = [["","Select Block"]].concat(response).sort();
				$("#block").empty();
				for (var i = 0; i < blocks.length; i++) {
					$("#block").append(
						$("<option></option>").attr(
							"value", blocks[i][0]).text(blocks[i][1])
					);
				$("#block").prop( "disabled", false);
				}
				$('#block').val(set_block);
			},
			error: function (error) {
				console.log(error);
			}
		});
	}
};


remove_flash = function() {
	$(".flash").remove();
}

$( window ).load(update_countries)
$("#country").change(update_regions).change(remove_flash);
$("#region").change(update_farms).change(remove_flash);
$("#farm").change(update_plots).change(remove_flash);
$('#plot').change(update_blocks).change(remove_flash);

//Disable submit on keypress "Enter" for all inputs boxes
$("input").keypress( function(e) {
	if (e.keyCode == 13) {
		e.preventDefault();	
		$(".flash").remove();
	}
})

//Submit locations
$("#submit_country").click( function(e) {
	e.preventDefault();
	remove_flash();
	var submit_country = $.ajax({
			url: "/add_country",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					flash = "<div id='country_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_country").after(flash);
					$("#text_country").val("");
					update_countries(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					flash = flash = "<div id='country_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_country").after(flash);
					update_countries(response['found'].toLowerCase());
					$("#text_country").val("");
				} else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after(flash);
							}
						}
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
})

$("#submit_region").click( function(e) {
	e.preventDefault();
	remove_flash();
	var submit_region = $.ajax({
			url: "/add_region",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					flash = "<div id='region_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_region").after(flash);
					$("#text_region").val("");
					update_regions(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					flash = flash = "<div id='region_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_region").after(flash);
					update_regions(response['found'].toLowerCase());
					$("#text_region").val("");
				} else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after(flash);
							}
						}
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
})

$("#submit_farm").click( function(e) {
	e.preventDefault();
	remove_flash();
	var submit_farm = $.ajax({
			url: "/add_farm",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					flash = "<div id='farm_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_farm").after(flash);
					$("#text_farm").val("");
					update_farms(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					flash = flash = "<div id='farm_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_farm").after(flash);
					update_farms(response['found'].toLowerCase());
					$("#text_farm").val("");
				} else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after(flash);
							}
						}
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
})


$("#submit_plot").click( function(e) {
	e.preventDefault();
	remove_flash();
	var submit_plot = $.ajax({
		url: "/add_plot",
		data: $("form").serialize(),
		type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					flash = "<div id='plot_flash' class='flash'> Submitted: " + response['submitted']['name'] + " </div>";
					$("#submit_plot").after(flash);
					$("#text_plot").val("");
					update_plots(response['submitted']['uid']);
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					flash = flash = "<div id='plot_flash' class='flash'> Found: " + response['found']['name'] + " </div>";
					$("#submit_plot").after(flash);
					update_plots(response['found']['uid']);
					$("#text_plot").val("");
				} else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after(flash);
							}
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
	var submit_block = $.ajax({
		url: "/add_block",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
					flash = "<div id='block_flash' class='flash'> Submitted: " + response['submitted']['name'] + " </div>";
					$("#submit_block").after(flash);
					$("#text_block").val("");
					update_blocks(response['submitted']['uid']);
					load_chart;
			}  else if (response.hasOwnProperty('found')) {
					flash = flash = "<div id='block_flash' class='flash'> Found: " + response['found']['name'] + " </div>";
					$("#submit_block").after(flash);
					update_blocks(response['found']['uid']);
					$("#text_block").val("");
			}  else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after().after(flash);
							}
						}
					}
			}
		},
		error: function (error) {
			console.log(error);
		}
	});
})

//register new trees
$("#submit_trees").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for trees to be registered and files generated"
	flash_wait = "<div id='trees_flash' class='flash'>" + wait_message + "</div>"
	$(this).parent().after(flash_wait)
	var submit_trees = $.ajax({
		url: "/add_trees",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='trees_flash' class='flash'>" + response.submitted + "</div>";
				$("#trees_flash").replaceWith(flash_submitted);
				load_chart();
			}  else {
					for (i in response) {
						for (var key in response[i]){
							if (response[i].hasOwnProperty(key)) {
								flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
								$('#' + key).after(flash);
							}
						}
					}
				}
		},
		error: function(error) {
			console.log(error);
		}
	});
})

//autocomplete the start and end tree
//This is for the collect and record forms
$("#trees_end").val('')

update_defaults = function() {
	var sel_plot = $("#plot").find(":selected").val();
	if (sel_plot !== "") {
		$("#trees_start,trees_end").prop("disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/treecount/" + sel_plot + '/',
		});
		request.done(function(data){
			if (data[0] === 0) {
				$("#trees_end").val('')
				$("#trees_start").val('')
			}
			else if (data[0] >= 0) {
				$("#trees_end").val(data[0])
				//lazily return starting ID as 1
				$("#trees_start").val(1)
			}
			//and be sure to allow the user to edit
			$("#trees_start,#trees_end").prop( "disabled", false);
		});
	}
	else {
		//handling return to nothing entered on deselection of plot
		$("#trees_start,#trees_end").val('')
	}
};

$('#plot').change(update_defaults);