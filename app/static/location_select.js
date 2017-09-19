//update the drop down boxes

update_countries = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/location_trees/countries/",
	});
	request.done(function(data){
		var countries = [["","Select Country"]].concat(data).sort();
		$("#country").empty();
		for (var i = 0; i < countries.length; i++) {
			$("#country").append(
				$("<option></option>").attr(
					"value", countries[i][0]).text(countries[i][1])
			);
		}
	});
}

update_regions = function() {
	var sel_country= $("#country").find(":selected").val();
	if (sel_country !== "") {
		var request = $.ajax({
			type: 'GET',
			url: "/location_trees/" + sel_country +'/',
		});
		request.done(function(data){
			var regions = [["","Select Region"]].concat(data).sort();
			$("#region").empty();
			for (var i = 0; i < regions.length; i++) {
				$("#region").append(
					$("<option></option>").attr(
						"value", regions[i][0]).text(regions[i][1])
				);
			}
		});
	}
}

update_farms = function() {
	var sel_country= $("#country").find(":selected").val();
	var sel_region= $("#region").find(":selected").val();
	if (sel_country !== "" && sel_region !== "" ) {
		var request = $.ajax({
			type: 'GET',
			url: "/location_trees/" + sel_country +'/' + sel_region + '/',
		});
		request.done(function(data){
			var farms = [["","Select Farm"]].concat(data).sort();
			$("#farm").empty();
			for (var i = 0; i < farms.length; i++) {
				$("#farm").append(
					$("<option></option>").attr(
						"value", farms[i][0]).text(farms[i][1])
				);
			}
		});
	}
};

update_plots = function() {
	var sel_country = $("#country").find(":selected").val();
	var sel_region = $("#region").find(":selected").val();
	var sel_farm = $("#farm").find(":selected").val();
	if (sel_country !== "" && sel_region !== "" && sel_farm !== "") {
		var request = $.ajax({
			type: 'GET',
			url: "/location_trees/" + sel_country + '/' + sel_region + '/' + sel_farm + '/',
		});
		request.done(function(data){
			var plots = [["","Select Plot"]].concat(data).sort();
			$("#plot").empty();
			for (var i = 0; i < plots.length; i++) {
				$("#plot").append(
					$("<option></option>").attr(
						"value", plots[i][0]).text(plots[i][1])
				);
			}
		});
	}
};

$( window ).load(update_countries).load(update_regions).load(update_farms).load(update_plots)
$("#country").change(update_regions).change(update_farms).change(update_plots);
$("#region").change(update_farms).change(update_plots);
$("#farm").change(update_plots);

//Disable submit on keypress "Enter" for all inputs boxes
$("input").keypress( function(e) {
	if (e.keyCode == 13) {
		e.preventDefault();	
	}
})

//Submit locations
$("#submit_country").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_country = $.ajax({
			url: "/add_country",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='country_flash' class='flash'> " + response + " </div>"
				$("#submit_country").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_country.done(update_countries).done(load_chart);
})

$("#submit_region").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_region = $.ajax({
			url: "/add_region",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='region_flash' class='flash'> " + response + " </div>"
				$("#submit_region").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_region.done(update_regions).done(load_chart);
})

$("#submit_farm").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_farm = $.ajax({
			url: "/add_farm",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='farm_flash' class='flash'> " + response + " </div>"
				$("#submit_farm").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_farm.done(update_farms).done(load_chart);
})


$("#submit_plot").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_plot = $.ajax({
		url: "/add_plot",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			flash = "<div id='plot_flash' class='flash'> " + response + " </div>"
			$("#submit_plot").after(flash)
		},
		error: function(error) {
			console.log(error);
		}
	});
	submit_plot.done(update_plots).done(load_chart);
})

//register new trees
$("#submit_trees").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
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

//get custom fields.csv
$("#submit_fields").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	wait_message = "Please wait for file to be generated";
	flash_wait = "<div id='fields_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var submit_fields = $.ajax({
		url: "/custom_fields",
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

