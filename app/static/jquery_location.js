//update the drop down boxes

update_countries = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/location/countries/",
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
	update_regions();
	update_farms();
	update_plots();
}

update_regions = function() {
	var sel_country = $("#country").find(":selected").val();
	if (sel_country === "") {
		$("#region").empty();
		$("#region").append($("<option></option>").attr("value", "").text("Select Region"))
	}
	else {
		$("#region").prop( "disabled", true);
		var request = $.ajax({
			type: 'GET',
			url: "/location/" + sel_country +'/',
		});
		request.done(function(data){
			var regions = [["","Select Region"]].concat(data).sort();
			$("#region").empty();
			for (var i = 0; i < regions.length; i++) {
				$("#region").append(
					$("<option></option>").attr(
						"value", regions[i][0]).text(regions[i][1])
				);
			$("#region").prop( "disabled", false);
			}
		});
	}
	update_farms();
	update_plots();
}

update_farms = function() {
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
		});
		request.done(function(data){
			var farms = [["","Select Farm"]].concat(data).sort();
			$("#farm").empty();
			for (var i = 0; i < farms.length; i++) {
				$("#farm").append(
					$("<option></option>").attr(
						"value", farms[i][0]).text(farms[i][1])
				);
			$("#farm").prop( "disabled", false);
			}
		});
	}
	update_plots();
};

update_plots = function() {
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
		});
		request.done(function(data){
			var plots = [["","Select Plot"]].concat(data).sort();
			$("#plot").empty();
			for (var i = 0; i < plots.length; i++) {
				$("#plot").append(
					$("<option></option>").attr(
						"value", plots[i][0]).text(plots[i][1])
				);
			$("#plot").prop( "disabled", false);
			}
		});
	}
};

update_blocks = function() {
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
		});
		request.done(function(data){
			var blocks = [["","Select Block"]].concat(data).sort();
			$("#block").empty();
			for (var i = 0; i < blocks.length; i++) {
				$("#block").append(
					$("<option></option>").attr(
						"value", blocks[i][0]).text(blocks[i][1])
				);
			$("#block").prop( "disabled", false);
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
	remove_flash();
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
	remove_flash();
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
	remove_flash();
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
})

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
            //lazily return starting ID as 1
            $("#trees_start").val(1)
            //but have looked up the following so replace
            $("#trees_end").val(data[0])
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