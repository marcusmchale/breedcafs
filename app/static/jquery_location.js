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
		$(".flash").remove();
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
