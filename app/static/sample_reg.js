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


update_tissues = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/sample_reg/tissues/",
	});
	request.done(function(data){
		var tissues = [["","Select Tissue"]].concat(data).sort();
		$("#tissue").empty();
		for (var i = 0; i < tissues.length; i++) {
			$("#tissue").append(
				$("<option></option>").attr(
					"value", tissues[i][0]).text(tissues[i][1])
			);
		}
	});
}

update_storage = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/sample_reg/storage_methods/",
	});
	request.done(function(data){
		var storage_methods = [["","Select Storage"]].concat(data).sort();
		$("#storage").empty();
		for (var i = 0; i < storage_methods.length; i++) {
			$("#storage").append(
				$("<option></option>").attr(
					"value", storage_methods[i][0]).text(storage_methods[i][1])
			);
		}
	});
}

$( window ).load(update_countries).load(update_regions).load(update_farms).load(update_plots).load(update_tissues).load(update_storage)
$("#country").change(update_regions).change(update_farms).change(update_plots);
$("#region").change(update_farms).change(update_plots);
$("#farm").change(update_plots);

//Disable submit on keypress "Enter" for all inputs boxes
$("input").keypress( function(e) {
	if (e.keyCode == 13) {
		e.preventDefault();	
	}
})

$("#submit_tissue").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_tissue = $.ajax({
			url: "/add_tissue",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='tissue_flash' class='flash'> " + response + " </div>"
				$("#submit_tissue").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_tissue.done(update_tissues);
})


$("#submit_storage").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_storage = $.ajax({
			url: "/add_storage",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='storage_flash' class='flash'> " + response + " </div>"
				$("#submit_storage").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_storage.done(update_storage);
})

//register new samples
$("#submit_samples").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	wait_message = "Please wait for samples to be registered and files generated"
	flash_wait = "<div id='samples_flash' class='flash'>" + wait_message + "</div>"
	$("#submit_samples").after(flash_wait)
	var submit_samples = $.ajax({
		url: "/add_samples",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='samples_flash' class='flash'> " + response.submitted + " </div>";
				$("#samples_flash").replaceWith(flash_submitted);
			} else {
				$("#samples_flash").remove();
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
})

//Render a calendar in jquery-ui for date selection
$("#date_collected").datepicker({ dateFormat: 'yy-mm-dd'});
