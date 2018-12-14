//update the drop down boxes

update_countries = function(set_country="") {
	const country_select = $("#country");
	country_select.empty();
	country_select.append($("<option></option>").attr("value", "").text("Select Country"));
	$.ajax({
		type: 'GET',
		url: "/location/countries/",
		success: function(response) {
			const countries = response.sort();
			for (let i = 0; i < countries.length; i++) {
				country_select.append(
					$("<option></option>").attr(
						"value", countries[i][0]).text(countries[i][1])
				);
			}
			country_select.val(set_country);
			update_regions();
		},
		error: function(error) {
			console.log(error);
		}
	});
};

update_regions = function(set_region = "") {
	const sel_country = $("#country").find(":selected").val();
	const region_select = $("#region");
	region_select.empty();
	region_select.append($("<option></option>").attr("value", "").text("Select Region"));
	if (sel_country !== "") {
		region_select.prop( "disabled", true);
		$.ajax({
			type: 'GET',
			url: "/location/" + sel_country +'/',
			success: function(response) {
				const regions = response.sort();
				for (let i = 0; i < regions.length; i++) {
					region_select.append(
						$("<option></option>").attr(
							"value", regions[i][0]).text(regions[i][1])
					);
				}
				region_select.prop( "disabled", false);
				region_select.val(set_region);
				update_farms();
			},
			error: function(error) {
				console.log(error);
			}
		});
	} else {
		update_farms();
	}
};

update_farms = function(set_farm = "") {
	const sel_country=$("#country").find(":selected").val();
	const sel_region=$("#region").find(":selected").val();
	const farm_select=$("#farm");
	farm_select.empty();
	farm_select.append($("<option></option>").attr("value", "").text("Select Farm"));
	if (sel_region !== "") {
		farm_select.prop( "disabled", true);
		$.ajax({
			type: 'GET',
			url: "/location/" + sel_country +'/' + sel_region + '/',
			success: function(response){
				const farms = response.sort();
				for (let i = 0; i < farms.length; i++) {
					farm_select.append(
						$("<option></option>").attr(
							"value", farms[i][0]).text(farms[i][1])
					);
				}
				farm_select.prop( "disabled", false);
				farm_select.val(set_farm);
				update_fields();
			},
			error: function (error) {
				console.log(error);
			}
		});
	} else {
		update_fields();
	}

};

update_fields = function(set_field = "") {
	const sel_country = $("#country").find(":selected").val();
	const sel_region = $("#region").find(":selected").val();
	const sel_farm = $("#farm").find(":selected").val();
	const field_select = $("#field");
	field_select.empty();
	field_select.append($("<option></option>").attr("value", "").text("Select Field"));
	if (sel_farm !== "") {
		field_select.prop( "disabled", true);
		$.ajax({
			type: 'GET',
			url: "/location/" + sel_country + '/' + sel_region + '/' + sel_farm + '/',
			success: function(response){
				const fields = response.sort();
				for (let i = 0; i < fields.length; i++) {
					field_select.append(
						$("<option></option>").attr(
							"value", fields[i][0]).text(fields[i][1])
					);
				}
				field_select.prop( "disabled", false);
				field_select.val(set_field);
				update_blocks();
			},
			error: function (error) {
				console.log(error);
			}
		});
	} else {
		update_blocks();
	}
};

update_blocks = function(set_block = "") {
	const sel_field = $("#field").find(":selected").val();
	const block_select = $("#block");
	block_select.empty();
	block_select.append($("<option></option>").attr("value", "").text("Select Block"));
	if (sel_field !== "")  {
		block_select.prop( "disabled", true);
		$.ajax({
			type: 'GET',
			url: "/location/blocks/" + sel_field + '/',
			success: function(response){
				const blocks = response.sort();
				for (let i = 0; i < blocks.length; i++) {
					block_select.append(
						$("<option></option>").attr(
							"value", blocks[i][0]).text(blocks[i][1])
					);
				}
				block_select.prop( "disabled", false);
				block_select.val(set_block);
			},
			error: function (error) {
				console.log(error);
			}
		});
	}
};


update_item_count = function() {
	const sel_level = $("#level").find(":selected").val();
	if (sel_level && sel_level !== "") {
		const item_count_text = $('#item_count_div a:eq(0)');
		const item_type_text = $('#item_count_div a:eq(1)');
        const sel_country = $("#country").find(":selected").val();
        const sel_region = $("#region").find(":selected").val();
        const sel_farm = $("#farm").find(":selected").val();
        const sel_field = $("#field").find(":selected").val();
        const sel_block = $("#block").find(":selected").val();
        const tree_id_list = $("#tree_id_list").val();
        $.ajax({
            type: "GET",
            url: (
                "/item_count"
                + "?level=" + sel_level
                + "&country=" + sel_country
                + "&region=" + sel_region
                + "&farm=" + sel_farm
                + "&field_uid=" + sel_field
                + "&block_uid=" + sel_block
				+ "&tree_id_list=" + tree_id_list
            ),
            success: function (response) {
            	if (isNaN(response['item_count'])) {
            		item_count_text.replaceWith(
                        "<a>" + response['item_count'] + "</a>"
                    );
            		 item_type_text.replaceWith(
                        "<a></a>"
                    );
                } else {
                    item_count_text.replaceWith(
                        "<a>" + response['item_count'] + "</a>"
                    );
                    const item_type_text_entry = (response['item_count'] === 1)
                        ? sel_level.toString() : sel_level.toString() + 's';
                    item_type_text.replaceWith(
                        "<a>" + item_type_text_entry + "</a>"
                    );
                }
            },
            error: function (error) {
            	item_count_text.replaceWith(
            		"<a>" + error.statusText + "</a>"
				);
            	item_type_text.replaceWith(
					"<a></a>"
				);
            }
        });
    }
};


//Submit locations
$("#submit_country").click( function(e) {
	e.preventDefault();
	remove_flash();
	$.ajax({
			url: "/add_country",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					const flash = "<div id='country_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_country").after(flash);
					$("#text_country").val("");
					update_countries(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					const flash = "<div id='country_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_country").after(flash);
					update_countries(response['found'].toLowerCase());
					$("#text_country").val("");
				} else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
							for (const key in response[i]){
								if (response[i].hasOwnProperty(key)) {
									const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
									$('#' + key).after(flash);
								}
							}
						}
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
});

$("#submit_region").click( function(e) {
	e.preventDefault();
	remove_flash();
	$.ajax({
			url: "/add_region",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					const flash = "<div id='region_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_region").after(flash);
					$("#text_region").val("");
					update_regions(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					const flash = "<div id='region_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_region").after(flash);
					update_regions(response['found'].toLowerCase());
					$("#text_region").val("");
				} else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
                            for (const key in response[i]) {
                                if (response[i].hasOwnProperty(key)) {
                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                                    $('#' + key).after(flash);
                                }
                            }
                        }
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
});

$("#submit_farm").click( function(e) {
	e.preventDefault();
	remove_flash();
	$.ajax({
			url: "/add_farm",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					const flash = "<div id='farm_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
					$("#submit_farm").after(flash);
					$("#text_farm").val("");
					update_farms(response['submitted'].toLowerCase());
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					const flash = "<div id='farm_flash' class='flash'> Found: " + response['found'] + " </div>";
					$("#submit_farm").after(flash);
					update_farms(response['found'].toLowerCase());
					$("#text_farm").val("");
				} else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
                            for (const key in response[i]) {
                                if (response[i].hasOwnProperty(key)) {
                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                                    $('#' + key).after(flash);
                                }
                            }
                        }
					}
				}
			},
			error: function(error) {
				console.log(error);
			}
		});
});


$("#submit_field").click( function(e) {
	e.preventDefault();
	remove_flash();
	$.ajax({
		url: "/add_field",
		data: $("form").serialize(),
		type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					const flash = "<div id='field_flash' class='flash'> Submitted: " + response['submitted']['name'] + " </div>";
					$("#submit_field").after(flash);
					$("#text_field").val("");
					update_fields(response['submitted']['uid']);
					load_chart();
				}  else if (response.hasOwnProperty('found')) {
					const flash = "<div id='field_flash' class='flash'> Found: " + response['found']['name'] + " </div>";
					$("#submit_field").after(flash);
					update_fields(response['found']['uid']);
					$("#text_field").val("");
				} else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
                            for (const key in response[i]) {
                                if (response[i].hasOwnProperty(key)) {
                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                                    $('#' + key).after(flash);
                                }
                            }
                        }
					}
				}
			},
		error: function(error) {
			console.log(error);
		}
	});
});

//Add new block to the field
$("#submit_block").click( function(e) {
	e.preventDefault();
	remove_flash();
	$.ajax({
		url: "/add_block",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
					const flash = "<div id='block_flash' class='flash'> Submitted: " + response['submitted']['name'] + " </div>";
					$("#submit_block").after(flash);
					$("#text_block").val("");
					update_blocks(response['submitted']['uid']);
					load_chart();
			}  else if (response.hasOwnProperty('found')) {
					const flash = "<div id='block_flash' class='flash'> Found: " + response['found']['name'] + " </div>";
					$("#submit_block").after(flash);
					update_blocks(response['found']['uid']);
					$("#text_block").val("");
			}  else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
							for (const key in response[i]){
								if (response[i].hasOwnProperty(key)) {
									const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
									$('#' + key).after().after(flash);
								}
							}
						}
					}
			}
		},
		error: function (error) {
			console.log(error);
		}
	});
});

//register new trees
$("#submit_trees").click( function(e) {
	e.preventDefault();
	remove_flash();
	const wait_message = "Please wait for trees to be registered and files generated";
	const flash_wait = "<div id='trees_flash' class='flash'>" + wait_message + "</div>";
	$(this).parent().after(flash_wait);
	$.ajax({
		url: "/add_trees",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				const flash_submitted = "<div id='trees_flash' class='flash'>" + response.submitted + "</div>";
				$("#trees_flash").replaceWith(flash_submitted);
				load_chart();
			}  else {
					for (const i in response) {
						if (response.hasOwnProperty(i)) {
							for (const key in response[i]){
								if (response[i].hasOwnProperty(key)) {
									const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
									$('#' + key).after(flash);
								}
							}
						}
					}
				}
		},
		error: function(error) {
			console.log(error);
		}
	});
});

remove_flash = function() {
	$(".flash").remove();
};


//Disable submit on keypress "Enter" for all inputs boxes
$("input").keypress( function(e) {
	if (e.keyCode === 13) {
		e.preventDefault();
		$(".flash").remove();
	}
});


$('#tree_id_list').keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});

$( window ).load(update_countries).load(update_item_count);
$("#country").change(update_regions).change(update_farms).change(update_fields).change(update_blocks).change(update_item_count).change(remove_flash);
$("#region").change(update_farms).change(update_fields).change(update_blocks).change(update_item_count).change(remove_flash);
$("#farm").change(update_fields).change(update_blocks).change(update_item_count).change(remove_flash);
$('#field').change(update_blocks).change(update_item_count).change(remove_flash);

$('#level, #block').change(update_item_count);
