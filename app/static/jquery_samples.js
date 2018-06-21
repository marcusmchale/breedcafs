update_tissues = function(set_tissue = "") {
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
		$('#tissue').val(set_tissue);
	});
}

update_storage = function(set_storage = "") {
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
		$('#storage').val(set_storage)
	});
}

$( window ).load(update_tissues).load(update_storage)

$('#email_checkbox').prop("checked", true);

//Submit tissue
$("#submit_tissue").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_tissue = $.ajax({
			url: "/add_tissue",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
			    if (response.hasOwnProperty('submitted')) {
			        flash = "<div id='tissue_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
				    $("#submit_tissue").after(flash);
				    $("#text_tissue").val("");
				    update_tissues(response['submitted'].toLowerCase());
				    load_chart();
			    }  else if (response.hasOwnProperty('found')) {
			        flash = flash = "<div id='tissue_flash' class='flash'> Found: " + response['found'] + " </div>";
			        $("#submit_tissue").after(flash);
			        update_tissues(response['found'].toLowerCase());
				    $("#text_tissue").val("");
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


$("#submit_storage").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_storage = $.ajax({
			url: "/add_storage",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
			    if (response.hasOwnProperty('submitted')) {
			        flash = "<div id='storage_flash' class='flash'> Submitted: " + response['submitted'] + " </div>";
				    $("#submit_storage").after(flash);
				    $("#text_storage").val("");
				    update_storage(response['submitted'].toLowerCase());
				    load_chart();
			    }  else if (response.hasOwnProperty('found')) {
			        flash = flash = "<div id='storage_flash' class='flash'> Found: " + response['found'] + " </div>";
			        $("#submit_storage").after(flash);
			        update_storage(response['found'].toLowerCase());
				    $("#text_storage").val("");
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

//register new samples
$("#submit_samples").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	wait_message = "Please wait for samples to be registered and files generated"
	flash_wait = "<div id='samples_flash' class='flash'>" + wait_message + "</div>"
	$(this).parent().after(flash_wait);
	var submit_samples = $.ajax({
		url: "/add_samples",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='samples_flash' class='flash'> " + response.submitted + " </div>";
				$("#samples_flash").replaceWith(flash_submitted);
			}  else {
				$("#samples_flash").remove();
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
		}
	});
})

//Render a calendar in jquery-ui for date selection
$("#date_collected").datepicker({ dateFormat: 'yy-mm-dd'});
$("#date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#date_to").datepicker({ dateFormat: 'yy-mm-dd'});

