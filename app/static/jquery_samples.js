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

$( window ).load(update_tissues).load(update_storage)

//Submit tissue
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
	$("form").append(flash_wait)
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
