//LOCATION
//update blockx (not in location form as different rendering on different pages (list dropbox))
update_blocks = function() {
	var sel_plot = $("#plot").find(":selected").val();
	if (sel_plot !== "") {
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
			}
		});
	}
};

//hide boxes that aren't relevant
$('#country').change(update_blocks).change(function () {
	if (this.value === "") {
		$('#region').hide();
		$('#farm').hide();
		$('#plot').hide();
		$('#block').hide();
	} else {
		$('#region').show();
	}
});
$('#region').change(update_blocks).change(function () {
	if (this.value === "") {
		$('#farm').hide();
		$('#plot').hide();
		$('#block').hide();
	} else {
		$('#farm').show();
	}
});
$('#farm').change(update_blocks).change(function () {
	if (this.value === "") {
		$('#plot').hide();
		$('#block').hide();
	} else {
		$('#plot').show();
	}
});
$('#plot').change(update_blocks).change(function () {
	if (this.value === "") {
		$('#block').hide();
	} else {
		$('#block').show();
	}
});

//start hidden, only show when parent locale selected
$('#region').hide();
$('#farm').hide();
$('#plot').hide();
$('#block').hide();

//TRAITS
//remove submit traits buttons (these are loaded with the traits forms)
$('#submit_block_traits').remove();
$('#submit_tree_traits').remove();

//also load set trait level undefined by defauly and only display level traits when selected
$('#trait_level').val('0');
$('#treeTraits').hide();
$('#blockTraits').hide();

$('#trait_level').change(function () {
	$(".flash").remove();
	if (this.value === 'tree') {
		$('#treeTraits').show();
		$('#blockTraits').hide();
	}
	if (this.value === 'block') {
		$('#treeTraits').hide();
		$('#blockTraits').show();
	}
	if (this.value === '') {
		$('#treeTraits').hide();
		$('#blockTraits').hide();
	}
})

$('#data_format').change(function () {
	$(".flash").remove();
})

//Add select all checkboxes to the form
$('dl').each( function () {
	//get ID from ul element
	id = $(this).find('dd > ul').attr('id'); 
	//expand collapse trait details on click label
	$(this).find('dt > label').click(function () {
		$(this).parent().next().toggle();
	});
	//and start collapsed
	$(this).find('dd').hide()
	//add checkbox
	$(this).find('dt > label').before("<input id='select_all_" + id + "' type='checkbox'>");
	//on checkbox change toggle children true/false
	$('#select_all_' + id).change(function () {
		if (this.checked) { 
			$(this).parent().next().find("input").prop("checked", true);
		}
		else {
			$(this).parent().next().find("input").prop("checked", false);
		}
	});
});

//expand the general tab as defaults are set (and as example)
$('#select_all_general').parent().next().show();
$('#select_all_block_general').parent().next().show();

//DOWNLOAD
$('#submit_download').click(function(e) {
	e.preventDefault();
	$(".flash").remove();
	var sel_level = $("#trait_level").find(":selected").val();
	wait_message = "Please wait for file to be generated"
	flash_wait = "<div id='download_flash' class='flash'>" + wait_message + "</div>"
	$('form').append(flash_wait);
	if ($('#trait_level').find(":selected").val() === "") {
		select_level_message = "This field is required"
		$('#trait_level').after("<div id='level_flash' class='flash'>" + select_level_message + "</div>")
	}
	trait_count = $('#' + sel_level + 'Traits').find('input[type=checkbox]:checked').length - $('#' + sel_level + 'Traits').find('[id^=select_all_]:checked').length;	
	if (trait_count === 0) {
		select_message = "Please select traits to include in traits.trt"
		flash_select = "<div id='traits_flash' class='flash'>" + select_message + "</div>"
		$("#download_flash").replaceWith(flash_select)
	} else {
		var submit_traits = $.ajax({
			url: "/download/generate_csv",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				if (response.hasOwnProperty('submitted')) {
					flash_submitted = "<div id='download_flash' class='flash'>" + response.submitted + "</div>";
					$("#download_flash").replaceWith(flash_submitted);
				} else {
					$("#download_flash").remove();
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
			}
		});
	}
})

//Render a calendar in jquery-ui for date selection
$("#date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#date_to").datepicker({ dateFormat: 'yy-mm-dd'});
