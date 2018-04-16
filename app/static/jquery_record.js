//TRAITS
//Set trait level undefined by default and only display traits when level selected
$('#trait_level').val('0');
traits = $('#plot_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits,#sample_traits');
traits.hide();
id_forms = $('#plot,#tree_ids,#branch_ids,#leaf_ids,#sample_ids');
id_forms.hide();
$('#existing_ids').hide();

$('#trait_level').change(function () {
	$(".flash").remove();
	if (this.value === '') {
		traits.hide();
		id_forms.hide();
	}
	else if (this.value === 'plot') {
		$('#block_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#plot_traits').show();
		$('#existing_ids').hide();
		$('#plot, #tree_ids, #sample_ids').hide();
	}
	else if (this.value === 'block') {
		$('#plot_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#block_traits').show();
		$('#plot').show();
		$('#existing_ids').hide();
		$('#tree_ids, #sample_ids').hide();
	}
	else if (this.value === 'tree') {
		$('#plot_traits,#block_traits,#branch_traits,#leaf_traits,#sample_traits').hide();
		$('#tree_traits').show();
		$('#plot, #tree_ids').show();
		$('#existing_ids').hide();
		$('#branch_ids,#leaf_ids,#sample_ids').hide();
	}
	else if (this.value === 'branch') {
		$('#plot_traits,#block_traits,#tree_traits,#leaf_traits,#sample_traits').hide();
		$('#branch_traits').show();
		$('#existing_ids').show();
		$('#old_new_ids').val('old');
		$('#new_ids').hide();
		$('#plot, #tree_ids, #branch_ids').show();
		$('#leaf_ids, #sample_ids').hide();
	}
	else if (this.value === 'leaf') {
		$('#plot_traits,#block_traits,#tree_traits,#branch_traits,#sample_traits').hide();
		$('#leaf_traits').show();
		$('#existing_ids').show();
		$('#old_new_ids').val('old');
		$('#new_ids').hide();
		$('#plot,#leaf_ids,#tree_ids').show();
		$('#branch_ids,#sample_ids').hide();
	}
	else if (this.value === 'sample') {
		$('#plot_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits').hide();
		$('#sample_traits').show();
		$('#existing_ids').show();
		$('#old_new_ids').val('old');
		$('#new_ids').hide();
		$('#plot, #sample_ids, #tree_ids').show();
		$('#branch_ids,#leaf_ids').hide();
	}
})

$('#old_new_ids').val('old');
$('#old_ids').show();

$('#old_new_ids').change(function () {
   $(".flash").remove;
   if (this.value === 'old') {
        $('#new_ids').hide();
        $('#old_ids').show();
    }
    if (this.value === 'new') {
        $('#new_ids').show();
        $('#old_ids').hide();
   }
})

//remove flash message on change of data format select box
$('#data_format').change(function () {
	$(".flash").remove();
})

//Add select all checkboxes to the form
$('dl').each( function () {
	//get ID from ul element
	id = $(this).find('dd > ul').attr('id');
	//make trait groups look clickable
	$(this).find('dt > label').mouseover(function () {
	    $(this).css('font-weight', 'bold');
	});
	//make trait groups look clickable
	$(this).find('dt > label').mouseout(function () {
	    $(this).css('font-weight', 'normal');
	});
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
//$('#select_all_plot-general').parent().next().show();
//$('#select_all_block-general').parent().next().show();
//$('#select_all_tree-general').parent().next().show();
//$('#select_all_sample-general').parent().next().show();

//submit collect button
$("#submit_record").click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for files to be generated"
	flash_wait = "<div id='files_flash' class='flash'>" + wait_message + "</div>";
	$(this).parent().after(flash_wait);
	var generate_files = $.ajax({
	    url: "/record/generate_files",
	    data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				flash_submitted = "<div id='files_flash' class='flash'>" + response.submitted + "</div>";
				$("#files_flash").replaceWith(flash_submitted);
			} else {
				$("#files_flash").remove();
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
    })
})



//button to reset form values
//$("#custom_samples_csv").after('<br><input id="reset_form_button" name="Reset Form" value="Clear Form" type="submit")><br>')

//$("#reset_form_button").click( function(e) {
//	e.preventDefault();
//	$("form").find('input:text').val('');
//	$("form").find('select').val('');
//	$(".flash").remove();
//	})
