//TRAITS
//Set trait level undefined by default and only display traits when level selected
$('#trait_level').val('0');
traits = $('#trial_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits,#sample_traits');
traits.hide();
id_forms = $('#trial,#tree_ids,#branch_ids,#leaf_ids,#sample_ids');
id_forms.hide();
$('#existing_ids').hide();

$('#trait_level').change(function () {
	$(".flash").remove();
	if (this.value === '') {
		traits.hide();
		id_forms.hide();
	}
	else if (this.value === 'trial') {
		$('#block_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#trial_traits').show();
		$('#existing_ids').hide();
		$('#trial, #tree_ids, #sample_ids').hide();
	}
	else if (this.value === 'block') {
		$('#trial_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#block_traits').show();
		$('#trial').show();
		$('#existing_ids').hide();
		$('#tree_ids, #sample_ids').hide();
	}
	else if (this.value === 'tree') {
		$('#trial_traits,#block_traits,#branch_traits,#leaf_traits,#sample_traits').hide();
		$('#tree_traits').show();
		$('#trial, #tree_ids').show();
		$('#existing_ids').hide();
		$('#branch_ids,#leaf_ids,#sample_ids').hide();
	}
	else if (this.value === 'branch') {
		$('#trial_traits,#block_traits,#tree_traits,#leaf_traits,#sample_traits').hide();
		$('#branch_traits').show();
		$('#existing_ids').show();
		$('#old_new_ids').val('old');
		$('#new_ids').hide();
		$('#trial, #tree_ids, #branch_ids').show();
		$('#leaf_ids, #sample_ids').hide();
	}
	else if (this.value === 'leaf') {
		$('#trial_traits,#block_traits,#tree_traits,#branch_traits,#sample_traits').hide();
		$('#leaf_traits').show();
		$('#existing_ids').show();
		$('#old_new_ids').val('old');
		$('#new_ids').hide();
		$('#trial,#leaf_ids,#tree_ids').show();
		$('#branch_ids,#sample_ids').hide();
	}
	else if (this.value === 'sample') {
		$('#trial_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits').hide();
		$('#sample_traits').show();
		$('#existing_ids').hide();
		$('#trial, #sample_ids, #tree_ids').show();
		$('#branch_ids,#leaf_ids').hide();
	}
});

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
        $('#email_checkbox').prop("checked", true);
   }
});

//remove flash message on change of data format select box
$('#data_format').change(function () {
	$(".flash").remove();
});

checkbox_formatting = function () {
	if ($(this).closest("dl").find("li > input:checked").length === $(this).closest("dl").find("li > input").length) {
		$(this).closest("dl").find('dt > label').css('text-decoration', 'none');
		$(this).closest("dl").find('dt > input').prop("checked", true);
	}
	else if ($(this).closest("dl").find("li > input:checked").length > 0) {
		$(this).closest("dl").find('dt > label').css('text-decoration', 'underline');
		$(this).closest("dl").find('dt > input').prop("checked", false);
	}
	else {
		$(this).closest("dl").find('dt > label').css('text-decoration', 'none');
		$(this).closest("dl").find('dt > input').prop("checked", false);
	}
};

$( window ).load(function () {
	$('dl').each(checkbox_formatting);
})

//Add select all checkboxes to the form
$('dl').each( function () {
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
	//get trait ul id
	trait_id = $(this).find('dd > ul').attr('id');
	//add checkbox
	$(this).find('dt > label').before("<input id='select_all_" + trait_id + "' type='checkbox'>");
	//on checkbox change toggle children true/false
	$('[id="select_all_' + trait_id + '"]').change(function () {
		$(this).parent().find('label').css('text-decoration', 'none');
		if (this.checked) {
			$(this).parent().next().find("input").prop("checked", true);
		}
		else {
			$(this).parent().next().find("input").prop("checked", false);
		}
	});
	//make trait groups underlined if any items checked within
	$(this).find('li > input').change(checkbox_formatting);
});

//expand the general tab as defaults are set (and as example)
//$('#select_all_trial-general').parent().next().show();
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
