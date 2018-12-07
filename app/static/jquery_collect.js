//TRAITS
//Set trait level undefined by default and only display traits when level selected
traits = $('#field_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits,#sample_traits');
$('#trait_selection').hide();
traits.hide();
$('#location').hide();
level_forms = $('#trees, #branches_leaves_samples');
level_forms.hide();

update_traits = function() {
	remove_flash();
	if (this.value==='') {
		$('#trait_selection').hide();
		traits.hide();
		$('#location').hide();
		level_forms.hide();
	}
	else if (this.value === 'field') {
		$('#location').show();
		$('#field, #block').hide();
		$('#trait_selection').show();
		$('#block_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#field_traits').show();
		level_forms.hide();
}
	else if (this.value === 'block') {
		$('#location').show();
		$('#field').show();
		$('#block').hide();
		$('#trait_selection').show();
		$('#field_traits, #tree_traits, #branch_traits, #leaf_traits, #sample_traits').hide();
		$('#block_traits').show();
		level_forms.hide();
	}
	else if (this.value === 'tree') {
		$('#location').show();
		$('#field, #block').show();
		$('#trait_selection').show();
		$('#field_traits,#block_traits,#branch_traits,#leaf_traits,#sample_traits').hide();
		$('#tree_traits').show();
		level_forms.show();
		$('#trees').show();
		$('#branches_leaves_samples').hide();
		update_replicates();
	}
	else if (this.value === 'branch') {
		$('#location').show();
		$('#field, #block').show();
		$('#trait_selection').show();
		$('#field_traits,#block_traits,#tree_traits,#leaf_traits,#sample_traits').hide();
		$('#branch_traits').show();
		level_forms.show();
		$('#trees').show();
		$('#branches_leaves_samples').show();
		$('#branches').show()
		$('#leaves, #samples').hide();
		update_replicates();
	}
	else if (this.value === 'leaf') {
		$('#location').show();
		$('#field, #block').show();
		$('#trait_selection').show();
		$('#field_traits,#block_traits,#tree_traits,#branch_traits,#sample_traits').hide();
		$('#leaf_traits').show();
		level_forms.show();
		$('#trees').show();
		$('#branches_leaves_samples').show();
		$('#leaves').show()
		$('#branches, #samples').hide();
		update_replicates();
	}
	else if (this.value === 'sample') {
		$('#location').show();
		$('#field, #block').show();
		$('#trait_selection').show();
		$('#field_traits,#block_traits,#tree_traits,#branch_traits,#leaf_traits').hide();
		$('#sample_traits').show();
		level_forms.show();
		$('#trees').show();
		$('#branches_leaves_samples').show();
		$('#samples').show()
		$('#branches, #leaves').hide();
		update_replicates();
	}
};

update_existing_ids = function() {
	remove_flash();
	var selection=$('#create_new_items').val()
	if (selection === 'existing') {
	     $('.new').hide();
	     $('.existing').show();
	}
	else if (selection === 'new') {
	     $('.new').show();
	     $('.existing').hide();
	     $('#email_checkbox').prop("checked", true);
	}
	//var trait_level=$('#trait_level').val();
	//if (trait_level === 'sample') {
	//	$('#samples_pooled').show();
	//	$('#per_sample').show();
	//}
	//else {
	//	$('#samples_pooled').hide();
    //    $('#per_sample').hide();
    //}
};


$('#trait_level').change(update_traits);
$('#create_new_items').change(update_existing_ids);

//update replicates div
update_replicates = function () {
    var level = $('#trait_level').val();
    var pooled = $('#samples_pooled').val();
    if (level === 'sample') {
        $('.samples').show();
        if (pooled === 'single') {
            $('.single').show();
            $('.pooled').hide();
            $('#block').prop('disabled', false);
            $('#trees_start').prop('disabled', false);
            $('#trees_end').prop('disabled', false);
        }
        else {
            $('.single').hide();
            $('.pooled').show();
            $('#block').prop('disabled', true);
            $('#trees_start').prop('disabled', true);
            $('#trees_end').prop('disabled', true);
        }
    }
    else {
    	$('.samples').hide();
    	$('.single').show();
    	$('.pooled').hide();
    	$('#block').prop('disabled', false);
    	$('#trees_start').prop('disabled', false);
    	$('#trees_end').prop('disabled', false);
	}
}

$('#samples_pooled').change(update_replicates);

update_replicates();
update_existing_ids();


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
//$('#select_all_field-general').parent().next().show();
//$('#select_all_block-general').parent().next().show();
//$('#select_all_tree-general').parent().next().show();
//$('#select_all_sample-general').parent().next().show();

//submit collect button
$("#submit_collect").click( function(e) {
	e.preventDefault();
	remove_flash();
	const wait_message = "Please wait for files to be generated"
	const flash_wait = "<div id='files_flash' class='flash'>" + wait_message + "</div>";
	$(this).parent().after(flash_wait);
	$.ajax({
	    url: "/collect/generate_files",
	    data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('submitted')) {
				const flash_submitted = "<div id='files_flash' class='flash'>" + response.submitted + "</div>";
				$("#files_flash").replaceWith(flash_submitted);
			} else {
				if (response.hasOwnProperty('errors')) {
                    const errors= response['errors'];
                    for (let i = 0; i < errors.length; i++) {
                        for (const key in errors[i]) {
                            if (errors[i].hasOwnProperty(key)) {
                                const flash = "<div id='flash_" + key + "' class='flash'>" + errors[i][key][0] + "</div>";
                                $('[id="' + key + '"').after(flash);
                            }
                        }
                    }
                }
			}
		}
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
