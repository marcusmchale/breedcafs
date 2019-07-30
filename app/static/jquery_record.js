const item_level_select = $('#item_level');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const sample_div = $('#sample_selection_div');
const item_count_div = $('#item_count_div');
const input_group_select = $('#input_group');
const input_variable_checkbox_div = $('#input_variable_checkbox_div');
const dynamic_form_div = $('#dynamic_form_div');
const location_div = $('#location');
const input_variable_div = $('#input_variable_selection');
const generate_template_button = $('#generate_template');
const generate_template_div = $('#generate_template_div');
const submit_records_button = $('#submit_records');
const replicates_div = $('#replicates_div');

$("#record_start").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_end").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_time").datepicker({ dateFormat: 'yy-mm-dd'});

block_div.hide();
tree_div.hide();
sample_div.hide();

remove_flash = function() {
	$(".flash").remove();
};

// need to reimplement with scan of selected input group for record types
    //if (record_type === "trait") {
    //    $('#template_format').append('<option value="fb">Field Book (csv)</option>');
    //}
    //else {
    //    $('#template_format option[value="fb"]').remove();
    //}




//        if (['trait', 'property', 'condition'].includes(record_type)){
//                 $('#web_form_div').show();
//         }


//    const record_period_div = $('#record_period_div');
//     const record_time_div = $('#record_time_div');
//  if (record_type === 'trait') {
//             record_period_div.hide();
//             record_time_div.show();
//         } else if (record_type === 'condition') {
//             record_period_div.show();
//             record_time_div.hide();
//         } else {
//             record_period_div.hide();
//             record_time_div.hide();
//         }



level_update = function() {
    const item_level = item_level_select.val();
    input_group_select.empty();
    input_group_select.append(
        '<option value="">Select group</option>'
    );
    location_div.show();
    item_count_div.show();
    if (item_level === "field") {
        block_div.hide();
        tree_div.hide();
        sample_div.hide();
    }
    else if (item_level === "block") {
        block_div.show();
        tree_div.hide();
        sample_div.hide();
    }
    else if (item_level === 'tree') {
        block_div.show();
        tree_div.show();
        sample_div.hide();
    }
    else if (item_level === 'sample') {
        block_div.show();
        tree_div.show();
        sample_div.show();
    }
    $.ajax({
        url: (
            "/record/input_groups"
            + "?item_level=" + item_level
            + "&username=True"
        ),
        type: 'GET',
        success: function (response) {
            const input_groups = response;
            for (let i = 0; i < input_groups.length; i++) {
                input_group_select.append(
                    $("<option></option>").attr(
                        "value", input_groups[i][0]).text(input_groups[i][1])
                );
            }
        },
        error: function (error) {
            console.log(error);
        }
    });
    group_update();
    update_item_count();
    remove_flash();
};


group_update = function() {
    input_variable_checkbox_div.empty();
    dynamic_form_div.empty();
    const item_level = item_level_select.val();
    const input_group = input_group_select.val();
    if (item_level && input_group) {
        $.ajax({
            url: (
                "/record/inputs"
                + "?item_level=" + item_level
                + "&input_group=" + input_group
                + "&username=True"
            ),
            type: 'GET',
            success: function (response) {
                input_variable_checkbox_div.append(
                    "<br><input id=select_all_input_variables type=checkbox>" +
                    "<label>Select all</label>" +
                    "<ul id='select_inputs'></ul>"
                );
                generate_form(response);
                $('#select_all_input_variables').change(function () {
                    if (this.checked) {
                        input_variable_checkbox_div.find(":checkbox:not(#select_all_input_variables)").each(function () {
                            this.checked = true;
                        }).trigger('change');
                    } else {
                        input_variable_checkbox_div.find(":checkbox:not(#select_all_input_variables)").each(function () {
                            this.checked = false;
                        }).trigger('change');
                    }
                });
            },
            error: function (error) {
                console.log(error);
            }
        });
    }
    update_submit_fields();
};


generate_form = function (response) {
    dynamic_form_div.append('<dl></dl>');
    for (let i = 0; i < response.length; i++) {
//        if (['text', 'numeric', 'percent'].includes(response[i]['format'])) {
//            dynamic_form_div.find('dl').append(
//                '<dt>' + response[i]['name'] + '</dt>' +
//                '<dd><input type="text" ' +
//                'id="' + response[i]['name_lower'] + '" ' +
//                'name="' + response[i]['name_lower'] + '" ' +
//                'placeholder="' + response[i]['details'] + '" ' +
//                'title="' + response[i]['details'] + '" ' +
//                '</dd>'
//            )
//        } else if (response[i]['format'] === 'boolean') {
//            dynamic_form_div.find('dl').append(
//                '<dt>' + response[i]['name'] + '</dt>' +
//                '<dd><select ' +
//                'id="' + response[i]['name_lower'] + '" ' +
//                'name="' + response[i]['name_lower'] + '" ' +
//                'title="' + response[i]['details'] + '"> ' +
//                '<option value = "true">True</option>' +
//                '<option value = "false">False</option>' +
//                '</select></dd>'
//            )
//        } else if (response[i]['format'] === 'date') {
//            dynamic_form_div.find('dl').append(
//                '<dt>' + response[i]['name'] + '</dt>' +
//                '<dd><input type="text" ' +
//                'id="' + response[i]['name_lower'] + '" ' +
//                'name="' + response[i]['name_lower'] + '" ' +
//                'placeholder="' + response[i]['details'] + '" ' +
//                'title="' + response[i]['details'] + '" ' +
//                '</dd>'
//            );
//            $('[id="' + response[i]["name_lower"] + '"]').datepicker({dateFormat: 'yy-mm-dd'});
//        } else if (response[i]['format'] === 'categorical') {
//            let category_options = "";
//            const category_list = response[i]['category_list'];
//            for (let j = 0; j < category_list.length; j++) {
//                const category = category_list[j];
//                category_options += (
//                    '<option value = "' + category + '">' +
//                    category + '</option>'
//                )
//            }
//            dynamic_form_div.find('dl').append(
//                '<dt>' + response[i]['name'] + '</dt>' +
//                '<dd><select ' +
//                'id="' + response[i]['name_lower'] + '" ' +
//                'name="' + response[i]['name_lower'] + '" ' +
//                'title="' + response[i]['details'] + '"> ' +
//                category_options + '</select></dd>'
//            );
//        } else if (response[i]['format'] === 'multicat') {
//            let category_options = "";
//            const category_list = response[i]['category_list'];
//            for (let j = 0; j < category_list.length; j++) {
//                const category = category_list[j];
//                category_options += (
//                    '<option value = "' + category.toLowerCase() + '">' +
//                    category + '</option>'
//                )
//            }
//            dynamic_form_div.find('dl').append(
//                '<dt>' + response[i]['name'] + '</dt>' +
//                '<dd><select ' +
//                'id="' + response[i]['name_lower'] + '" ' +
//                'name="' + response[i]['name_lower'] + '" ' +
//                'title="' + response[i]['details'] + '"> ' +
//                category_options + '</select></dd>'
//            );
//        }
        input_variable_checkbox_div.find('ul').append(
            "<li>" +
            "<input id=select_inputs-" + i + " " +
            "name='select_inputs' " +
            "type=checkbox value='" + response[i]['name_lower'] + "' " +
            ">" +
            "<label for='checkbox_" + response[i]['name_lower'] + "'>" +
            response[i]['name'] +
            "</label>" +
            "</li>"
        );
//        const form_field = $('[id="' + response[i]['name_lower'] + '"]').parent('dd');
//        form_field.hide();
//        form_field.prev().hide();
        $('#select_inputs-' + i).change(function () {
            update_submit_fields();
//            if (this.checked) {
//                 form_field.show();
//                 form_field.prev().show();
//            } else {
//                 form_field.hide();
//                 form_field.prev().hide();
//            }
        });
    }
    suppress_input();
};

update_submit_fields = function () {
    const checkboxes = input_variable_checkbox_div.find(":checkbox:not(#select_all_input_variables)");
    const count_checkboxes = checkboxes.length;
    const count_checked = checkboxes.filter(":checked").length;
    if (count_checked > 0){
        submit_records_button.show();
        generate_template_div.show();
        if (count_checked === count_checkboxes) {
            $('#select_all_input_variables').prop('checked', true);
        } else {
            $('#select_all_input_variables').prop('checked', false);
        }
    } else {
        $('#select_all_input_variables').prop('checked', false);
        $('#web_form_div').hide();
        submit_records_button.hide();
        generate_template_div.hide();
    }
};


update_item_count = function() {
	const sel_level = $("#item_level").find(":selected").val();
	if (sel_level && sel_level !== "") {
		const item_count_text = $('#item_count_div a:eq(0)');
		const item_type_text = $('#item_count_div a:eq(1)');
        const sel_country = $("#country").find(":selected").val();
        const sel_region = $("#region").find(":selected").val();
        const sel_farm = $("#farm").find(":selected").val();
        const sel_field = $("#field").find(":selected").val();
        const sel_block = $("#block").find(":selected").val();
        const tree_id_list = $("#tree_id_list").val();
        const sample_id_list = $("#sample_id_list").val();
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
                + "&sample_id_list=" + sample_id_list
            ),
            success: function (response) {
            	if (isNaN(response['item_count']) || response['item_count'] === 0) {
            	    input_variable_div.hide();
                    item_count_text.replaceWith(
                        "<a>None</a>"
                    );
                    item_type_text.replaceWith(
                        "<a></a>"
                    );
                } else {
            	    input_variable_div.show();
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

generate_template_button.click( function (e) {
    e.preventDefault();
    update_item_count();
    remove_flash();
    const wait_message = "Please wait for template to be generated";
    const flash_wait = "<div id='records_flash' class='flash'>" + wait_message + "</div>";
    generate_template_button.after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: "/record/generate_template",
        data: data,
        type: 'POST',
        success: function(response) {
            if (response.hasOwnProperty('submitted')) {
                const flash_submitted = "<div id='records_flash' class='flash'>" + response.submitted + "</div>";
                $("#records_flash").replaceWith(flash_submitted);
            } else {
                $("#records_flash").remove();
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
        },
        error: function(error) {
            console.log(error);
            const error_message = error.status === 500 ? 'An error has occurred. Please try again':
                'An unknown error as occurred, please contact an administrator';
            const flash_error = "<div id='records_flash' class='flash'>" + error_message + "</div>";
            $("#records_flash").replaceWith(flash_error);
		}
    })
});


submit_records_button.click( function (e) {
    e.preventDefault();
    update_item_count();
    remove_flash();
    const wait_message = "Please wait for submission to complete";
    const flash_wait = "<div id='records_flash' class='flash'>" + wait_message + "</div>";
    $(this).after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: "/record/submit_records",
        data: data,
        type: 'POST',
        success: function(response) {
            if (response.hasOwnProperty('submitted')) {
                if (response.hasOwnProperty('class') && response.class === 'conflicts') {
                    const flash_submitted = "<div id='records_flash' class='flash' style='background:#f0b7e1'>" + response.submitted + "</div>";
                    $("#records_flash").replaceWith(flash_submitted);
                } else {
                    const flash_submitted = "<div id='records_flash' class='flash'>" + response.submitted + "</div>";
                    $("#records_flash").replaceWith(flash_submitted);
                }
            } else {
                $("#records_flash").remove();
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
        },
        error: function(error) {
            const error_message = error.status === 500 ? 'An error has occurred. Please try again':
                'An unknown error as occurred, please contact an administrator';
            const flash_error = "<div id='records_flash' class='flash'>" + error_message + "</div>";
            $("#records_flash").replaceWith(flash_error);
		}
    })
});

suppress_input = function () {
    $(':input').keypress(function (e) {
        if (e.keyCode === 13 || e.keyCode === 10) {
            e.preventDefault();
            update_item_count();
        }
    });
};


$(window).on('load', level_update);
//$( window ).load(update_item_count);

item_level_select.change(level_update);
input_group_select.change(group_update);

$("#country, #region, #farm, #field, #block, #level").change(update_item_count);