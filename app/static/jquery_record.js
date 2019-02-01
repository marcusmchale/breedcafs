const data_type_select = $('#data_type');
const level_select = $('#level');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const sample_div = $('#sample_selection_div');
const item_count_div = $('#item_count_div');
const group_select = $('#feature_group');
const feature_checkbox_div = $('#feature_checkbox_div');
const dynamic_form_div = $('#dynamic_form_div');
const location_div = $('#location');
const feature_div = $('#feature_selection');

$("#record_start").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_end").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_time").datepicker({ dateFormat: 'yy-mm-dd'});

block_div.hide();
tree_div.hide();
sample_div.hide();

remove_flash = function() {
	$(".flash").remove();
};

data_type_update = function() {
    const data_type = data_type_select.val();
    const level = level_select.val();
    if (data_type === "" || level === "") {
        location_div.hide();
        block_div.hide();
        tree_div.hide();
        sample_div.hide();
        item_count_div.hide();
        feature_div.hide();
    }
    level_update();
};


level_update = function() {
    const data_type = data_type_select.val();
    const level = level_select.val();
    group_select.empty();
    group_select.append(
        '<option value="">Select group</option>'
    );
    if (level === "" || data_type === "") {
        location_div.hide();
        block_div.hide();
        tree_div.hide();
        sample_div.hide();
        item_count_div.hide();
    } else {
        location_div.show();
        item_count_div.show();
        if (level === "field") {
            block_div.hide();
            tree_div.hide();
            sample_div.hide();
        }
        else if (level === "block") {
            block_div.show();
            tree_div.hide();
            sample_div.hide();
        }
        else if (level === 'tree') {
            block_div.show();
            tree_div.show();
            sample_div.hide();
        }
        else if (level === 'sample') {
            block_div.show();
            tree_div.show();
            sample_div.show();
        }
        $.ajax({
            url: "/record/" + data_type + "/" + level + "/",
            type: 'GET',
            success: function (response) {
                const feature_groups = response.sort();
                for (let i = 0; i < feature_groups.length; i++) {
                    group_select.append(
                        $("<option></option>").attr(
                            "value", feature_groups[i][0]).text(feature_groups[i][1])
                    );
                }
            },
            error: function (error) {
                console.log(error);
            }
        });
    }
    group_update();
    update_item_count();
    remove_flash();
};

generate_form = function (response) {
    dynamic_form_div.append('<dl></dl>');
    for (let i = 0; i < response.length; i++) {
        if (['text', 'numeric', 'percent'].includes(response[i]['format'])) {
            dynamic_form_div.find('dl').append(
                '<dt>' + response[i]['name'] + '</dt>' +
                '<dd><input type="text" ' +
                'id="' + response[i]['name_lower'] + '" ' +
                'name="' + response[i]['name_lower'] + '" ' +
                'placeholder="' + response[i]['details'] + '" ' +
                'title="' + response[i]['details'] + '" ' +
                '</dd>'
            )
        } else if (response[i]['format'] === 'boolean') {
            dynamic_form_div.find('dl').append(
                '<dt>' + response[i]['name'] + '</dt>' +
                '<dd><select ' +
                'id="' + response[i]['name_lower'] + '" ' +
                'name="' + response[i]['name_lower'] + '" ' +
                'title="' + response[i]['details'] + '"> ' +
                '<option value = "true">True</option>' +
                '<option value = "false">False</option>' +
                '</select></dd>'
            )
        } else if (response[i]['format'] === 'date') {
            dynamic_form_div.find('dl').append(
                '<dt>' + response[i]['name'] + '</dt>' +
                '<dd><input type="text" ' +
                'id="'+ response[i]['name_lower'] + '" ' +
                'name="'+ response[i]['name_lower'] + '" ' +
                'placeholder="' + response[i]['details'] + '" ' +
                'title="' + response[i]['details'] + '" ' +
                '</dd>'
            );
            $('[id="' + response[i]["name_lower"] + '"]').datepicker({ dateFormat: 'yy-mm-dd'});
        } else if (response[i]['format'] === 'categorical') {
            let category_options = "";
            const category_list = response[i]['category_list'];
            for (let j = 0; j < category_list.length; j++) {
                const category = category_list[j];
                category_options += (
                    '<option value = "' + category.toLowerCase() + '">' +
                    category + '</option>'
                )
            }
            dynamic_form_div.find('dl').append(
                '<dt>' + response[i]['name'] + '</dt>' +
                '<dd><select ' +
                'id="'+ response[i]['name_lower'] + '" ' +
                'name="'+ response[i]['name_lower'] + '" ' +
                'title="' + response[i]['details'] + '"> ' +
                category_options + '</select></dd>'
            );
        } else if (response[i]['format'] === 'multicat') {
            let category_options = "";
            const category_list = response[i]['category_list'];
            for (let j = 0; j < category_list.length; j++) {
                const category = category_list[j];
                category_options += (
                    '<option value = "' + category.toLowerCase() + '">' +
                    category + '</option>'
                )
            }
            dynamic_form_div.find('dl').append(
                '<dt>' + response[i]['name'] + '</dt>' +
                '<dd><select ' +
                'id="'+ response[i]['name_lower'] + '" ' +
                'name="'+ response[i]['name_lower'] + '" ' +
                'title="' + response[i]['details'] + '"> ' +
                category_options + '</select></dd>'
            );
        }
        feature_checkbox_div.find('ul').append(
            "<li>" +
            "<input id=select_features-" + i + " " +
            "name='select_features' " +
            "type=checkbox value='" + response[i]['name_lower'] + "' " +
            ">" +
            "<label for='checkbox_" + response[i]['name_lower'] + "'>" +
            response[i]['name'] +
            "</label>" +
            "</li>"
        );
        const form_field = $('[id="' + response[i]['name_lower'] + '"]').parent('dd');
        form_field.hide();
        form_field.prev().hide();
        $('#select_features-' + i).change(function () {
            if (this.checked) {
                 form_field.show();
                 form_field.prev().show()
                 update_submit_fields();
            } else {
                 form_field.hide();
                 form_field.prev().hide()
                 update_submit_fields();
            }
        });
    }
};

group_update = function() {
    feature_checkbox_div.empty();
    dynamic_form_div.empty();
    const data_type = data_type_select.val();
    const level = level_select.val();
    const group = group_select.val();
    if (level && group) {
        $.ajax({
            url: "/record/" + data_type + "/" + level + "/" + group + "/",
            type: 'GET',
            success: function (response) {
                feature_checkbox_div.append(
                    "<br><input id=select_all_features type=checkbox>" +
                    "<label>Select all</label>" +
                    "<ul id='select_features'></ul>"
                );
                generate_form(response);
                $('#select_all_features').change(function () {
                    if (this.checked) {
                        feature_checkbox_div.find(":checkbox:not(#select_all_features)").each(function () {
                            this.checked = true;
                        }).trigger('change');
                    } else {
                        feature_checkbox_div.find(":checkbox:not(#select_all_features)").each(function () {
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



update_submit_fields = function () {
    const data_type = data_type_select.val();
    const record_period_div = $('#record_period_div');
    const record_time_div = $('#record_time_div');
    const checkboxes = feature_checkbox_div.find(":checkbox:not(#select_all_features)");
    const count_checkboxes = checkboxes.length;
    const count_checked = checkboxes.filter(":checked").length;
    if (count_checked > 0){
        $('#web_form_div').show();
        if (data_type === 'trait') {
            record_period_div.hide();
            record_time_div.show();
        } else if (data_type === 'condition') {
            record_period_div.show();
            record_time_div.hide();
        }
        if (count_checked === count_checkboxes) {
            $('#select_all_features').prop('checked', true);
        } else {
            $('#select_all_features').prop('checked', false);
        }
    } else {
        $('#select_all_features').prop('checked', false);
        $('#web_form_div').hide();
        $('#generate_template').hide();
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
            	    feature_div.hide();
                    item_count_text.replaceWith(
                        "<a>None</a>"
                    );
                    item_type_text.replaceWith(
                        "<a></a>"
                    );
                } else {
            	    feature_div.show();
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

$('#submit_records').click( function (e) {
    e.preventDefault();
    update_item_count();
    remove_flash();
    const wait_message = "Please wait for submission to complete";
    const flash_wait = "<div id='records_flash' class='flash'>" + wait_message + "</div>";
    $(this).parent().after(flash_wait);
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

$('#tree_id_list').keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});



$(window).on('load', data_type_update);
//$(window).on('load', group_update);
//$( window ).load(update_item_count);

data_type_select.change(data_type_update);
level_select.change(level_update);
group_select.change(group_update);

$("#country, #region, #farm, #field, #block, #level").change(update_item_count);