const level_select = $('#level');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const group_select = $('#condition_group');
const condition_checkbox_div = $('#condition_checkbox_div');
const dynamic_form_div = $('#dynamic_form_div');
const location_div = $('#location');
const condition_div = $('#condition_selection');

$("#record_start").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_end").datepicker({ dateFormat: 'yy-mm-dd'});

block_div.hide();
tree_div.hide();

remove_flash = function() {
	$(".flash").remove();
};

level_update = function() {
    const level = level_select.val();
    group_select.empty();
    group_select.append(
        '<option value="">Select group</option>'
    );
    if (level === "") {
        location_div.hide();
        condition_div.hide();
    } else {
        location_div.show();
        condition_div.show();
        if (level === "field") {
            block_div.hide();
            tree_div.hide();
        }
        else if (level === "block") {
            block_div.show();
            tree_div.hide();
        }
        else if (level === 'tree') {
            block_div.show()
            tree_div.show();
        }
    }
    if (level) {
        $.ajax({
            url: "/record/conditions/" + level + "/",
            type: 'GET',
            success: function (response) {
                const condition_groups = response.sort();
                for (let i = 0; i < condition_groups.length; i++) {
                    group_select.append(
                        $("<option></option>").attr(
                            "value", condition_groups[i][0]).text(condition_groups[i][1])
                    );
                }
            },
            error: function (error) {
                console.log(error);
            }
        });
    }
    group_update();
};

group_update = function() {
    condition_checkbox_div.empty();
    dynamic_form_div.empty();
    const level = level_select.val();
    const group = group_select.val();
    if (level && group) {
        $.ajax({
            url: "/record/conditions/" + level + "/" + group + "/",
            type: 'GET',
            success: function (response) {
                condition_checkbox_div.append(
                    "<br><input id=select_all_conditions type=checkbox>" +
                    "<label>Select all</label>" +
                    "<ul id='select_conditions'></ul>"
                );
                dynamic_form_div.append('<dl></dl>');
                for (let i = 0; i < response.length; i++) {
                    if (['numeric', 'percent'].includes(response[i]['format'])) {
                        dynamic_form_div.find('dl').append(
                            '<dt>' + response[i]['name'] + '</dt>' +
                            '<dd><input type="text" ' +
                            'id="'+ response[i]['name_lower'] + '" ' +
                            'name="'+ response[i]['name_lower'] + '" ' +
                            'placeholder="' + response[i]['details'] + '" ' +
                            'title="' + response[i]['details'] + '" ' +
                            '</dd>'
                        )
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
                    }
                    condition_checkbox_div.find('ul').append(
                        "<li>" +
                        "<input id=select_conditions-" + i + " " +
                        "name='select_conditions' " +
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
                    $('#select_conditions-' + i).change(function () {
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
                $('#select_all_conditions').change(function () {
                    if (this.checked) {
                        condition_checkbox_div.find(":checkbox:not(#select_all_conditions)").each(function () {
                            this.checked = true;
                        }).trigger('change');
                    } else {
                        condition_checkbox_div.find(":checkbox:not(#select_all_conditions)").each(function () {
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
    const checkboxes = condition_checkbox_div.find(":checkbox:not(#select_all_conditions)");
    const count_checkboxes = checkboxes.length;
    const count_checked = checkboxes.filter(":checked").length;
    if (count_checked > 0){
        $('#web_form_div').show();
        $('#generate_template').show();
        if (count_checked === count_checkboxes) {
            $('#select_all_conditions').prop('checked', true);
        } else {
            $('#select_all_conditions').prop('checked', false);
        }
    } else {
        $('#select_all_conditions').prop('checked', false);
        $('#web_form_div').hide();
        $('#generate_template').hide();
    }
};

$(window).on('load', level_update);

level_select.change(level_update);
group_select.change(group_update);


$('#submit_records').click( function (e) {
    e.preventDefault();
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
                    for (const i in response) {
                        if (response.hasOwnProperty(i)) {
                            for (const key in response[i]) {
                                if (response[i].hasOwnProperty(key)) {
                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
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
