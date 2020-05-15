const record_type_select = $('#record_type');
const item_level_select = $('#item_level');
const input_group_select = $('#input_group');
const input_variable_selection_div = $('#input_variable_selection_div');
const input_variable_checkbox_div = $('#input_variable_checkbox_div');
const submit_download_button = $('#submit_download');
const block_select_div = $('#block_div');
const tree_selection_div = $('#tree_selection_div');
const sample_selection_div = $('#sample_id_list');


//Render a calendar in jquery-ui for date selection
$("#submission_date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#submission_date_to").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_date_to").datepicker({ dateFormat: 'yy-mm-dd'});

items_update = function() {
    const item_type = item_level_select.val();
    if (item_type === 'field') {
        block_select_div.hide();
        tree_selection_div.hide();
        sample_selection_div.hide();
    }
};

group_select_update = function() {
    const record_type = record_type_select.val();
    const item_level = item_level_select.val();
    input_group_select.empty();
    input_group_select.append(
        '<option value="">Select group</option>'
    );
    $.ajax({
        url: (
            "/record/input_groups"
            + "?record_type=" + record_type
            + "&item_level=" + item_level
            + "&username=true"
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
};

const select_all = $('#select_all_input_variables')
select_all.attr('checked', true);
update_select_all = function () {
    if (select_all.prop('checked')) {
        input_variable_checkbox_div.find(":checkbox").each(function () {
            this.checked = true;
        })
    } else {
        input_variable_checkbox_div.find(":checkbox").each(function () {
            this.checked = false;
        })
    }
}
select_all.change(update_select_all);

inputs_variables_update = function() {
    input_variable_checkbox_div.empty();
    const record_type = record_type_select.val();
    const item_level = item_level_select.val();
    const input_group = input_group_select.val();
    let args = ("?record_type=" + record_type
            + "&item_level=" + item_level
            + "&record_type=" + record_type
            + "&details=true"
    );
    if (input_group !== '') {
        args = (args
            + "&input_group=" + escape(input_group)
            + "&username=true"
        )
    }
    $.ajax({
        url: (
            "/record/inputs_selection" + args
        ),
        type: 'GET',
        success: function (response) {
            const inputs = response;
            for (let i = 0; i < response.length; i++) {
                input_variable_checkbox_div.append(
                   $('<li></li>').append(
                       $('<input type="checkbox">').attr({
                            "id": "inputs-" + i,
                            "name": "inputs",
                            "value": inputs[i]['name_lower']
                       })
                   ).append($('<label title="' + inputs[i]['details'] + '">').text(
                   inputs[i]['name']
               )));
            }
            update_select_all();
        },
        error: function (error) {
            console.log(error);
        }
    });
};

submit_download_button.click( function (e) {
    e.preventDefault();
    remove_flash();
    const wait_message = "Please wait for template to be generated";
    const flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
    submit_download_button.after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: "/download/generate_file",
        data: data,
        type: 'POST',
        success: function(response) {
            if (response.hasOwnProperty('result')) {
                const flash_submitted = "<div id='submit_flash' class='flash'>" + response.result + "</div>";
                $("#submit_flash").replaceWith(flash_submitted);
            } else {
                $("#submit_flash").remove();
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
            const flash_error = "<div id='submit_flash' class='flash'>" + error_message + "</div>";
            $("#submit_flash").replaceWith(flash_error);
		}
    })
});


item_level_select.change(group_select_update);
record_type_select.change(group_select_update);
item_level_select.change(inputs_variables_update);
record_type_select.change(inputs_variables_update);
input_group_select.change(inputs_variables_update);
group_select_update();inputs_variables_update();