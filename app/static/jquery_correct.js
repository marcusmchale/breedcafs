const record_type_select = $('#record_type');
const item_level_select = $('#item_level');
const feature_group_select = $('#feature_group');
const feature_selection_div = $('#feature_selection');
const feature_checkbox_div = $('#feature_checkbox_div');
const submit_list_button = $('#list_records');
const submit_delete_button = $('#delete_records');


//Render a calendar in jquery-ui for date selection
$("#submission_date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#submission_date_to").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_date_from").datepicker({ dateFormat: 'yy-mm-dd'});
$("#record_date_to").datepicker({ dateFormat: 'yy-mm-dd'});

group_select_update = function() {
    const record_type = record_type_select.val();
    const item_level = item_level_select.val();
    feature_group_select.empty();
    feature_group_select.append(
        '<option value="">Select group</option>'
    );
    $.ajax({
        url: (
            "/feature_groups"
            + "?record_type=" + record_type
            + "&item_level=" + item_level
        ),
        type: 'GET',
        success: function (response) {
            const feature_groups = response;
            for (let i = 0; i < feature_groups.length; i++) {
                feature_group_select.append(
                    $("<option></option>").attr(
                        "value", feature_groups[i][0]).text(feature_groups[i][1])
                );
            }
        },
        error: function (error) {
            console.log(error);
        }
    });
};

features_update = function() {
    feature_checkbox_div.empty();
    const record_type = record_type_select.val();
    const item_level = item_level_select.val();
    const feature_group = feature_group_select.val();
    $.ajax({
        url: (
            "/features"
            + "?record_type=" + record_type
            + "&item_level=" + item_level
            + "&feature_group=" + feature_group
        ),
        type: 'GET',
        success: function (response) {
            $('#select_all_features').change(function () {
                    const _this = this;
                    if (_this.checked) {
                        feature_checkbox_div.find(":checkbox").each(function () {
                            this.checked = true;
                        })
                    } else {
                        feature_checkbox_div.find(":checkbox").each(function () {
                            this.checked = false;
                        })
                    }
                });
            for (let i = 0; i < response.length; i++) {
                feature_checkbox_div.append(
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
            }
        },
        error: function (error) {
            console.log(error);
        }
    });
};

submit_list_button.click( function (e) {
    e.preventDefault();
    remove_flash();
    const wait_message = "Please wait for template to be generated";
    const flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
    submit_list_button.after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: "/correct/list_records",
        data: data,
        type: 'POST',
        success: function(response) {
            console.log(response);
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
            console.log(error);
            const error_message = error.status === 500 ? 'An error has occurred. Please try again':
                'An unknown error as occurred, please contact an administrator';
            const flash_error = "<div id='submit_flash' class='flash'>" + error_message + "</div>";
            $("#submit_flash").replaceWith(flash_error);
		}
    })
});


submit_delete_button.click( function (e) {
    e.preventDefault();
    remove_flash();
    const wait_message = "Please wait for template to be generated";
    const flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
    submit_delete_button.after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: "/correct/delete_records",
        data: data,
        type: 'POST',
        success: function(response) {
            console.log(response);
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
            console.log(error);
            const error_message = error.status === 500 ? 'An error has occurred. Please try again':
                'An unknown error as occurred, please contact an administrator';
            const flash_error = "<div id='submit_flash' class='flash'>" + error_message + "</div>";
            $("#submit_flash").replaceWith(flash_error);
		}
    })
});



item_level_select.change(group_select_update);
record_type_select.change(group_select_update);
item_level_select.change(features_update);
record_type_select.change(features_update);
feature_group_select.change(features_update);
group_select_update();features_update();