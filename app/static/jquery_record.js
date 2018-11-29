const level_select = $('#level');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const group_select = $('#condition_group');
const condition_checkbox_div = $('#condition_checkbox_div');
const dynamic_form_div = $('#dynamic_form_div');

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
    if (["field", ""].includes(level)) {
        block_div.hide();
        tree_div.hide();
    }
    else if (level === "block") {
        block_div.show()
        tree_div.hide();
    }
    else if (level === 'tree') {
        block_div.show()
        tree_div.show();
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
                                '<option value = "' + category.toLowerCase + '">' +
                                category + '</option>'
                            )
                        }
                        dynamic_form_div.find('dl').append(
                            '<dt>' + response[i]['name'] + '</dt>' +
                            '<dd><select ' +
                            'id="'+ response[i]['name_lower'] + '" ' +
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
                const flash_submitted = "<div id='records_flash' class='flash'>" + response.submitted + "</div>";
                $("#records_flash").replaceWith(flash_submitted);
            } else {
                $("#records_flash").remove();
                    for (const i in response) {
                        if (response.hasOwnProperty(i)) {
                            for (const key in response[i]) {
                                if (response[i].hasOwnProperty(key)) {
                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                                    $('#' + key).after(flash);
                                }
                            }
                        }
                    }
            }
        },
        error: function(error) {
			console.log(error);
		}
    })
});


//$('#submit_controlled_environment').click( function (e) {
//    e.preventDefault();
//    remove_flash();
//    const wait_message = "Please wait for controlled environment data to complete submission";
//    const flash_wait = "<div id='controlled_environment_flash' class='flash'>" + wait_message + "</div>";
//    $(this).parent().after(flash_wait);
//    const data = $("form").serialize();
//    $.ajax({
//        url: "/record/controlled_environment",
//        data: data,
//        type: 'POST',
//        success: function(response) {
//            console.log(response);
//            if (response.hasOwnProperty('submitted')) {
//                const flash_submitted = "<div id='controlled_environment_flash' class='flash'>" + response.submitted + "</div>";
//                $("#controlled_environment_flash").replaceWith(flash_submitted);
//            } else {
//                $("#controlled_environment_flash").remove();
//                    for (const i in response) {
//                        if (response.hasOwnProperty(i)) {
//                            for (const key in response[i]) {
//                                if (response[i].hasOwnProperty(key)) {
//                                    const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
//                                    $('#' + key).after(flash);
//                                }
//                            }
//                        }
//                    }
//            }
//        },
//        error: function(error) {
//			console.log(error);
//		}
//    })
//});


//const category_select = $("#treatment_category");
//const value_select = $("#treatment_value");
//category_select.hide();
//value_select.hide();
//
//update_conditions_form = function() {
//    const condition_type = $("#condition_type").find(":selected").val();
//    const treatment_div = $("#treatment")
//    if (treatment_type !== "") {
//        $.ajax({
//            type: 'GET',
//            url:'/record/treatment_details/' + treatment_type + '/',
//            success: function(response) {
//                const treatment_checkbox_div = $("#treatment_checkbox_div");
//                const treatment_form = $("#treatment_form");
//                treatment_checkbox_div.empty();
//                treatment_checkbox_div.append(
//                    "<br><input id=select_all_treatments type=checkbox>" +
//                    "<label>Select all</label>" +
//                    "<ul></ul>"
//                );
//                treatment_form.empty()
//                for (const i in response) {
//                    if (response.hasOwnProperty(i)) {
//                        const treatment = response[i];
//                        $('#treatment_checkbox_div ul').append(
//                            "<li>" +
//                            "<input id=checkbox_" + i + " " +
//                            "type=checkbox value='" + treatment['name_lower'] + "' " +
//                            ">" +
//                            "<label for='checkbox_" + treatment['name_lower'] + "'>" +
//                            treatment['name'] +
//                            "</label>" +
//                            "</li>"
//                        )
//                        $('#treatment_form').append(
//
//                        )
//                    }
//                }
//                $('#select_all_treatments').change(function() {
//                   if(this.checked) {
//                       treatment_checkbox_div.find(":checkbox").each(function() {
//                           this.checked=true;
//                       });
//                   } else {
//                       treatment_checkbox_div.find(":checkbox").each(function() {
//                           this.checked=false;
//                       });
//                   }
//                });
//
//            }
//        })
//        //const treatment_name =  $("#treatment_name").find(":selected").val();
//        //category_select.empty();
//        //value_select.attr("value", "");
//        //if (treatment_name !== "") {
//        //    $.ajax({
//        //        type: 'GET',
//        //        url: '/record/treatment_details/' + treatment_name + '/',
//        //        success: function(response){
//        //            const format = response['format'];
//        //            if (format === 'categorical') {
//        //                value_select.hide();
//        //                category_select.show();
//        //                const categories = response['category_list'];
//        //                category_select.append($("<option></option>").attr("value", "").text("Select Category"));
//        //                for (let i = 0; i< categories.length; i++) {
//        //                    category_select.append($("<option></option>").attr("value", categories[i]).text(categories[i]));
//        //                }
//        //            } else if (['numeric','percent'].includes(format)) {
//        //                category_select.hide();
//        //                value_select.show();
//        //                value_select.attr("placeholder", response['details']);
//        //                value_select.attr("title", response['details']);
//        //            }
//        //        },
//        //        error: function(response) {
//        //        }
//        //    })
//        //}
//    }
//};

//$('#select_all_treatments').change(function() {
   //if(this.checked) {
   //    $('#treatment_checkboxes').find(":checkbox").each(function() {
   //        this.checked=true;
   //    });
   //} else {
   //    $('#treatment_checkboxes').find(":checkbox").each(function() {
   //        this.checked=false;
   //    });
   //}
//});

//$('[id="select_all_' + trait_id + '"]').change(function () {
//		$(this).parent().find('label').css('text-decoration', 'none');
//		if (this.checked) {
//			$(this).parent().next().find("input").prop("checked", true);
//		}
//		else {
//			$(this).parent().next().find("input").prop("checked", false);
//		}
//	});
//

//$("#treatment_type").change(update_treatment_form);


//$('#submit_treatment').click( function (e) {
//    e.preventDefault();
//    remove_flash();
//    const wait_message = "Please wait for treatment assignment to complete";
//    const flash_wait = "<div id='treatment_flash' class='flash'>" + wait_message + "</div>";
//    const treatment_flash = $("#treatment_flash");
//    $(this).parent().after(flash_wait);
//    const data = $("form").serialize();
//    $.ajax({
//        url: "/record/treatment",
//        data: data,
//        type: 'POST',
//        success: function(response) {
//            console.log(response);
//            if (response.hasOwnProperty('submitted')) {
//                console.log(response);
//                const flash_submitted = "<div id='treatment_flash' class='flash'>" + response.submitted + "</div>";
//                treatment_flash.replaceWith(flash_submitted);
//            } else {
//                treatment_flash.remove();
//                for (const i in response) {
//                    if (response.hasOwnProperty(i)) {
//                        for (const key in response[i]) {
//                            if (response[i].hasOwnProperty(key)) {
//                                const flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
//                                $('#' + key).after(flash);
//                            }
//                        }
//                    }
//                }
//            }
//        },
//        error: function(error) {
//			console.log(error);
//		}
//    })
//});


//$('#record_type').val('').change(show_record_form).change(remove_flash);