
remove_flash = function() {
	$(".flash").remove();
};

const partner_to_copy = $('#partner_to_copy');
const group_to_copy = $('#group_to_copy');


update_group_to_copy_list = function() {
    group_to_copy.empty();
    group_to_copy.append($("<option></option>").attr("value", "").text(""));
    group_to_copy.prop("disabled", true);
    const partner = (partner_to_copy.val() !== "") ? partner_to_copy.val() : false;
    $.ajax({
       type: 'GET',
       url: '/record/input_groups',
       data: {
           partner: partner,
           username: false
       },
       success: function(response) {
           const groups = response;
           for (let i = 0; i < groups.length; i++) {
               group_to_copy.append(
                   $("<option></option>").attr(
                       "value", groups[i][0]).text(groups[i][1])
               );
           }
           group_to_copy.prop( "disabled", false);
       },
        error: function(error) {
           console.log('Error in update_group_to_copy_list');
           console.log(error);
        },
    });
};

partner_to_copy.change(update_group_to_copy_list);


const group_to_copy_levels = $('#group_to_copy_levels');

update_group_to_copy_levels = function() {
    group_to_copy_levels.empty();
    $.ajax({
       type: 'GET',
       url: '/record/input_group_levels',
       data: {
           input_group: group_to_copy.val(),
           partner: partner_to_copy.val(),
           username: false
       },
       success: function(response) {
           const levels = response;
           for (let i = 0; i < levels.length; i++) {
               group_to_copy_levels.append(
                        $("<li></li>").attr(
                            "value", levels[i][0]).text(levels[i][1])
                    );
           }
       },
       error: function(error) {
           console.log('Error in update_group_levels');
           console.log(error);
       },
    });
};

group_to_copy.change(update_group_to_copy_levels);


const group_to_copy_members= $('#group_to_copy_members');

update_group_to_copy_members = function() {
    group_to_copy_members.empty();
    if (group_to_copy.val() !== '') {
        const partner = (partner_to_copy.val() !== "") ? partner_to_copy.val() : false;
        $.ajax({
            type: 'GET',
            url: '/record/inputs_selection',
            data: {
                partner: partner,
                input_group: group_to_copy.val()
            },
            success: function (response) {
                const groups = response;
                for (let i = 0; i < groups.length; i++) {
                    group_to_copy_members.append(
                        $("<li></li>").attr(
                            "value", groups[i][0]).text(groups[i][1])
                    );
                }
            },
            error: function(error) {
                console.log('Error in update_group_to_copy_members');
                console.log(error);
            },
        });
    }
};

group_to_copy.change(update_group_to_copy_members);

const input_group_name = $('#input_group_name');

provide_default_name = function() {
    const selected_name = $('#group_to_copy option:selected').text()
    //if (input_group_name.val().trim() == '') {
    input_group_name.val(selected_name);
   //}
};

group_to_copy.change(provide_default_name);

partner_to_copy.change(update_group_to_copy_members);

const submit_input_group_button = $('#submit_input_group_name');

submit_input_group_button.click( function (e) {
    e.preventDefault();
    remove_flash();
    const wait_message = "Please wait for group to be registered";
    const flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
    submit_input_group_button.after(flash_wait);
    const data = $("form").serialize();
    $.ajax({
        url: (
            "/record/add_input_group"
        ),
        data: data,
        type: 'POST',
        success: function(response) {
            remove_flash();
            if (response.hasOwnProperty('submitted')) {
                const flash_submitted = "<div id='add_input_group_flash' class='flash'> Added group: " + response.submitted[1] + "</div>";
                submit_input_group_button.after(flash_submitted);
                update_group_select(response.submitted[0]);
                update_group_levels();
            } else if (response.hasOwnProperty('found')) {
                const flash_submitted = "<div id='add_input_group_flash' class='flash'> Found group: " + response.found[1] + "</div>";
                submit_input_group_button.after(flash_submitted);
                update_group_select(response.found[0]);
                update_group_levels();
            }
            else {
                if (response.hasOwnProperty('errors')) {
                    const errors = response['errors'];
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
           console.log('Error in add_input_group');
           console.log(error);
       },
    });
});

const group_select = $('#input_group_select');

update_group_select = function(set_group = "") {
    group_select.empty();
    group_select.append($("<option></option>").attr("value", "").text("Select group"));
    group_select.prop("disabled", true);
    $.ajax({
       type: 'GET',
       url: '/record/input_groups',
        data: {
           username: true
       },
       success: function(response) {
           const groups = response;
           for (let i = 0; i < groups.length; i++) {
               group_select.append(
                   $("<option></option>").attr(
                       "value", groups[i][0]).text(groups[i][1])
               );
           }
           group_select.prop("disabled", false);
           group_select.val(set_group);
           update_group_inputs();
       },
       error: function(error) {
           console.log(error);
       },
    });
};

const group_levels = $('#group_levels_select');

update_group_levels = function() {
    //uncheck all group_levels;
    group_levels.find(":checkbox").each(function () {
        this.checked = false;
    });
    $.ajax({
       type: 'GET',
       url: '/record/input_group_levels',
       data: {
           input_group: group_select.val(),
           username: true
       },
       success: function(response) {
           const levels = response;
           for (let i = 0; i < levels.length; i++) {
               group_levels.find(":checkbox[value='" + levels[i][0] +"']").prop("checked", true);
           }
       },
       error: function(error) {
           console.log('Error in update_group_levels');
           console.log(error);
       },
    });
};

group_select.change(update_group_levels);
update_group_levels();

const group_inputs = $('#group_inputs');

update_group_inputs = function() {
    group_inputs.empty();
    group_inputs.css("min-height", "50px");
    group_inputs.prop("disabled", true);
    $.ajax({
       type: 'GET',
       url: '/record/inputs_selection',
       data: {
           input_group: group_select.val(),
           username: true
       },
       success: function(response) {
           const inputs = response;
           for (let i = 0; i < inputs.length; i++) {
               group_inputs.append(
                   $('<li></li>').append(
                       $('<input type="hidden">').attr({
                            "id": "group_inputs-" + i,
                            "name": "group_inputs",
                            "value": inputs[i][0]
                       })
                   ).append($('<label>').text(
                   inputs[i][1]
               )));
           }
           group_inputs.prop( "disabled", false);
           group_inputs.sortable( {
                connectWith: all_inputs
            });
           //group_inputs.disableSelection();
           update_all_inputs(inputs);
       },
       error: function(error) {
           console.log('Error in update_group_inputs');
           console.log(error);
       },
    });
};

group_select.change(update_group_inputs);
update_group_inputs();

const record_type_select = $('#record_type');
const item_level_select = $('#item_level');

const all_inputs = $('#all_inputs');

update_all_inputs = function() {
    all_inputs.empty();
    all_inputs.prop("disabled", true);
    $.ajax({
        type: 'GET',
        url: '/record/inputs_selection',
        data: {
            input_group: group_select.val(),
            username: true,
            inverse: true,
            record_type: record_type_select.val(),
            item_level: item_level_select.val()
        },
        success: function(response) {
            const inputs = response;
            for (let i = 0; i < inputs.length; i++) {
                all_inputs.append(
                   $('<li></li>').append(
                       $('<input type="hidden">').attr({
                            "id": "all_inputs-" + i,
                            "name": "all_inputs",
                            "value": inputs[i][0]
                       })
                   ).append($('<label>').text(
                   inputs[i][1]
               )));
            }
            all_inputs.prop("disabled", false);
            all_inputs.sortable( {
                connectWith: group_inputs
            });
            all_inputs.disableSelection();
        },
        error: function (error) {
           console.log('Error in update_all_inputs');
            console.log(error);
        }
    });
};

record_type_select.change(update_all_inputs);
item_level_select.change(update_all_inputs);

const commit = $('#commit_group_changes');

commit.click(function(e) {
    e.preventDefault();
    remove_flash();
    const wait_message = "Please wait for changes to be stored";
    const flash_wait = "<div id='commit_flash' class='flash'>" + wait_message + "</div>";
    commit.after(flash_wait);
    const group_inputs_list = $('#group_inputs li');
    for (let i = 0; i < group_inputs_list.length; i++) {
        $(group_inputs_list[i]).find('input').attr({
            "id": "group_inputs-" + i,
            "name": "group_inputs"
        })
    }
    const all_inputs_list = $('#all_inputs li');
    for (let i = 0; i < all_inputs_list.length; i++) {
        $(all_inputs_list[i]).find('input').attr({
            "id": "all_inputs-" + i,
            "name": "all_inputs"
        })
    }
    $.ajax({
        type: 'POST',
        url: '/record/commit_group_changes',
        data: $("form").serialize(),
       success: function(response) {
           remove_flash();
           update_group_inputs();
           if (response.hasOwnProperty('submitted')) {
               const flash_submitted_message = response['submitted'];
               const flash_submitted = "<div id='commit_flash' class='flash'>" + flash_submitted_message + "</div>";
               $('#manage_group_members_div').after(flash_submitted)
           }
           else {
                if (response.hasOwnProperty('errors')) {
                    const errors = response['errors'];
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
       },
    })
});

//const add_to_group = $('#add_to_group');
//
//add_to_group.click(function(e) {
//   e.preventDefault();
//   remove_flash();
//   const wait_message = "Please wait for input variables to be added to group";
//   const flash_wait = "<div id='add_to_group_flash' class='flash'>" + wait_message + "</div>";
//   add_to_group.after(flash_wait);
//   $.ajax({
//       type: 'POST',
//       url: '/record/add_inputs_to_group',
//       data: $("form").serialize(),
//       success: function(response) {
//           remove_flash();
//           update_group_inputs();
//           if (response.hasOwnProperty('submitted')) {
//               const flash_submitted_message = response['submitted'];
//               const flash_submitted = "<div id='add_to_group_flash' class='flash'>" + flash_submitted_message + "</div>";
//               add_to_group.after(flash_submitted)
//           }
//           else {
//                if (response.hasOwnProperty('errors')) {
//                    const errors = response['errors'];
//                    for (let i = 0; i < errors.length; i++) {
//                        for (const key in errors[i]) {
//                            if (errors[i].hasOwnProperty(key)) {
//                                const flash = "<div id='flash_" + key + "' class='flash'>" + errors[i][key][0] + "</div>";
//                                $('[id="' + key + '"').after(flash);
//                            }
//                        }
//                    }
//                }
//            }
//       }
//   })
//});

//const trash = $('#trash');
//
//trash.droppable({
//    accept: '#group_inputs > li',
//    activeClass: 'dropArea',
//    hoverClass: 'dropAreaHover',
//    drop: function(event, ui) {
//
//        //ui.draggable.remove()
//    }
//});
