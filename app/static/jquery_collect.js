const sampling_activity_select = $('#sampling_activity');
const level_select = $('#item_level');
const location_div = $('#location');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const sample_div = $('#sample_selection_div');
const item_count_div = $('#item_count_div');
const replicates_div = $('#replicates');
const country_select = $('#country');
const region_select = $('#region');
const farm_select = $('#farm');
const field_select = $('#field');
const block_select = $('#block');
const field_uid_list_box = $('#field_uid_list');
const block_id_list_box = $('#block_id_list');
const tree_id_list_box = $('#tree_id_list');
const sample_id_list_box = $('#sample_id_list');
const per_item_count = $('#per_item_count');
var item_count = 0

change_activity = function () {
    const sampling_activity = sampling_activity_select.val();
    if (sampling_activity === 'sample registration (sub-sample)') {
        level_select.children('option').attr('disabled', true);
        level_select.children('option[value="sample"]').attr('disabled', false).attr('selected', true);
        $('.selDiv option:eq(1)').prop('selected', true)
    } else if (sampling_activity == 'sample registration (harvest)') {
        level_select.children('option').attr('disabled', false);
        level_select.children('option[value="sample"]').attr('disabled', true);
        level_select.children('option[value=""]').attr('selected', true);
    } else {
        level_select.children('option').attr('disabled', false);
    }
};

sampling_activity_select.change(change_activity);


update_item_count = function() {
	const sel_level = level_select.find(":selected").val();
	if (sel_level && sel_level !== "") {
		const item_count_text = $('#item_count_div a:eq(0)');
		const item_type_text = $('#item_count_div a:eq(1)');
        const sel_country = $("#country").find(":selected").val();
        const sel_region = $("#region").find(":selected").val();
        const sel_farm = $("#farm").find(":selected").val();
        const sel_field = $("#field").find(":selected").val();
        const field_uid_list = $("#field_uid_list").val();
        const sel_block = $("#block").find(":selected").val();
        const block_id_list = $("#block_id_list").val();
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
                + "&field_uid_list=" + field_uid_list
                + "&block_uid=" + sel_block
                + "&block_id_list=" + block_id_list
				+ "&tree_id_list=" + tree_id_list
                + "&sample_id_list=" + sample_id_list
            ),
            success: function (response) {
            	if (isNaN(response['item_count']) || response['item_count'] === 0) {
                    item_count_text.replaceWith(
                        "<a>None</a>"
                    );
                    item_type_text.replaceWith(
                        "<a></a>"
                    );
                    item_count = 0;
                    update_sample_count();
                } else {
                    item_count_text.replaceWith(
                        "<a>" + response['item_count'] + "</a>"
                    );
                    const item_type_text_entry = (response['item_count'] === 1)
                        ? sel_level.toString() : sel_level.toString() + 's';
                    item_type_text.replaceWith(
                        "<a>" + item_type_text_entry + "</a>"
                    );
                    item_count = response['item_count']
                    update_sample_count();
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

level_update = function() {
    const level = level_select.val();
    if (level === "") {
        location_div.hide();
        block_div.hide();
        tree_div.hide();
        sample_div.hide();
        item_count_div.hide();
        replicates_div.hide();
    } else {
        location_div.show();
        item_count_div.show();
        replicates_div.show();
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
    }
    update_item_count();
    remove_flash();
};

update_sample_count = function () {
    var n = (isNaN(parseInt(per_item_count.val())) ? 1 : per_item_count.val())
    console.log(n);
    sample_count = (n * item_count);
    const sample_count_text = $('#sample_count_div a:eq(0)');
    sample_count_text.replaceWith(
        "<a>" + sample_count + "</a>"
    );
};

per_item_count.on('input', update_sample_count);

$(window).on('load', level_update);


$(level_select).change(level_update);
$(sampling_activity_select).change(level_update);

$(region_select).change(update_item_count);
$(farm_select).change(update_item_count);
$(field_select).change(update_item_count);
$(country_select).change(update_item_count);
$(block_select).change(update_item_count);

field_uid_list_box.keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});

block_id_list_box.keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});

tree_id_list_box.keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});

sample_id_list_box.keypress(function(e){
	if (e.keyCode === 13 || e.keyCode === 10) {
		update_item_count();
	}
});

$("#submit_collect").click( function(e) {
	e.preventDefault();
	remove_flash();
	const wait_message = "Please wait for samples to be registered. A file listing their UIDs will be generated"
	const flash_wait = "<div id='files_flash' class='flash'>" + wait_message + "</div>";
	$(this).parent().after(flash_wait);
	const data = $("form").serialize();
	$.ajax({
	    url: "/collect/register_samples",
	    data: data,
		type: 'POST',
		success: function(response) {
	        update_item_count();
			if (response.hasOwnProperty('submitted')) {
				const flash_submitted = "<div id='files_flash' class='flash'>" + response.submitted + "</div>";
				$("#files_flash").replaceWith(flash_submitted);
			} else {
				$("#files_flash").remove();
				if (response.hasOwnProperty('errors')) {
				    const errors = response['errors'];
                    for (let i = 0; i < errors.length; i++) {
                        for (const key in errors[i]) {
                            if (errors[i].hasOwnProperty(key)) {
                                const flash = "<div id='flash_" + key + "' class='flash'>" + errors[i][key][0] + "</div>";
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
})


