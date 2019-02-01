const level_select = $('#level');
const location_div = $('#location');
const block_div = $('#block_div');
const tree_div = $('#tree_selection_div');
const sample_div = $('#sample_selection_div');
const item_count_div = $('#item_count_div');
const replicates_div = $('#replicates');


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
                    item_count_text.replaceWith(
                        "<a>None</a>"
                    );
                    item_type_text.replaceWith(
                        "<a></a>"
                    );
                } else {
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

$(window).on('load', level_update);

level_select.change(level_update);
