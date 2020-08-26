update_files = function () {
	const file_table_div = $("#file_table");
	$.ajax({
		type: 'GET',
		url: "/download/files/list",
		success: function(response) {
			file_table_div.replaceWith(response)
		},
		error: function(error) {
			console.log(error);
		}
	});
};


$( window ).load(update_files);