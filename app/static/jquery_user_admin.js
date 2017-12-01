remove_flash = function() {
	$(".flash").remove();
}

//update blockx (not in location form as different rendering on different pages (list dropbox))
update_users = function () {
	var request = $.ajax({
		type: 'GET',
		url: "/admin/users"
	});
	request.done(function(data){
		//remove table entries
		$("td").parent().remove();
		//make the new rows from data
		$("#unconfirmed_users").next().next("table").find('tr').after(data['unconfirmed'])
		$("#confirmed_users").next().next("table").find('tr').after(data['confirmed'])
	})
}

update_users();

$('#admin_submit').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for users to be confirmed/unconfirmed";
	flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var admin_confirm_users = $.ajax({
		url: "/admin/confirm_users",
		data: $("form").serializeArray(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('success')) {
				flash_submitted = "<div id='submit_flash' class='flash'>Updated users: " + response.success + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
				update_users();
			}
		},
		error: function (error) {
			console.log(error);
		}
	});
})