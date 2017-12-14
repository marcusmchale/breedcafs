remove_flash = function() {
	$(".flash").remove();
}

//update blockx (not in location form as different rendering on different pages (list dropbox))
update_admins = function () {
	var request = $.ajax({
		type: 'GET',
		url: "/admin/partner_admins"
	});
	request.done(function(data){
		//remove table entries
		$("td").parent().remove();
		//make the new rows from data
		$("#admin").find('tr').after(data['partner_admins'])
		$("#non-admin").find('tr').after(data['not_partner_admins'])
	})
}

$('#submit').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for user privilege to be changed";
	flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var admin_confirm_users = $.ajax({
		url: "/admin/confirm_partner_admins",
		data: $("form").serializeArray(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('success')) {
				flash_submitted = "<div id='submit_flash' class='flash'>Updated users: " + response.success + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
				update_admins();
			}
			if (response.hasOwnProperty('error')) {
				flash_submitted = "<div id='submit_flash' class='flash'>" + response.error + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
			}
		},
		error: function (error) {
			console.log(error);
		}
	});
})