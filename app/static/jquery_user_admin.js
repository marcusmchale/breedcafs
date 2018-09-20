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
		$("#confirmed").find('tr').after(data['confirmed']);
		$("#unconfirmed").find('tr').after(data['unconfirmed']);
	})
}


$('#submit_user_email').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for new user email to be added to the list of allowed emails";
	flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var admin_add_user_email = $.ajax({
		url: "/admin/add_allowed_email",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('success')) {
				flash_submitted = "<div id='submit_flash' class='flash'>Added email to allowed list: " + response.success + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
				update_user_allowed_emails();
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



$('#remove_user_email').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for email to be removed from the list of allowed emails";
	flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait)
	var admin_remove_user_email = $.ajax({
		url: "/admin/remove_allowed_email",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('success')) {
				flash_submitted = "<div id='submit_flash' class='flash'>Removed email from allowed list: " + response.success + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
				update_user_allowed_emails();
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


//update the  user allowed emails list
update_user_allowed_emails = function () {
	var user_allowed_emails = $.ajax({
		type: 'GET',
		url: "/admin/get_user_allowed_emails",
	});
	user_allowed_emails.done(function(data){
		$("#emails_list").empty();
		var allowed_emails = [].concat(data).sort();
		for (var i=0; i < allowed_emails.length; i++) {
			$("#emails_list").append('<li><input id="emails_list-' + i + '" name="emails_list" value="' + allowed_emails[i] + '" type="checkbox"</li>' + '<label for="emails_list-' + i + '">' + allowed_emails[i] + '</label>')
		}			
	})
}

//allow user confirmation/ remove access
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

