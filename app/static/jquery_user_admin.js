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

//update_users();


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

//add a div containing elements from the  user allowed emails list


//update blockx (not in location form as different rendering on different pages (list dropbox))
update_user_allowed_emails = function () {
	var user_allowed_emails = $.ajax({
		type: 'GET',
		url: "/admin/get_user_allowed_emails",
	});
	user_allowed_emails.done(function(data){
		$('form').before("<div id=allowed_email_list><a>Your allowed emails list:</a><ul id=allowed_email_list></ul></div>");
		$("#allowed_email_list").empty();
		var allowed_emails = [].concat(data).sort();
		for (var i=0; i < allowed_emails.length; i++) {
			$("#allowed_email_list").append('<li>' + allowed_emails[i] + '</li>')
		}			
	})
}

update_user_allowed_emails();