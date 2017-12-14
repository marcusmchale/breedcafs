remove_flash = function() {
	$(".flash").remove();
}


update_partners = function () {
	var request = $.ajax({
		type: 'GET',
		url: "/user/get_affiliations"
	});
	request.done(function(data){
		$('ul#confirmed').empty();
		$('ul#pending').empty();
		$('ul#other').empty();
		//make the new rows from data
		confirmed = data['confirmed'];
		pending = data['pending'];
		other = data['other'];
		for (var i = 0; i < confirmed.length; i++) {
			$('ul#confirmed').append(
				$('<li>' + confirmed[i][1] + '</li>')
				);
		}
		for (var i = 0; i < pending.length; i++) {
			$('ul#pending').append(
				$('<li><input id="pending-' + i + '" name="pending" value="' + pending[i][0] + '" type="checkbox"> <label for="pending-' + i + '">' + pending[i][1] + '</label></li>')
				);
		}
		for (var i = 0; i < other.length; i++) {
			$("ul#other").append(
				$('<li><input id="other-' + i + '" name="other" value="' + other[i][0] + '" type="checkbox"> <label for="other-' + i + '">' + other[i][1] + '</label></li>')
				);
		}
	})
}

update_partners();


$('#submit_affiliations').click( function(e) {
	e.preventDefault();
	remove_flash();
	wait_message = "Please wait for affiliations to be updated";
	flash_wait = "<div id='submit_flash' class='flash'>" + wait_message + "</div>";
	$(this).after(flash_wait);
	var update_affiliations = $.ajax({
		url: "/user/add_affiliations",
		data: $("form").serialize(),
		type: 'POST',
		success: function(response) {
			if (response.hasOwnProperty('success')) {
				flash_submitted = "<div id='submit_flash' class='flash'>" + response.success + "</div>";
				$("#submit_flash").replaceWith(flash_submitted);
				update_partners();
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
