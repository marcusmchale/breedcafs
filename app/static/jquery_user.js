remove_flash = function() {
	$(".flash").remove();
}

//update blockx (not in location form as different rendering on different pages (list dropbox))
update_partners = function () {
	var request = $.ajax({
		type: 'GET',
		url: "/user/get_affiliations"
	});
	request.done(function(data){
		$('ul#affiliations').empty();
		$('ul#partners').empty();
		//make the new rows from data
		affiliations = data['current'];
		partners = data['other'];
		for (var i = 0; i < affiliations.length; i++) {
			$('ul#affiliations').append(
				$('<li><input id="affiliations-' + i + '" name="affiliations" value="' + affiliations[i][0] + '" type="checkbox"><label for="affiliations-' + i + '">' + affiliations[i][1] + '</label></li>')
				);
		}
		for (var i = 0; i < partners.length; i++) {
			$("ul#partners").append(
				$('<li><input id="partners-' + i + '" name="partners" value="' + partners[i][0] + '" type="checkbox"><label for="partners-' + i + '">' + partners[i][1] + '</label></li>')
				);
		}
	})
}


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
	update_partners();
})
