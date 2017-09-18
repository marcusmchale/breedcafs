update_tissues = function() {
	var request = $.ajax({
		type: 'GET',
		url: "/sample_reg/tissues/",
	});
	request.done(function(data){
		var tissues = [["","Select Tissue"]].concat(data).sort();
		$("#tissue").empty();
		for (var i = 0; i < tissues.length; i++) {
			$("#tissue").append(
				$("<option></option>").attr(
					"value", tissues[i][0]).text(tissues[i][1])
			);
		}
	});
}

$( window ).load(update_tissues)

//Disable submit on keypress "Enter" for all inputs boxes
$("input").keypress( function(e) {
	if (e.keyCode == 13) {
		e.preventDefault();	
	}
})

$("#submit_tissue").click( function(e) {
	e.preventDefault();
	$(".flash").remove();
	var submit_tissue = $.ajax({
			url: "/add_tissue",
			data: $("form").serialize(),
			type: 'POST',
			success: function(response) {
				flash = "<div id='tissue_flash' class='flash'> " + response + " </div>"
				$("#submit_tissue").after(flash)
			},
			error: function(error) {
				console.log(error);
			}
		});
	submit_tissue.done(update_tissues);
})

$("#date_collected").datepicker({ dateFormat: 'yy-mm-dd'});
$("#date_received").datepicker({ dateFormat: 'yy-mm-dd'});