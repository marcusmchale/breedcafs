$("#weather_start").datepicker({ dateFormat: 'yy-mm-dd'});
$("#weather_end").datepicker({ dateFormat: 'yy-mm-dd'});
$("#controlled_environment_start").datepicker({ dateFormat: 'yy-mm-dd'});
$("#controlled_environment_end").datepicker({ dateFormat: 'yy-mm-dd'});

record_forms = $('#weather,#controlled_environment,#treatment,#input,#output,#labour,#transaction');
$('#tree_selection').hide();
$('#location').hide();
record_forms.hide()

show_record_form = function() {
    var record_form_type = this.value;
    record_forms.hide();
    $('#location').hide();
    $('#tree_selection').hide();
    if (record_form_type === 'weather') {
        $('#tree_selection').hide();
        $('#location').show();
        $('#block').hide()
    }
    if (record_form_type === 'controlled_environment') {
        $('#tree_selection').hide();
        $('#location').show();
        $('#block').show()
    }
    if (record_form_type === 'treatment') {
        $('#tree_selection').show();
        $('#location').show();
        $('#block').hide()
    }
    $('#'+record_form_type).show();
};

remove_flash = function() {
	$(".flash").remove();
}

$('#record_type').val('').change(show_record_form).change(remove_flash);

$('#submit_weather').click( function (e) {
    e.preventDefault();
    remove_flash();
    var wait_message = "Please wait for weather data to complete submission";
    var flash_wait = "<div id='weather_flash', class='flash'>" + wait_message + "</div>";
    $(this).parent().after(flash_wait);
    var data = $("form").serialize();
    var submit_weather_record = $.ajax({
        url: "/record/weather",
        data: data,
        type: 'POST',
        success: function(response) {
            if (response.hasOwnProperty('submitted')) {
                var flash_submitted = "<div id='weather_flash' class='flash'>" + response.submitted + "</div>";
                $("#weather_flash").replaceWith(flash_submitted);
            } else {
                $("#weather_flash").remove();
                    for (var i in response) {
                        for (var key in response[i]){
                            var flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                            $('#' + key).after(flash);
                        }
                    }
            }
        },
        error: function(error) {
			console.log(error);
		}
    })
});


$('#submit_controlled_environment').click( function (e) {
    e.preventDefault();
    remove_flash();
    var wait_message = "Please wait for controlled environment data to complete submission";
    var flash_wait = "<div id='controlled_environment_flash', class='flash'>" + wait_message + "</div>";
    $(this).parent().after(flash_wait);
    var data = $("form").serialize();
    var submit_controlled_environment_record = $.ajax({
        url: "/record/controlled_environment",
        data: data,
        type: 'POST',
        success: function(response) {
            console.log(response);
            if (response.hasOwnProperty('submitted')) {
                var flash_submitted = "<div id='controlled_environment_flash' class='flash'>" + response.submitted + "</div>";
                $("#controlled_environment_flash").replaceWith(flash_submitted);
            } else {
                $("#controlled_environment_flash").remove();
                    for (var i in response) {
                        for (var key in response[i]){
                            var flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                            $('#' + key).after(flash);
                        }
                    }
            }
        },
        error: function(error) {
			console.log(error);
		}
    })
});


update_treatment_categories = function() {
    var treatment_name =  $("#treatment_name").find(":selected").val();
    $("#treatment_category").empty();
    if (treatment_name !== "") {
        var request = $.ajax({
            type: 'GET',
            url: '/record/treatment_categories/' + treatment_name + '/',
            success: function(response){
                var categories = response
                $("#treatment_category").append($("<option></option>").attr("value", "").text("Select Category"))
                for (var i = 0; i< categories.length; i++) {
                    $("#treatment_category").append($("<option></option>").attr("value", categories[i]).text(categories[i]));
                }
            },
            error: function(response) {
            }
        })
    }
}


update_treatment_categories()

$("#treatment_name").change(update_treatment_categories)



$('#submit_treatment').click( function (e) {
    e.preventDefault();
    remove_flash();
    var wait_message = "Please wait for treatment assignment to complete";
    var flash_wait = "<div id='treatment_flash', class='flash'>" + wait_message + "</div>";
    $(this).parent().after(flash_wait);
    var data = $("form").serialize();
    var submit_treatment_record = $.ajax({
        url: "/record/treatment",
        data: data,
        type: 'POST',
        success: function(response) {
            console.log(response);
            if (response.hasOwnProperty('submitted')) {
                console.log(response);
                var flash_submitted = "<div id='treatment_flash' class='flash'>" + response.submitted + "</div>";
                $("#treatment_flash").replaceWith(flash_submitted);
            } else {
                $("#treatment_flash").remove();
                    for (i in response) {
                        for (var key in response[i]){
                            var flash = "<div id='flash_" + key + "' class='flash'>" + response[i][key][0] + "</div>";
                            $('#' + key).after(flash);
                        }
                    }
            }
        },
        error: function(error) {
			console.log(error);
		}
    })
});