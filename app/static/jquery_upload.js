const upload_button = $('#upload_button');
const chart = $('.chart');



const r = new Resumable({
	target: 'resumable_upload',
	maxFiles: 1,
    simultaneousUploads: 1,
    fileType: ['csv', 'xlsx']
});

remove_flash = function() {
	$(".flash").remove();
};

if(!r.support){
	 remove_flash();
	const flash = "<div id='upload_submit_flash' class='flash'><p>'Please use an up-to-date browser'</p></div>";
	upload_button.after(flash)
}

r.assignBrowse(upload_button);
r.assignDrop(upload_button);

r.on('fileAdded', function(file) {
    remove_flash();
    $('#error_table_div').empty();
    const wait_message = "Please wait for file to complete upload and assembly";
    const flash_wait = "<div id='upload_submit_flash' class='flash'>" + wait_message + "</div>";
    upload_button.after(flash_wait);
    r.upload();
    const progress = Math.floor(file.progress() * 100);
    const progress_div = "<div id='upload_progress_flash' class='flash'><a id='progress'>" + progress + "</a><a>%</a></div>";
    upload_button.after(progress_div);
    const progress_flash = $('#upload_progress_flash')
    progress_flash.append(
	    "<br><a>Click to pause/resume</a>"
    );
    progress_flash.click(function() {
        if (r.isUploading()) {
            r.pause()
        } else {
            r.upload()
        }
    })
});



r.on('fileProgress', function (file) {
    const progress = Math.floor(file.progress() * 100);
    $('#progress').html(progress);
});



r.on('fileError', function(file, message){
	remove_flash();
	console.log(message);
    const flash_wait = "<div id='upload_submit_flash' class='flash'>" + message + "</div>";
    upload_button.after(flash_wait);
});

r.on('fileSuccess', function(file, message){
    console.log(message)
    const total_chunks = JSON.parse(message)['total_chunks'];
    const complete = JSON.parse(message)['status'];
    if (complete) {
        $.ajax({
            url: "assemble_upload",
            data: $("form").serialize()
                + "&fileName=" + file.fileName
                + "&uniqueIdentifier=" + file.uniqueIdentifier
                + "&size=" + file.size
                + "&total_chunks=" + total_chunks,
            type: 'POST',
            success: function (response) {
                console.log(response);
                //console.log('assembled?');
                remove_flash();
                if (['csv', 'xlsx'].indexOf(file.fileName.split('.').pop()) > -1) {
                    $.ajax({
                        url: "/upload_submit",
                        data: $("form").serialize() + "&filename=" + file.fileName,
                        type: 'POST',
                        success: function (response) {
                            console.log(response);
                            if (response.hasOwnProperty('status')) {
                                if (response.status === 'ERRORS') {
                                    remove_flash();
                                    upload_button.after("<div id='response' style='background:#f0b7e1' class='flash'></div>");
                                    $('#response').append("<p>Errors were found in the uploaded file:</p>");
                                    $('#response').append('<div>' + response.result + '</div>');
                                } else if (response.status === 'SUCCESS') {
                                    let flash_submitted = "<div id='upload_submit_flash' class='flash'>" + response.result + " </div>";
                                    upload_button.after(flash_submitted);
                                    if (response.hasOwnProperty('task_id')) {
                                        poll(response.task_id);
                                    }
                                }
                            } else {
                                for (var key in response) {
                                    if (response.hasOwnProperty(key)) {
                                        let flash = "<div id='flash_" + key + "' class='flash'>" + response[key][0] + "</div>";
                                        $('#' + key).after(flash);
                                    }
                                }
                            }
                        },
                        error: function (error) {
                        }
                    });

                    //poll for result of submission
                    function poll(task_id) {
                        setTimeout(function () {
                            $.ajax({
                                type: 'GET',
                                url: "/status/" + task_id + "/",
                                success: function (response) {
                                    if (response.hasOwnProperty('status')) {
                                        //flash_status = "<div id='upload_submit_flash' class='flash'> " + response.status + "</div>";
                                        //$("#upload_submit_flash").replaceWith(flash_status);
                                        if (response.status === 'PENDING') {
                                            poll(task_id)
                                        }
                                        ;
                                        if (response.status === 'RETRY') {
                                            remove_flash();
                                            upload_button.after("<div id='upload_submit_flash' class='flash'></div>");
                                            const message = "<p>Your file will be processed as soon as the database becomes available</p>"
                                            $('#upload_submit_flash').append(message);
                                            poll(task_id);
                                        }
                                        if (response.status === 'ERRORS') {
                                            remove_flash();
                                            upload_button.after("<div id='response' style='background:#f0b7e1' class='flash'></div>");
                                            $('#response').append("<p>Errors were found in the uploaded file:</p>");
                                            $('#response').append('<div>' + response.result + '</div>');
                                        }
                                        ;
                                        if (response.status === 'SUCCESS') {
                                            remove_flash();
                                            upload_button.after("<div id='response' class='flash'></div>")
                                            $('#response').append(response.result.result);
                                            load_graph("/json_submissions");
                                        }
                                        ;
                                    }
                                }
                            });
                        }, 1000);
                    }
                } else {
                    upload_button.after("<div id='response' style='background:#f0b7e1' class='flash'></div>")
                    $('#response').append('File received but no further action was taken')
                }
            },
            error: function (error) {
                console.log(error)
            }
        });
    } else {
        remove_flash();
        upload_button.after("<div id='response' style='background:#f0b7e1' class='flash'></div>");
        $('#response').append("<p>An error occurred and the server did not get the whole file, please try again</p>");
    }
});
