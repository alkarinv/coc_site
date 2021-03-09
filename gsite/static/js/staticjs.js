/// flash error message
function flash(msg){
    $('#flash').show().html(msg);
    $("#flash").fadeOut(5000);
}
