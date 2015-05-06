var signedInUserEmail = null;

Handlebars.registerHelper('urlEscape', function(smth) {
  return encodeURIComponent(smth);
});

Handlebars.registerHelper('urlInsertLinks', function(smth) {
    var str = smth.replace(/http[^ ]+/g, function(str){ return '<a href="' + str +'" target="_blank">' +str + '</a>';});
    console.log(str);
    return str;
});


function getCurUrlParams() {
    var cur_url = "" + document.URL;
    var url_parts = cur_url.split("?");
    
    var params = {};
    if (url_parts.length > 1) {
        var param_parts = url_parts[1].split("&");
        for(var i =0 ; i< param_parts.length; i++) {
            var p = param_parts[i].split("=");
            params[p[0]] = p[1];
        }
    }
    
    return params;
}

function getCurUrlStringParam(param, default_val) {
    if (param in getCurUrlParams()) {
        return getCurUrlParams()[param];
    } else {
        return default_val;
    }
};

function getCurUrlIntParam(param, default_val) {
    var p_val = parseInt(getCurUrlParams()[param]);
    if (isNaN(p_val)) {
        return default_val;
    } else {
        return p_val;
    }
}

function get_max_trend(cluster) {
    var trend_vals = []
    for(var i=0; i<cluster["members"].length; i++) {
        trend_vals.push(parseFloat(cluster["members"][i]["trend"]));    
    }
    trend_vals.sort().reverse();

    return trend_vals[0];
}

function get_avg_trend(cluster) {
    var avg_trend = 0;
    for(var i=0; i<cluster["members"].length; i++) {
        avg_trend += parseFloat(cluster["members"][i]["trend"]);
    }
    avg_trend = avg_trend / cluster["members"].length;

    return avg_trend;
}

function set_trend_color(cluster) {
    var avg_trend = get_max_trend(cluster);

    var trend_class = "trend-unknown";
    var sign = "";
    if (avg_trend >0) {
        trend_class = "trend-raise";
        sign = "+";
    } else if (avg_trend < 0) {
        trend_class = "trend-fall";
    }
    
    cluster["avg_trend"] = sign +  avg_trend.toFixed(2);
    cluster["trend_class"] = trend_class;
}

function goBackOld() {
    var skip;
    skip = parseInt(getCurUrlParams()["skip"]);
    if (isNaN(skip)) {
        skip = 0;
    }
    window.location.assign("/?skip=" + parseInt(skip + 1))
}
function goBack() {
    var before = $("#current-timestamp").text();
    var lang = getCurUrlStringParam("lang","ru");
    window.location.assign("/?before=" + before + "&lang=" + lang);
}
function goNow() {
    var lang = getCurUrlStringParam("lang","ru");
    window.location.assign("/?lang=" + lang);
}


function compare_max_trend (a,b) {
    var a_val = get_max_trend(a);
    var b_val = get_max_trend(b);

    if (a_val < b_val) return -1;
    if (a_val > b_val) return 1;
    return 0;
}

function fill_cluster_properties(cl) {
    var cl2= [];
    for(var i=0; i<cl.length; i++) {
        var mems = $.map(cl[i]["members"], function(l) {
            return l["text"];
        })

        try {
            set_trend_color(cl[i]);
        } catch(e) {
            console.log(e);
        }

        cl[i]["query_string"] = mems.join("+").replace(/#/g,'');
        cl[i]["title_string"] = mems.join(" ");
        if ( cl[i]["members_len"] > 0) {
            cl2.push(cl[i]);
        }
    }

    return cl2;
}

function getApiRequest() {
    var skip = parseInt(getCurUrlParams()["skip"]);
    var before = getCurUrlParams()["before"];
    var date = getCurUrlParams()["date"];
   
    var request = "/api/cluster?"
    if (date && typeof date != 'undefined') {
        request += "date=" + date;
    } else if (before && typeof before !=  'undefined') {
        request += "before=" + before;
    } else if (!isNaN(skip)) {
        request += "skip=" + skip;
    }

    return request;
}

function parseResponse(resp){

    var cl = resp["clusters"];

    cl.sort(compare_max_trend).reverse();

    var topics = fill_cluster_properties(cl); 

    return topics;
}

function getLocalPageTranslateUrl(lang){
    var url = document.URL.replace(new RegExp("&?lang=[^&]*"), "");
    if (url.indexOf("?") > -1) {
        url += "&lang=" + lang;
    } else {
        url += "?lang=" + lang;
    }

    return url;
}

function loadTopicDebug() {
    
    var request = getApiRequest();
    var offset = getCurUrlIntParam("offset",0);
    var members_md5 = "";     
    var date = getCurUrlParams()["date"];

    $.get( request, function( data ) {
        try{
            var resp = JSON.parse(data);
            var topic = parseResponse(resp)[offset];
            topic["update_time"] = resp["update_time"];
            members_md5 = topic["members_md5"]
            
            topic["i18n"] = getI18n();

            var source = $("#cluster-template").html();
            var template = Handlebars.compile(source);

            $( "#cluster-holder" ).html( template(topic) );

            var shareUrl = "http://molva.spb.ru/?date=" + encodeURIComponent(resp["update_time"])+ "&offset=" + offset;
            var source2 = $("#shares-template").html();
            var template2 = Handlebars.compile(source2);
            $( "#shares-holder" ).html( template2({"shareUrl": shareUrl, "shareTitle": '"' + topic["title_string"] + '"'}) );

            $("#choose-lang").html(getI18n()["choose_lang"]);
        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
        $( "#cluster-holder" ).html("Произошла ошибка.")
    });

    $.get( '/api/relevant?date=' + date, function( data ) {
        try{
            var resp = JSON.parse(data);
            var tw_rel = null;
            console.log(members_md5);
            for(var i=0; i<resp.length; i++) {
                if (resp[i]["members_md5"] == members_md5) {
                    tw_rel = resp[i];
                    break;
                }
            }

            var source = $("#relevant-template").html();
            var template = Handlebars.compile(source);

            $( "#relevant-holder" ).html( template(tw_rel) );

        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
    });
}

function loadTopic() {
    var request = getApiRequest();
    var offset = getCurUrlIntParam("offset",0);

     $.get( request, function( data ) {
        try{
            var resp = JSON.parse(data);
            var topic = parseResponse(resp)[offset];
            topic["update_time"] = resp["update_time"];
            
            topic["i18n"] = getI18n();

            var source = $("#cluster-template").html();
            var template = Handlebars.compile(source);

            $( "#cluster-holder" ).html( template(topic) );

            var shareUrl = "http://molva.spb.ru/?date=" + encodeURIComponent(resp["update_time"])+ "&offset=" + offset;
            var source2 = $("#shares-template").html();
            var template2 = Handlebars.compile(source2);
            $( "#shares-holder" ).html( template2({"shareUrl": shareUrl, "shareTitle": '"' + topic["title_string"] + '"'}) );

            $("#choose-lang").html(getI18n()["choose_lang"]);
        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
        $( "#cluster-holder" ).html("Произошла ошибка.")
    });
   
}

function getI18n(){
    var lang = getCurUrlStringParam("lang","ru");
    if (lang == "en") {
        return {
            "lang": "en",
            "earlier": "Earlier",
            "now": "Now",
            "updated": "Last update",
            "lookup": "Search with...",
            "this_topic": "Show this topic only",
            "howto": {
                "caption": "Molva how-to",
                "first_line": "Pick topic from list",
                "second_line": "Click \"Search with...\" and select one",
                "search": "Check out",
                "in_twitter": "via Twitter search" 
            },
            "choose_lang": "<a href=\"" + getLocalPageTranslateUrl("ru") + "\">Ru</a>",
            "lookup_topic": "Search with:"
        };
    } else {
        return {
            "lang": "ru",
            "earlier": "Раньше",
            "now": "Сейчас",
            "updated": "Обновлено",
            "lookup": "В других источниках",
            "this_topic": "Только эта тема",
            "howto": {
                "caption": "Как пользоваться молвой",
                "first_line": "Выберите интересующую вас тему из списка",
                "second_line": "Нажмите \"Уточнить\", и выберите способ",
                "search": "Например, поищите",
                "in_twitter": "в Твиттере"
            },
            "choose_lang": "<a href=\"" + getLocalPageTranslateUrl("en") + "\">En</a>",
            "lookup_topic": "Уточнить:"
        };
    }
}

Handlebars.registerHelper('i18n', function(smth) {
    var i18n = getI18n();
    return  (smth in i18n ? i18n[smth] : "") ;
});

function loadClusters() {
    var request = getApiRequest();
    var limit = getCurUrlIntParam("limit",20);
    var offset = getCurUrlIntParam("offset",0);
    var lang = getCurUrlStringParam("lang","ru");

    console.log("loadClusters");
    $.get( request, function( data ) {
        try{
            var resp = JSON.parse(data);
            var cl2 = parseResponse(resp);
            var source = $("#cluster-template").html();
            var template = Handlebars.compile(source);
            var renderDoc = {
                "groups": cl2.slice(offset, offset + limit), 
                "update_time": resp["update_time"],
                "i18n": getI18n() 
            };

            $( "#cluster-holder" ).html( template(renderDoc) );

            var shareUrl = "http://molva.spb.ru/?date=" + encodeURIComponent(resp["update_time"])+ "&offset=" + offset + "&limit=" + limit;;
            var source2 = $("#shares-template").html();
            var template2 = Handlebars.compile(source2);
            $( "#shares-holder" ).html( template2({"shareUrl": shareUrl, "shareTitle": "Последние события в Twitter"}) );
            
            var chooseLangTemplate = "<a href=\"./?lang=en\">En</a>";

            $("#choose-lang").html(getI18n()["choose_lang"]);
        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
        $( "#cluster-holder" ).html("Произошла ошибка.")
    });
}

function signinCallback(authResult) {
  if (authResult['status']['signed_in']) {
    // Update the app to reflect a signed in user
    // Hide the sign-in button now that the user is authorized, for example:
    document.getElementById('signinButton').setAttribute('style', 'display: none');
    $('.quality-assess').each(function() {
      $(this).show();
    })
    gapi.client.load('plus', 'v1', function() {
      var request = gapi.client.plus.people.get({
        'userId': 'me'
      });
      request.execute(function(resp) {
        console.log(resp);
        signedInUserEmail = resp.emails[0]["value"]; 
        console.log(signedInUserEmail);
      });
    });
  } else {
    // Update the app to reflect a signed out user
    // Possible error values:
    //   "user_signed_out" - User is signed-out
    //   "access_denied" - User denied access to your app
    //   "immediate_failed" - Could not automatically log in the user
    console.log('Sign-in state: ' + authResult['error']);
  }
}

function sendQualityAssessment() {
    var forms = $('form.quality-assess');
    var marks = [];
    for(var i=0; i<forms.length; i++) {
        var vals = $(forms[i]).serializeArray();
        var mark = {};
        for(var j=0; j<vals.length; j++) {
            mark[vals[j]["name"]] = vals[j]["value"];
        }
        marks.push(mark);
    }
    var postData = {"update_time": $("#current-timestamp").text(), 
        "marks": marks,
        "username": signedInUserEmail,
        "experiment_name": $("#experiment_name").val(),
        "experiment_descr": $("#experiment_descr").val()
    };
    $.post("/api/mark_topic", JSON.stringify(postData))
    .done(function() {
        $('#marks-send-status').html("Отправлено");
    })
    .fail(function() {
        $('#marks-send-status').html("Ошибка при отправке");
        console.log("failed to POST to /api/mark_topic");
    });
}
