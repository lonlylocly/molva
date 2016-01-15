var signedInUserEmail = null;

Handlebars.registerHelper('urlEscape', function(smth) {
  return encodeURIComponent(smth);
});
Handlebars.registerHelper('urlDoubleEscape', function(smth) {
  return encodeURIComponent(encodeURIComponent(smth));
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
    var max_trend = 0;
    for(var i=0; i<cluster["members"].length; i++) {
        if (cluster["members"][i]["trend"] > max_trend) {
            max_trend = cluster["members"][i]["trend"];
        }
    }

    return max_trend;
}

function get_avg_trend(cluster) {
    var avg_trend = 0;
    for(var i=0; i<cluster["members"].length; i++) {
        avg_trend += parseFloat(cluster["members"][i]["trend"]);
    }
    avg_trend = avg_trend / cluster["members"].length;

    return avg_trend;
}

// because of Tomita imperfection same source of word can be with different lemmas
function filter_duplicates(cluster) {
    var words = {}
    var mems = []
    for(var i=0; i<cluster["members"].length; i++) {
        if (!(cluster["members"][i]["text"] in words)) {
            mems.push(cluster["members"][i]);
            words[cluster["members"][i]["text"]] = true;
        }
    }
    if (mems.length < cluster["members"].length) {
        console.info("Filtered several words for topic " + cluster["gen_title"])
        cluster["members"] = mems;
        var gen_title = ""
        for(var i=0; i<mems.length ; i++) {
            gen_title += mems[i]["text"];
            if (i != mems.length -1 ) {
                gen_title += " ";
            }
        }
        cluster["gen_title"] = gen_title;
    }
}

function set_trend_color(cluster, cluster_id) {
    var avg_trend = get_avg_trend(cluster);

    var threshold_high = 0.5;
    var threshold_medium = 0.1;

    if (cluster_id < "20160109111303") {
        threshold_high = 15;
        threshold_medium = 3;
    }

    var trend_class = "trend-unknown";
    var sign = "";
    if (avg_trend >0) {
        trend_class = "trend-raise";
        sign = "+";
    } else if (avg_trend < 0) {
        trend_class = "trend-fall";
    }
    var trend_text = "обычное";
    if (avg_trend > threshold_high) {
        trend_text = "важное";
        trend_class = "trend-raise-important";
    } else if (avg_trend > threshold_medium ) {
        trend_text = "новое";
    }
    
    cluster["avg_trend"] = sign +  avg_trend.toFixed(2);
    cluster["trend_class"] = trend_class;
    cluster["trend_text"] = trend_text;
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
    var a_val = get_avg_trend(a);
    var b_val = get_avg_trend(b);

    if (a_val < b_val) return -1;
    if (a_val > b_val) return 1;
    return 0;
}

function makeGraphLink(members) {
    var texts = [];
    var lemmas = [];
    for(var i=0; i<members.length; i++) {
        texts.push(encodeURIComponent(encodeURIComponent(members[i]["text"])));
        lemmas.push(encodeURIComponent(members[i]["stem_text"]));
    }

    return "/graph?word=" + lemmas.join('%20') + "&surface=" + texts.join('%20') + "&trend";
}

function fill_cluster_properties(cl, cluster_id) {
    var cl2= [];
    for(var i=0; i<cl.length; i++) {
        var mems = $.map(cl[i]["members"], function(l) {
            return l["text"];
        })

        try {
            set_trend_color(cl[i], cluster_id);
        } catch(e) {
            console.log(e);
        }
        try {
            filter_duplicates(cl[i], cluster_id);
        } catch(e) {
            console.log(e);
        }


        cl[i]["query_string"] = mems.join("+").replace(/#/g,'');
        cl[i]["title_string"] = mems.join(" ");
        cl[i]["graph_link"] = makeGraphLink(cl[i]["members"]);
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
    return _parseResponse(resp, true)
}

function _parseResponse(resp, doFilter){

    var cl = resp["clusters"];

    cl.sort(compare_max_trend).reverse();

    var cl2 = [];

    if (doFilter) {
        for(var i=0; i<cl.length; i++) {
            if (!("topic_density" in cl[i]) || cl[i]["topic_density"] > 3) {
                cl2.push(cl[i]);
            }
        }
    } else {
        cl2 = cl;
    }
    var topics = fill_cluster_properties(cl2, resp["cluster_id"]); 

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

function loadTopicV2() {
    
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

            var shareUrl = "http://molva.spb.ru/topic?date=" + encodeURIComponent(resp["update_time"])+ "&offset=" + offset;
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

function getTweetPositions(tw_len, cols) {
    var tweet_positions = [];
    var j = 0;
    var a = 0;
    for(var i=0; i<tw_len; i++) {
        if ((a + j * cols) >= tw_len) {
            a += 1;
            j = 0
            tweet_positions.push(-1);
        }
        tweet_positions.push(a + j * cols);
        j += 1;
    }
    return tweet_positions;
}

function loadTopicV3() {
    
    var request = getApiRequest();
    var offset = getCurUrlIntParam("offset",0);
    var members_md5 = "";     
    var date = getCurUrlParams()["date"];
    if (date < "2015-10-24 00:16:43") {
        loadTopicV2();
        console("load version 0.1");
        return;
    }

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

            var shareUrl = "http://molva.spb.ru/topic?date=" + encodeURIComponent(resp["update_time"])+ "&offset=" + offset;
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
            var tweet_positions = getTweetPositions(tw_rel["tweets"].length, 4) 
            var tw_text = '<div class="row">';
            tw_text += '<div class="col-md-3">';
            for(var i=0; i<tweet_positions.length; i++) {
                var j = tweet_positions[i];
                if (j==-1) {
                    tw_text += '</div><div class="col-md-3">';
                    continue;
                }
                tw_text += tw_rel["tweets"][j]["embed_html"];
            }
            tw_text += '</div></div>';
            //var source = $("#relevant-template").html();
            //var template = Handlebars.compile(source);

            $( "#relevant-holder" ).html( tw_text);

        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
        $( "#relevent-holder" ).html("Произошла ошибка.")
    });
}


function getI18n(){
    var lang = getCurUrlStringParam("lang","ru");
    if (lang == "en") {
        return {
            "lang": "en",
            "earlier": "Earlier",
            "now": "Now",
            "updated": "Generated by computer algorithm from Twitter messages stream. Update time ",
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
            "updated": "Сгенерировано компьютерным алгоритмом на основании сообщений Twitter. Актуально на ",
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
    _loadClusters(true);
}

function loadClustersDebug() {
    _loadClusters(false);
}

function _loadClusters(doFilter) {
    var request = getApiRequest();
    var limit = getCurUrlIntParam("limit",20);
    var offset = getCurUrlIntParam("offset",0);
    var lang = getCurUrlStringParam("lang","ru");

    console.log("loadClusters");
    $.get( request, function( data ) {
        try{
            var resp = JSON.parse(data);
            var cl2 = _parseResponse(resp, doFilter);
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
