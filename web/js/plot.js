
function linear_approx(series, seriesLabel) {
    var n = series.length;
    var xy_mean = 0;
    var x_mean = 0;
    var y_mean = 0;
    var x2_mean = 0;
    var x_min = series[0][0];
    for(var i =0; i<n; i++) {
        if (series[i][0] < x_min) {
            x_min = series[i][0];
        }
    }
    for(var i =0; i<n; i++) {
        var x = (series[i][0] - x_min) / 3600;
        var y = series[i][1];
        xy_mean += x * y;
        y_mean += y;
        x_mean += x;
        x2_mean += x * x;
    }
    xy_mean = xy_mean / n;
    x_mean = x_mean / n;
    y_mean = y_mean /n;
    x2_mean = x2_mean /n;

    var b = (xy_mean - x_mean * y_mean) / (x2_mean - x_mean * x_mean);
    var a = y_mean - b * x_mean ;

    console.log("A: " + a + "; B: " + b + "; x_mean: " + x_mean + "; y_mean: " + y_mean);   
 
    var approx_series = []
    for(var i =0; i<n; i++) {
        var approx_x = (series[i][0] - x_min) / 3600;
        var approx_y = a + b * approx_x;
        approx_series.push([series[i][0], approx_y]);
    }

    return {"data": approx_series, "approx_slope": b.toFixed(2), "approx_add": a, 
        "shadowSize": 0
    };
}

function changeGraph() {
    var word = $("#wordSelector").val();
    var trend = $("#trend-checkbox").prop('checked');
    if (word != undefined) {
        var url = '/graph?word=' + encodeURIComponent(word);
        if (trend) {
            url += '&trend';
        }
        window.location.assign(url);
    }
}

function getTimeLabel(time_hour) {
    var month = time_hour.substr(4,2);
    var day   = time_hour.substr(6,2);
    var hour  = time_hour.substr(8,2);

    var label = month +"." + day + " " + hour + ":00";

    return label;
}

function sumSeries(series) {
    var sum = 0;
    for(var i=0; i<series.length; i++) {
        sum += series[i][1];
    }
    return sum;
}

function compareSeries (a,b) {
    var a_val = a["sumData"];
    var b_val = b["sumData"];

    if (a_val < b_val) return -1;
    if (a_val > b_val) return 1;
    return 0;
}

function makeTooltipHandler(labelsLong) {

    return function tooltipHandler (event, pos, item) {
        if (item) {
            var x = item.datapoint[0],
                y = item.datapoint[1];

            var tooltipText = "";
            var pointLabel = item.series.label;
            if ("approx_label" in item.series) {
                tooltipText = item.series["approx_label"];
            } else {
                var times = "раз";
                var lastDigit = String(y).substr(-1, 1);
                if ( lastDigit == "2" || lastDigit == "3" || lastDigit == "4") {
                    times = "раза";
                }
                tooltipText = '"' + pointLabel + '"' + " " + labelsLong[x] + "<br/> " + y + " " + times
            }

            $("#tooltip").html(tooltipText)
                .css({top: item.pageY+15, left: item.pageX+15})
                .fadeIn(200);
        } else {
            $("#tooltip").hide();
        }
    }
}

function addWordAndPlot(wordToPlot, wordSurface, color, plotDataSeries, showApprox, labelsLong, normalize) {
    wordToPlot = decodeURIComponent(wordToPlot).trim();

    var encodedWord = encodeURIComponent(wordToPlot.toLowerCase().replace('#',''));

    var query = "/api/trend?word=" + encodedWord;
    if ($("#timeSelector1").val() != "") {
        query += "&time1=2015" + $("#timeSelector1").val();
    }
    if ($("#timeSelector2").val() != "") {
        query += "&time2=2015" + $("#timeSelector2").val();
    }

    $.get(query, function( data ) {
        try{
            var resp = JSON.parse(data);
            var dataSeries = resp["dataSeries"];
            var labels = [];
            var data = [];
            var tickDist = Math.round(dataSeries.length / 5);
            var maxCnt = 1; //artificial 
            for(var i=0; i<dataSeries.length; i++) {
                var label = getTimeLabel(dataSeries[i]["hour"]);
                var unixtime = parseFloat(dataSeries[i]["utc_unixtime"]);

                if (i % tickDist == 0) {
                    labels.push([unixtime, label]); 
                }
                labelsLong[unixtime] = label;
                data.push([unixtime, dataSeries[i]["count"]]); 
                if (dataSeries[i]["count"] > maxCnt) {
                    maxCnt = dataSeries[i]["count"];
                }
            } 
            if (normalize) {
                //var exp_sum = 0;
                //for(var i=0; i<data.length; i++) {
                //    exp_sum += Math.exp(data[i][1] / maxCnt)
                //}
                //var min_exp = exp_sum;
                //for(var i=0; i<data.length; i++) {
                //    data[i][1] = Math.exp(data[i][1] / maxCnt) / exp_sum;
                //    if (data[i][1] < min_exp) {
                //        min_exp = data[i][1];
                //    }
                //    //data[i][1] = (data[i][1] / maxCnt) * 100;
                //}
                for(var i=0; i<data.length; i++) {
                    //data[i][1] = (data[i][1] - min_exp) * 100;
                    data[i][1] = (data[i][1] / maxCnt) * 100;
                }
            }
            var sumData = sumSeries(data);
            var approx = linear_approx(data, wordToPlot);
            approx["color"] = color;
            approx["sumData"] = sumData + 1;
            if (showApprox) {
                plotDataSeries.push(approx);
                wordSurface = wordSurface + " (trend: " + approx["approx_slope"] + ")";
            }
            approx["approx_label"] = wordSurface; 

            plotDataSeries.push({"label": wordSurface, "data": data, "color": color,
                bars: {
                    show: true,
                    barWidth: 3600, //hour
                    fill: true,
                    fillColor: { colors: [ { opacity: 0.7 }, { opacity: 0.3 } ] }
                }, 
                sumData: sumData
            });

            plotDataSeries.sort(compareSeries).reverse();

            $.plot("#wordsChart", plotDataSeries, {
                yaxis: {
                    min: 0
                },
                xaxis: {
                    ticks: labels 
                },
                grid: {
                    hoverable: true,
                    borderWidth: 0
                },
                legend: {
                    margin: 20,
                    position: "nw"
                } 
            });
            

            $("#wordsChart").bind("plothover", makeTooltipHandler(labelsLong));
        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
            console.log("Unknown error");
    });

}

function loadGraph() {

    $('#wordSelector').bind('keypress',function (event){
      if (event.keyCode === 13){
        $("#change-graph-btn").trigger('click');
      }
    });

    // Get context with jQuery - using jQuery's .get() method.

    var bigWordToPlot = getCurUrlParams()["word"];
    var bigSurfaceWordToPlot = getCurUrlParams()["surface"];
    if (bigWordToPlot == undefined) {
        return;
    }
    if (bigSurfaceWordToPlot == undefined) {
        bigSurfaceWordToPlot = bigWordToPlot;
    }

    bigWordToPlot = decodeURIComponent(bigWordToPlot).trim();
    $("#wordSelector").val(bigWordToPlot);
    console.log(bigWordToPlot);

    var showTrend = "trend" in getCurUrlParams();
    $("#trend-checkbox").prop('checked', showTrend);

    var normalize = "normalize" in getCurUrlParams();
    
    var wordsToPlot = bigWordToPlot.split(' ');
    bigSurfaceWordToPlot = decodeURIComponent(bigSurfaceWordToPlot).trim(); 
    var surfacesToPlot = bigSurfaceWordToPlot.split(' ');
    var plotDataSeries = [];
    var labelsLong = {};

    for(var k=0; k<wordsToPlot.length && k < 5; k++) {
        if (wordsToPlot[k] == "") {
            continue;
        }
        var surface = decodeURIComponent(surfacesToPlot[k]);
        addWordAndPlot(wordsToPlot[k], surface, k, plotDataSeries, showTrend, labelsLong, normalize); 
    }

}


