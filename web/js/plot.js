
var myLineChart = null;
var myCorrChart = null;

function linear_approx(series) {
    var xs = [];
    for(var i =1; i<=series.length; i++) {
        xs.push(i);
    }

    return linear_approx_full(series, xs);
}
function linear_approx_full(series, xs) {
    var n = series.length;
    var xy_mean = 0;
    var x_mean = 0;
    var y_mean = 0;
    var x2_mean = 0;
    for(var i =0; i<n; i++) {
        xy_mean += xs[i]*series[i];
        y_mean += series[i];
        x_mean += xs[i];
        x2_mean += xs[i] * xs[i];
    }
    xy_mean = xy_mean / n;
    x_mean = x_mean / n;
    y_mean = y_mean /n;
    x2_mean = x2_mean /n;

    var b = (xy_mean - x_mean * y_mean) / (x2_mean - x_mean * x_mean);
    var a = y_mean - b * x_mean ;

    console.log("A: " + a + "; B: " + b + "; x_mean: " + x_mean + "; y_mean: " + y_mean);   
 
    var approx = []
    for(var i =0; i<n; i++) {
        approx.push(a + b * i);
    }

    return approx;
}

function loadGraph() {
    Chart.defaults.global.animation = false;
    Chart.defaults.global.responsive = true;

    Chart.defaults.global.scaleLabel = "<%='  ' + value%>";

    var dataTemplate = {
        labels: [],
        datasets: [
            {
                label: "",
                fillColor: "rgba(220,220,220,0.2)",
                strokeColor: "rgba(220,220,220,1)",
                pointColor: "rgba(220,220,220,1)",
                pointStrokeColor: "#fff",
                pointHighlightFill: "#fff",
                pointHighlightStroke: "rgba(220,220,220,1)",
                data: []
            },
            {
                label: "Скользящее среднее",
                fillColor: "rgba(151,187,205,0.2)",
                strokeColor: "rgba(151,187,205,1)",
                pointColor: "rgba(151,187,205,1)",
                pointStrokeColor: "#fff",
                pointHighlightFill: "#fff",
                pointHighlightStroke: "rgba(151,187,205,1)",
                data: []
            },
            {
                label: "Линейная аппроксимация",
                fillColor: "rgba(187,151,205,0.2)",
                strokeColor: "rgba(187,151,205,1)",
                pointColor: "rgba(151,187,205,1)",
                pointStrokeColor: "#fff",
                pointHighlightFill: "#fff",
                pointHighlightStroke: "rgba(151,187,205,1)",
                data: []
            }
        ]
    };


    // Get context with jQuery - using jQuery's .get() method.

    var wordToPlot = $("#wordSelector").val();
    var query = "/api/trend?word="+wordToPlot;
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
            for(var i=0; i<dataSeries.length; i++) {
               labels.push(dataSeries[i]["hour"].substr(4,10)); 
               data.push(dataSeries[i]["count"]); 
            } 
            dataTemplate["labels"] = labels;
            dataTemplate["datasets"][0]["label"] = wordToPlot;
            dataTemplate["datasets"][0]["data"] = data;
            dataTemplate["datasets"][1]["data"] = resp["movingAverage"];
            dataTemplate["datasets"][2]["data"] = linear_approx(data);
            
            var ctx = $("#wordsChart").get(0).getContext("2d");
            if (!(myLineChart === null)) {
                myLineChart.destroy();
            }
            myLineChart = new Chart(ctx).Line(dataTemplate, {pointHitDetectionRadius : 5,});

            $("#wordsChart").get(0).onclick = function(evt){
                var activePoints = myLineChart.getPointsAtEvent(evt);
                console.log(activePoints);
                console.log(evt);
                var pointLabel = activePoints[0]["label"];
                var ts1 = $("#timeSelector1").val();
                var ts2 = $("#timeSelector2").val();
                if( ts1 == "") {
                    $("#timeSelector1").val(pointLabel);
                } else if ($("#timeSelector2").val() == "") {
                    if (pointLabel > ts1) {
                        $("#timeSelector2").val(pointLabel);
                    } else {
                        $("#timeSelector1").val(pointLabel);
                        $("#timeSelector2").val(ts1);
                    }
                } else {
                    if (pointLabel > ts2) {
                        $("#timeSelector1").val($("#timeSelector2").val());
                        $("#timeSelector2").val(pointLabel);
                    } else {
                        $("#timeSelector1").val(pointLabel);
                    }
                }
                // => activePoints is an array of points on the canvas that are at the same position as the click event.
            };

        } catch (e){
            console.log(e);
        }
    })
    .fail(function() {
            console.log("Unknown error");
    });

    

}

function getWordData(wordToPlot) {
    var query = "/api/trend?word="+wordToPlot;
    if ($("#timeSelector1").val() != "") {
        query += "&time1=2015" + $("#timeSelector1").val();
    }
    if ($("#timeSelector2").val() != "") {
        query += "&time2=2015" + $("#timeSelector2").val();
    }
    
    var wordData = {};

    console.log(query);
    console.log(wordToPlot);
    $.ajax(query, {"async": false})
    .done(function( data ) {
        try{
            var resp = JSON.parse(data);
            wordData = resp;
        } catch (e){
            console.log(e);
        }
    })
    .fail(
    function() {
            console.log("Unknown error");
    });

    var wordDataMap = {};
    for(var i=0; i<wordData["dataSeries"].length; i++) {
        wordDataMap[wordData["dataSeries"][i]["hour"]] = wordData["dataSeries"][i]["count"];
    }

    return wordDataMap;

}

function loadCorr() {
    try {
        Chart.defaults.global.animation = false;
        Chart.defaults.global.responsive = true;

        Chart.defaults.global.scaleLabel = "<%='  ' + value%>";

        var dataTemplate = {
            labels: [],
            datasets: [
                {
                    label: "",
                    fillColor: "rgba(220,220,220,0.2)",
                    strokeColor: "rgba(220,220,220,1)",
                    pointColor: "rgba(220,220,220,1)",
                    pointStrokeColor: "#fff",
                    pointHighlightFill: "#fff",
                    pointHighlightStroke: "rgba(220,220,220,1)",
                    data: []
                },
                {
                    label: "",
                    fillColor: "rgba(187,151,205,0.2)",
                    strokeColor: "rgba(187,151,205,1)",
                    pointColor: "rgba(151,187,205,1)",
                    pointStrokeColor: "#fff",
                    pointHighlightFill: "#fff",
                    pointHighlightStroke: "rgba(151,187,205,1)",
                    data: []
                },
                {
                    label: "Линейная аппроксимация",
                    fillColor: "rgba(187,151,205,0.2)",
                    strokeColor: "rgba(187,151,205,1)",
                    pointColor: "rgba(151,187,205,1)",
                    pointStrokeColor: "#fff",
                    pointHighlightFill: "#fff",
                    pointHighlightStroke: "rgba(151,187,205,1)",
                    data: []
                }
            ]
        };


        var wordA = getWordData($("#wordSelectorA").val());
        var wordB = getWordData($("#wordSelectorB").val());

        var hoursMap = {};
        for(var k in wordA) hoursMap[k] = 1;
        for(var k in wordB) hoursMap[k] = 1;
        var hours = []
        for(var k in hoursMap) hours.push(k);
        hours.sort();
        var series = [];
        for(var i=0; i<hours.length; i++) {
            var a = 0;
            var b = 0;
            if (hours[i] in wordA) {
                a = wordA[hours[i]];
            } 
            if (hours[i] in wordB) {
                b = wordB[hours[i]];
            } 
            series.push([a,b]);
        }
        series.sort(function(x,y) {
            if (x[0] < y[0]) {
                return -1;
            }
            if (x[0] > y[0]) {
                return 1;
            }
            return 0;
        });

        var wordASeries = [];
        var wordBSeries = [];
        for(var i=0; i<series.length; i++) {
            wordASeries.push(series[i][0]);
            wordBSeries.push(series[i][1]);
        }

        dataTemplate["labels"] = wordASeries;
        dataTemplate["datasets"][0]["data"] = wordBSeries;
        dataTemplate["datasets"][1]["data"] = linear_approx_full(wordBSeries, wordASeries);

        
        var ctx = $("#corrChart").get(0).getContext("2d");
        if (!(myCorrChart === null)) {
            myCorrChart.destroy();
        }
        myCorrChart = new Chart(ctx).Line(dataTemplate, {pointHitDetectionRadius : 5,});
    } catch(e) {
        console.log(e);
    }
}
