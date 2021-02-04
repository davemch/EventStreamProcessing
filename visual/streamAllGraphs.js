
var parseDate = d3.timeParse("%Q");
var eruptionData = Array();
var appealData = Array();
var escalationData = Array();
var refuseData = Array();
var accusationData = Array();
var eruption = 0;

var margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = 860 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

var svgEruption = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

var eruptionGraph = svgEruption
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var svgEscalation = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

var escalationGraph = svgEscalation
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var svgAppeal = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

var appealGraph = svgAppeal
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var svgRefuse = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

var refuseGraph = svgRefuse
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var svgAccusation = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

var accusationGraph = svgAccusation
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var xEruption = d3.scaleTime().range([0,width]);
var xAxisEruption = d3.axisBottom(xEruption);
eruptionGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")

// Add Y axis
var yEruption = d3.scaleLinear() .range([ height, 0 ]);
var yAxisEruption = d3.axisLeft().scale(yEruption);
eruptionGraph.append("g")
    .attr("class", "myYaxis");

var xEscalation  = d3.scaleTime().range([0,width]);
var xAxisEscalation  = d3.axisBottom(xEscalation);
escalationGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")

var yEscalation = d3.scaleLinear() .range([ height, 0 ]);
var yAxisEscalation = d3.axisLeft().scale(yEscalation);
escalationGraph.append("g")
    .attr("class", "myYaxis");

var xAppeal = d3.scaleTime().range([0,width]);
var xAxisAppeal = d3.axisBottom(xAppeal);
appealGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")

var yAppeal = d3.scaleLinear() .range([ height, 0 ]);
var yAxisAppeal = d3.axisLeft().scale(yAppeal);
appealGraph.append("g")
    .attr("class", "myYaxis");

var xRefuse = d3.scaleTime().range([0,width]);
var xAxisRefuse = d3.axisBottom(xRefuse);
refuseGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")

var yRefuse = d3.scaleLinear() .range([ height, 0 ]);
var yAxisRefuse = d3.axisLeft().scale(yRefuse);
refuseGraph.append("g")
    .attr("class", "myYaxis");

var xAccusation = d3.scaleTime().range([0,width]);
var xAxisAccusation = d3.axisBottom(xAccusation);
accusationGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")

// Add Y axis
var yAccusation = d3.scaleLinear() .range([ height, 0 ]);
var yAxisAccusation = d3.axisLeft().scale(yAccusation);
accusationGraph.append("g")
    .attr("class", "myYaxis");

function drawGraph(graph ,data, xAxis, yAxis, x, y, name) {
    console.log(data);
    // Add X axis --> it is a date format
    /*
        var x = d3.scaleTime()
            .domain(d3.extent(eruptionData, d => d.endDate))
            .range([0,700])
        svg.append("g")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x)
                .ticks(d3.time.week));
        console.log(xAxis.ticks(d3.time.week));
        */
    x.domain(d3.extent(data, d => d.endDate))
    graph.selectAll(".myXaxis").call(xAxis.ticks(d3.time.week));

    y.domain(d3.extent(data, d => d.amount))
    graph.selectAll(".myYaxis")
        .call(yAxis);

    graph.append("text")
        .attr("x", (width / 4))
        .attr("y", 2)
        .attr("text-anchor", "middle")
        .style("font-size", "16px")
        .style("text-decoration", "underline")
        .text(name);

    graph
        .append("path")
        .datum(data)
        .attr("class", "line")//Welche Daten soll die Linie plotten?
        .attr("d", d3.line()
            .x(function(d) { console.log(x(parseDate(d.endDate)));
                return x(parseDate(d.endDate)) //Wert der x-Achse, ACHTUNG: Muss Date sein!
            })
            .y(function(d) {
                return y(d.amount) //Wert der y-Achse
            })
            .curve(d3.curveLinear))
        .attr("fill", "none")
        .attr("stroke", "red") //Farbe der Linie
        .attr("stroke-width", 1.5);
    //Zeichnen der Linie
    //addLine(svg, data, x, y, "red");
    //addLegend(svg, dataName, "red", 0)

}

var line = d3
    .line()
    .x(function(d) {
        return x(parseDate(d.endDate)) //Wert der x-Achse, ACHTUNG: Muss Date sein!
    })
    .y(function(d) {
        return y(d.amount) //Wert der y-Achse
    })
    .curve(d3.curveLinear)

function updateGraph(graph, svg, data, xAxis, yAxis, x, y){

    graph.selectAll("path.line").datum(data)
        .attr("d", function(d) { return line(d); });

    x.domain(d3.extent(data, d => d.endDate))
    graph.selectAll(".myXaxis").call(xAxis.ticks(d3.time.week));

    y.domain(d3.extent(data, d => d.amount))
    graph.selectAll(".myYaxis")
        .call(yAxis);

    console.log(data);
}

function webSocketInvoke() {
    if ("WebSocket" in window) {
        console.log("WebSocket is supported by your Browser!");
        var webSocket = new WebSocket("ws://localhost:8080/", "echo-protocol");

        webSocket.onopen = function () {
            console.log("Connection created");
        };

        webSocket.onmessage = function (evt) {
            // from the socket connection

            var received_msg = evt.data;
            var value = JSON.parse(received_msg);
            var splitData = value.eventDescription.split("_")
            if (splitData.length > 1 && splitData[1] !== "WARNING") {
                switch (splitData[0]) {
                    case "eruption":
                        addValueToArray(value, eruptionData);
                        if (eruptionData.length === 2) {
                            drawGraph(eruptionGraph, eruptionData, xAxisEruption, yAxisEruption, xEruption, yEruption, "eruption");
                        } else if (eruptionData.length > 2) {
                            updateGraph(eruptionGraph, svgEruption, eruptionData, xAxisEruption, yAxisEruption, xEruption, yEruption);
                        }
                        break;
                    case "appeal":
                        addValueToArray(value, appealData);
                        if (appealData.length === 2) {
                            drawGraph(appealGraph, appealData, xAxisAppeal, yAxisAppeal, xAppeal, yAppeal, "appeal");
                        } else if (appealData.length > 2) {
                            updateGraph(appealGraph, svgAppeal, appealData, xAxisAppeal, yAxisAppeal, xAppeal, yAppeal);
                        }
                        break;
                    case "refuse":
                        addValueToArray(value, refuseData);
                        if (refuseData.length === 2) {
                            drawGraph(refuseGraph, refuseData, xAxisRefuse, yAxisRefuse, xRefuse, yRefuse, "refuse");
                        } else if (refuseData.length > 2) {
                            updateGraph(refuseGraph, svgRefuse, refuseData, xAxisRefuse, yAxisRefuse, xRefuse, yRefuse);
                        }
                        break;
                    case "accusation":
                        addValueToArray(value, accusationData);
                        if (accusationData.length === 2) {
                            drawGraph(accusationGraph, accusationData,  xAxisAccusation, yAxisAccusation,xAccusation, yAccusation, "accusation");
                        } else if (accusationData.length > 2) {
                            updateGraph(accusationGraph, svgAccusation, accusationData, xAxisAccusation, yAxisAccusation, xAccusation, yAccusation);
                        }
                        break;
                    case "escalation":
                        addValueToArray(value, escalationData);
                        if (escalationData.length === 2) {
                            drawGraph(escalationGraph, escalationData,xAxisEscalation, yAxisEscalation, xEscalation, yEscalation, "escalation");
                        } else if (escalationData.length > 2) {
                            updateGraph(escalationGraph, svgEscalation, escalationData, xAxisEscalation, yAxisEscalation, xEscalation, yEscalation);
                        }
                        break;
                }
            }
        }
        webSocket.onclose = function () {
            console.log("Connection closed");
        };
    } else {
        alert("WebSocket NOT supported by your Browser!");
    }
}
webSocketInvoke();

function initGraph(data) {
    x.domain(d3.extent(data, function(d) { return d.endDate; }) );
    y.domain([0,1000]);

    eruptionGraph.append("g").attr("class", "x axis").attr("transform", "translate(0," + height + ")").call(d3.axisBottom(x));
    eruptionGraph.append("g").attr("class", "y axis").call(d3.axisLeft(y));
}

function addValueToArray (value, array) {
    if(array.length === 0) {
        array.push(value);
    }
    if(array[array.length - 1].endDate === value.endDate) {
        var int = parseInt(array[array.length - 1].amount);
        int += parseInt(value.amount);
        array[array.length - 1].amount = int.toString();
    } else {
        array.push(value);
    }
}