
var parseDate = d3.timeParse("%Q");
var eruptionData = Array();
var eruption = 0;

var margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = 860 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

var svg = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", 1000 + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

 var eruptionGraph = svg
     .append("g")
     .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var x = d3.scaleTime().range([0,700]);
var xAxis = d3.axisBottom(x);
eruptionGraph.append("g")
    .attr("class", "myXaxis")
    .attr("transform", "translate(0," + height + ")")



// Add Y axis
var y = d3.scaleLinear() .range([ height, 0 ]);
var yAxis = d3.axisLeft().scale(y);
eruptionGraph.append("g")
    .attr("class", "myYaxis");

function drawGraph(svg ,data) {
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
    x.domain(d3.extent(eruptionData, d => d.endDate))
    svg.selectAll(".myXaxis").call(xAxis.ticks(d3.time.week));

    y.domain(d3.extent(eruptionData, d => d.amount))
    svg.selectAll(".myYaxis")
        .call(yAxis);

    svg.append("text")
        .attr("x", (width / 4))
        .attr("y", 2)
        .attr("text-anchor", "middle")
        .style("font-size", "16px")
        .style("text-decoration", "underline")
        .text("Eruption");

    svg
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

function updateGraph(data){

    eruptionGraph.selectAll("path.line").datum(eruptionData)
        .attr("d", function(d) { return line(d); });

    x.domain(d3.extent(eruptionData, d => d.endDate))
    svg.selectAll(".myXaxis").call(xAxis.ticks(d3.time.week));

    y.domain(d3.extent(eruptionData, d => d.amount))
    svg.selectAll(".myYaxis")
        .call(yAxis);

    console.log(eruptionData);
}

function webSocketInvoke() {
    if ("WebSocket" in window) {
        console.log("WebSocket is supported by your Browser!");
        var webSocket = new WebSocket("ws://localhost:8080/","echo-protocol");

        webSocket.onopen = function() {
            console.log("Connection created");
        };

        webSocket.onmessage = function (evt) {
            // from the socket connection

            var received_msg = evt.data;
            var value = JSON.parse(received_msg);
            var splitData = value.eventDescription.split("_")
            if(splitData.length > 1){
                if(splitData[0] === "eruption" && splitData[1] !== "WARNING") {
                    addValueToArray(value,eruptionData);
                    if(eruptionData.length === 2) {
                        drawGraph(eruptionGraph,eruptionData);
                    } else if(eruptionData.length > 2){
                        updateGraph(eruptionData);
                    }
                }
            }
            console.log(eruptionData);
        };
        webSocket.onclose = function() {
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