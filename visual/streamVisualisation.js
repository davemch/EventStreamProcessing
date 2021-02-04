
/* TODO:
        - Display Tooltip on Map
        - Clean / Structure Code
        - Bind Colors to Event-Code
        - Bind Size to Circle-Size
        - number of events per day


/*  This visualization was made possible by modifying code provided by:

Scott Murray, Choropleth example from "Interactive Data Visualization for the Web"
https://github.com/alignedleft/d3-book/blob/master/chapter_12/05_choropleth.html

Malcolm Maclean, tooltips example tutorial
http://www.d3noob.org/2013/01/adding-tooltips-to-d3js-graph.html

Mike Bostock, Pie Chart Legend
http://bl.ocks.org/mbostock/3888852  */

var parseDate = d3.timeParse("%Q");
var eruptionData = Array();
var eruption = 0;

var margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = 460 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

var svg = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", 700 + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

 var eruptionGraph = svg
     .append("g")
     .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var x = d3.scaleTime().range([0,700])


// Add Y axis
var y = d3.scaleLinear()    .domain([0, 2000])    .range([ height, 0 ]);


function drawGraph(svg ,data) {
    console.log(data);
    // Add X axis --> it is a date format

    x.domain(d3.extent(eruptionData, d => d.endDate))
    svg.append("g")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.axisBottom(x)
            .ticks(d3.time.week));

    svg.append("g")
        .call(d3.axisLeft(y));

    svg.append("text")
        .attr("x", (width / 4))
        .attr("y", 2)
        .attr("text-anchor", "middle")
        .style("font-size", "16px")
        .style("text-decoration", "underline")
        .text("Eruption");

    svg.append("path")
        .datum(data) //Welche Daten soll die Linie plotten?
        .attr("fill", "none")
        .attr("stroke", "red") //Farbe der Linie
        .attr("stroke-width", 1.5)
        .attr("d", d3.line()
            .x(function(d) { console.log(parseDate(d.endDate));
                return x(parseDate(d.endDate)) //Wert der x-Achse, ACHTUNG: Muss Date sein!
            })
            .y(function(d) {
                return y(d.amount) //Wert der y-Achse
            })
            .curve(d3.curveLinear)
        );
    //Zeichnen der Linie
    //addLine(svg, data, x, y, "red");
    //addLegend(svg, dataName, "red", 0)

    console.log(svg);

}

function addLegend(svg, dataName, color, offset) {
    svg.append("circle").attr("cx", 700).attr("cy", offset).attr("r", 4).style("fill", color);
    svg.append("text").attr("x", 700 - 120).attr("y", offset).text(dataName).style("font-size", "10px").attr("alignment-baseline","middle")
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


function addLine (svg, data, x, y, color) {
    svg.append("path")
        .attr("class", "line")
        .datum(data) //Welche Daten soll die Linie plotten?
        .attr("fill", "none")
        .attr("stroke", color) //Farbe der Linie
        .attr("stroke-width", 1.5)
        .attr("d", d3.line()
            .x(function(d) {
                return x(parseDate(d.date)) //Wert der x-Achse, ACHTUNG: Muss Date sein!
            })
            .y(function(d) {
                return y(d.sum) //Wert der y-Achse
            })
            .curve(d3.curveLinear)
        );
}

function updateGraph(data){
    console.log(eruptionGraph.select(".x.axis"));
    var updateContextData = eruptionGraph.selectAll("path").datum(eruptionData);
    console.log(updateContextData);
    updateContextData.enter().append("path")
        .style("stroke", "red")
        .merge(updateContextData)
        .attr("d", function(d) { return line(d); });
    updateContextData.exit().remove();

    eruptionGraph.select(".x.axis").call(d3.axisBottom(x));
    eruptionGraph.select(".y.axis").call(d3.axisLeft(y));
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
                if(splitData[0] === "eruption") {
                    if(eruptionData.length === 2) {
                        drawGraph(eruptionGraph,eruptionData);
                        eruptionData.push(value)
                    } else if(eruptionData.length > 2){
                        eruptionData.push(value);
                        updateGraph();
                    } else {
                        eruptionData.push(value);
                    }

                }
            }

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