
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

var eruptionGraph = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", 700 + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)

function accumulate(array, data) {
    if(array.length === 0){
        console.log("ZERO")
    } else {
        array[0].push(data)
    }
    array.push(data)
}


function getDaily (data, event){
    var dates = d3.nest()
        .key(function(d) {return d.date; })
        .entries(data.filter (function (d) {
            return d.eventDescription === event
        }))
    console.log(dates);

    dates.forEach(function(date, index){
        var sum = 0;
        date.values.forEach(array => {
            sum += +array.numMentions;
        })
        dates[index].sum = sum;
    })

    return dates;
}

/**
 *
 * @param {Array} data
 * @param {Integer} time
 * @returns {Array}
 */
function averageData (data, time) {
    var mean = Array();
    var n= 0;
    var sum = 0;
    for(var i=0; i < data.length; i++){
        sum += data[i].sum;
        if(i !== 0 && (i % time === 0)) {
            mean[n] = {date: data[i].key, sum: sum/time}
            n++;
            sum = 0;
        }
    }

    return mean;
}


//READ THE DATA
d3.json("out.json", function(data) {
    console.log(data);
    //drawGraph(data, "accusation", 7);
    /*drawGraph(data ,"appeal", 7);
    drawGraph(data ,"eruption", 7);
    drawGraph(data ,"escalation", 7);
    drawGraph(data ,"refuse", 7);*/
})

function drawGraph(svg ,data) {

    svg
        .append("g")
        .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");

    // Add X axis --> it is a date format
    var x = d3.scaleTime()
        .domain(d3.extent(d => d.endDate))
        .range([0,700])
    svg.append("g")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.axisBottom(x)
            .ticks(d3.time.week));

    // Add Y axis
    var y = d3.scaleLinear()
        .domain([0, data.amount])
        .range([ height, 0 ]);
    svg.append("g")
        .call(d3.axisLeft(y));


    svg.append("text")
        .attr("x", (width / 4))
        .attr("y", 2)
        .attr("text-anchor", "middle")
        .style("font-size", "16px")
        .style("text-decoration", "underline")
        .text("Eruption");

    //Zeichnen der Linie
    addLine(svg, data, x, y, "red");
    //addLegend(svg, dataName, "red", 0)


}

function addLegend(svg, dataName, color, offset) {
    svg.append("circle").attr("cx", 700).attr("cy", offset).attr("r", 4).style("fill", color);
    svg.append("text").attr("x", 700 - 120).attr("y", offset).text(dataName).style("font-size", "10px").attr("alignment-baseline","middle")
}

function addLine (svg, data, x, y, color) {
    svg.append("path")
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

function updateGraph() {
    console.log(eruptionData);
    var updateContextData = eruptionGraph.selectAll("path.line").data(eruptionData);
    updateContextData.enter().append("path").attr("class", "line")
        //.style("stroke", function(d) { return color(d.name); })
        .attr("clip-path", "url(#clip)")
        .merge(updateContextData)
        .attr("d", function(d) { return line(d.values); });
    updateContextData.exit().remove();
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
                    if(eruption === 0) {
                        drawGraph(eruptionGraph,value);
                        eruption = 1;
                        eruptionData.push(value);
                    } else {
                        eruptionData.push(value)
                        updateGraph();
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

