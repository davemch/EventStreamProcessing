
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

var margin = {top: 10, right: 30, bottom: 30, left: 60},
width = 460 - margin.left - margin.right,
height = 400 - margin.top - margin.bottom;

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

  drawGraph(data ,"refuse", 7);


  var svg2 = d3.select("#my_dataviz")
  .append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .call(d3.zoom().on("zoom", function () {
    svg2.attr("transform", d3.event.transform)
  }))
  .append("g")
  .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

  // Add X axis --> it is a date format
  var x = d3.scaleTime()
    .domain(d3.extent(data, d => d.date))
    .range([0, width]);
    svg2.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

// Add Y axis
var y = d3.scaleLinear()
  .domain([0, 3000])
  .range([ height, 0 ]);
svg2.append("g")
  .call(d3.axisLeft(y));

var weeklyAccusation = data.filter(function(d) {
  return d.hasOwnProperty("amount") && d.eventDescription === "accusation_aggregate"
});

var weeklyEruption = data.filter(function(d) {
  return d.hasOwnProperty("amount") && d.eventDescription === "eruption_aggregate"
});

var weeklyAppeal = data.filter(function(d) {
  return d.hasOwnProperty("amount") && d.eventDescription === "appeal_aggregate"
});

var weeklyRefuse = data.filter(function(d) {
  return d.hasOwnProperty("amount") && d.eventDescription === "refuse_aggregate"
});

var weeklyEscalation = data.filter(function(d) {
  return d.hasOwnProperty("amount") && d.eventDescription === "escalation_aggregate"
});

svg2.append("path")
  .datum(weeklyAccusation)
  .attr("fill", "none")
  .attr("stroke", "steelblue")
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
      return x(parseDate(d.endDate))
    })
    .y(function(d) { 

      return y(d.amount) 
    })
    .curve(d3.curveLinear)
    );

svg2.append("path")
  .datum(weeklyEruption)
  .attr("fill", "none")
  .attr("stroke", "yellow")
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
        return x(parseDate(d.endDate))
    })
    .y(function(d) { 
        return y(d.amount) 
    })
    .curve(d3.curveLinear)
    );

    svg2.append("path")
  .datum(weeklyAppeal)
  .attr("fill", "none")
  .attr("stroke", "green")
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
        return x(parseDate(d.endDate))
    })
    .y(function(d) { 
        return y(d.amount) 
    })
    .curve(d3.curveLinear)
    );

    svg2.append("path")
  .datum(weeklyRefuse)
  .attr("fill", "none")
  .attr("stroke", "brown")
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
        return x(parseDate(d.endDate))
    })
    .y(function(d) { 
        return y(d.amount) 
    })
    .curve(d3.curveLinear)
    );

    svg2.append("path")
  .datum(weeklyEscalation)
  .attr("fill", "none")
  .attr("stroke", "red")
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
        return x(parseDate(d.endDate))
    })
    .y(function(d) { 
        return y(d.amount) 
    })
    .curve(d3.curveLinear)
    );

    svg2.append("text")
        .attr("x", (width / 4))             
        .attr("y", 2)
        .attr("text-anchor", "middle")  
        .style("font-size", "16px") 
        .style("text-decoration", "underline")  
        .text("All Events Weekly Avg.");

      svg2.append("circle").attr("cx",width).attr("cy", 0).attr("r", 4).style("fill", "steelblue");
      svg2.append("text").attr("x", width - 100).attr("y", 0).text("Weekly Accusation").style("font-size", "10px").attr("alignment-baseline","middle")
      svg2.append("circle").attr("cx",width).attr("cy", 10).attr("r", 4).style("fill", "brown");
      svg2.append("text").attr("x", width - 100).attr("y", 10).text("Weekly Refuse").style("font-size", "10px").attr("alignment-baseline","middle")
      svg2.append("circle").attr("cx",width).attr("cy", 20).attr("r", 4).style("fill", "green");
      svg2.append("text").attr("x", width - 100).attr("y", 20).text("Weekly Appeal").style("font-size", "10px").attr("alignment-baseline","middle")
      svg2.append("circle").attr("cx",width).attr("cy", 30).attr("r", 4).style("fill", "red");
      svg2.append("text").attr("x", width - 100).attr("y", 30).text("Weekly Escalation").style("font-size", "10px").attr("alignment-baseline","middle")
      svg2.append("circle").attr("cx",width).attr("cy", 40).attr("r", 4).style("fill", "yellow");
      svg2.append("text").attr("x", width - 100).attr("y", 40).text("Weekly Eruption").style("font-size", "10px").attr("alignment-baseline","middle")

/* EXAMPLE GRAPH */
  //Grundgerüst vom Graph
  var dates = getDaily(data,"refuse")
  var svgtest = d3.select("#my_dataviz")
  .append("svg")
  .attr("width", 1080 + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g")
  .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

  // Add X axis --> it is a date format
  var x = d3.scaleTime()
    .domain(d3.extent(dates, d => d.key))
    .range([0, 1080])
  svgtest.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x).ticks(d3.time.week));

  // Add Y axis
  var y = d3.scaleLinear()
    .domain([1000, 3000])
    .range([ height, 0 ]);
  svgtest.append("g")
    .call(d3.axisLeft(y));

  //Zeichnen der Linie
  svgtest.append("path")
  .datum(dates) //Welche Daten soll die Linie plotten?
  .attr("fill", "none")
  .attr("stroke", "steelblue") //Farbe der Linie
  .attr("stroke-width", 1.5)
  .attr("d", d3.line()
    .x(function(d) {
      return x(parseDate(d.key)) //Wert der x-Achse, ACHTUNG: Muss Date sein! 
    })
    .y(function(d) { 
      return y(d.sum) //Wert der y-Achse
    })
    .curve(d3.curveLinear)
    );

    svgtest.append("circle").attr("cx",width).attr("cy", 0).attr("r", 4).style("fill", "steelblue");
    svgtest.append("text").attr("x", width - 100).attr("y", 0).text("Daily Refuse").style("font-size", "10px").attr("alignment-baseline","middle")

})


function drawGraph(data, dataName, time) {

  var dailyData = getDaily(data, dataName)
  var svg = d3.select("#my_dataviz")
  .append("svg")
  .attr("width", 1080 + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g")
  .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

  // Add X axis --> it is a date format
  var x = d3.scaleTime()
    .domain(d3.extent(averageData(dailyData, time), d => d.date))
    .range([0, 1080])
    svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x).ticks(d3.time.week));

  // Add Y axis
  var y = d3.scaleLinear()
    .domain([0, 10000])

  //.domain([d3.min(averageData(dailyData, time), d => d.sum), d3.max(averageData(dailyData, time), d => d.sum)])
    .range([ height, 0 ]);
    svg.append("g")
    .call(d3.axisLeft(y));

  //Zeichnen der Linie
    addLine(svg, averageData(dailyData ,time) , x, y, "red");
    addLine(svg, averageData(getDaily(data, "appeal") ,time) , x, y, "blue");
    addLine(svg, averageData(getDaily(data, "accusation") ,time) , x, y, "green");
    addLine(svg, averageData(getDaily(data, "escalation") ,time) , x, y, "orange");
    addLine(svg, averageData(getDaily(data, "eruption") ,time) , x, y, "yellow");
    addLegend(svg, "refuse", time, "red",0)
    addLegend(svg, "accusation", time, "blue",10)
    addLegend(svg, "escalation", time, "green",20)
    addLegend(svg, "eruption", time, "orange",30)
    addLegend(svg, "appeal", time, "yellow",40)

}

function addLegend(svg, dataName, time, color, offset) {
  svg.append("circle").attr("cx",width).attr("cy", offset).attr("r", 4).style("fill", color);
  svg.append("text").attr("x", width - 100).attr("y", offset).text(time + " Day Mean" + dataName).style("font-size", "10px").attr("alignment-baseline","middle")
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
function drawMap() {
  //Width and height of map
var width = 960;
var height = 500;

// D3 Projection
var projection = d3.geo.albersUsa()
				   .translate([width/2, height/2])    // translate to center of screen
				   .scale([1000]);          // scale things down so see entire US
        
// Define path generator
var path = d3.geo.path()               // path generator that will convert GeoJSON to SVG paths
		  	 .projection(projection);  // tell path generator to use albersUsa projection

var svg = d3.select("#map")
      
//DRAW THE MAP
d3.json("us-states.json", function(json) {

svg.selectAll("path")
	.data(json.features)
	.enter()
	.append("path")
	.attr("d", path)
	.style("stroke", "#fff")
	.style("stroke-width", "1")
	.style("fill","rgb(213,222,217)")
		 
})      
}

function drawCircles(data){
    svg.selectAll("circle")
    .data(data)
    .enter()
		.append("circle")
		.attr("cx", function(d) {
      if(projection([d.a1Long, d.a1Lat]) != null){
        return projection([d.a1Long, d.a1Lat])[0]
      }
    })
		.attr("cy", function(d) {
      if(projection([d.a1Long, d.a1Lat]) != null){
        return projection([d.a1Long, d.a1Lat])[1]
      }
    })
		.attr("r", 5)
    .style("fill", "red")
    .attr("fill-opacity", .4)
}

function webSocketInvoke() {

    if ("WebSocket" in window) {
        console.log("WebSocket is supported by your Browser!");
        var webSocket = new WebSocket("ws://localhost:8080/","echo-protocol");

        webSocket.onopen = function() {
            console.log("Connection created");
        };

        var n = 0;
        var nMax = 1;  // using the first value to initialise the diagrams
        webSocket.onmessage = function (evt) {
            // from the socket connection

            var received_msg = evt.data;
            console.log(received_msg);
            //received_msg = received_msg.replace("(","");
            //received_msg = received_msg.replace(")","");
            var value = received_msg.split(",");
            //var d = new Date(val[2]);

            /*if(n>nMax) {
                if(val[0] == "Lufttemperatur") {
                    console.log("       n:"+n+"  received_msg: "+  received_msg);
                    sources[0].values.push({ date: d, signal: Number(val[1]) });
                    refreshChart();
                }
                if(val[0] == "MALufttemperatur") {
                    console.log("       n:"+n+"  received_msg: "+  received_msg);
                    sources[1].values.push({ date: d, signal: Number(val[1]) });
                    refreshChart();
                }
            } else {
                console.log("       n:"+n+"  received_msg: "+  received_msg + "   val[2]: " + val[2]);
                // use the first value to initialise the diagrams
                data[n] = {"date": val[2], "source1": Number(val[1]), "source2": Number(val[1])};
                if(n==nMax) {  // I need at least two values for initialise
                    initialization(data);
                }
                n = n+1;
            }          */
        };

        webSocket.onclose = function() {
            console.log("Connection closed");
        };
    } else {
        alert("WebSocket NOT supported by your Browser!");
    }
}

//webSocketInvoke();
