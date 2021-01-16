
/* TODO: 
        - Display Graph
        - Display Tooltip on Map
        - Clean / Structure Code
        - Bind Colors to Event-Code
        - Bind Size to Circle-Size


/*  This visualization was made possible by modifying code provided by:

Scott Murray, Choropleth example from "Interactive Data Visualization for the Web" 
https://github.com/alignedleft/d3-book/blob/master/chapter_12/05_choropleth.html   
		
Malcolm Maclean, tooltips example tutorial
http://www.d3noob.org/2013/01/adding-tooltips-to-d3js-graph.html

Mike Bostock, Pie Chart Legend
http://bl.ocks.org/mbostock/3888852  */

		
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

		
// Define linear scale for output
//var color = d3.scale.linear()
			  //.range(["rgb(213,222,217)","rgb(69,173,168)","rgb(84,36,55)","rgb(217,91,67)"]);

//var legendText = ["Cities Lived", "States Lived", "States Visited", "Nada"];

//Create SVG element and append map to the SVG
var svg = d3.select("#map")
        

// Load GeoJSON data and merge with states data
d3.json("us-states.json", function(json) {

// Bind the data to the SVG and create one path per GeoJSON feature
svg.selectAll("path")
	.data(json.features)
	.enter()
	.append("path")
	.attr("d", path)
	.style("stroke", "#fff")
	.style("stroke-width", "1")
	.style("fill","rgb(213,222,217)")
		 
        
// Modified Legend Code from Mike Bostock: http://bl.ocks.org/mbostock/3888852
// var legend = d3.select("body").append("svg")
//       			.attr("class", "legend")
//      			.attr("width", 140)
//     			.attr("height", 200)
//    				.selectAll("g")
//    				.data(color.domain().slice().reverse())
//    				.enter()
//    				.append("g")
//      			.attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

//   	legend.append("rect")
//    		  .attr("width", 18)
//    		  .attr("height", 18)
//    		  .style("fill", color);

//   	legend.append("text")
//   		  .data(legendText)
//       	  .attr("x", 24)
//       	  .attr("y", 9)
//       	  .attr("dy", ".35em")
// 			.text(function(d) { return d; });
			

$.getJSON("out.json", function(data) {

    //CREATE GRAPH
    // set the dimensions and margins of the graph
    var margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = 460 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

    // append the svg object to the body of the page
    var svg2 = d3.select("#my_dataviz")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");
  
              // Add X axis --> it is a date format
    var x = d3.scaleTime()
    .domain(d3.extent(data, function(d) { return d.date; }))
    .range([ 0, width ]);
  svg2.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

  // Add Y axis
  var y = d3.scaleLinear()
    .domain([0, 1000])
    .range([ height, 0 ]);
  svg2.append("g")
    .call(d3.axisLeft(y));


  //CREATE THE MAP
  svg.selectAll("circles").data(data).enter();
  
  //iterate over the data, this needs to be changed because with kafka this will be done on the fly
	for(var i = 0; i < 100; i++){ 
    if(projection([data[i].a1Long, data[i].a1Lat]) === null){ //Catch faulty GeoData
      continue;
    }

    //Put the circle on the Map
		svg
		.append("circle")
		.attr("cx", projection([data[i].a1Long, data[i].a1Lat])[0])
		.attr("cy", projection([data[i].a1Long, data[i].a1Lat])[1])
		.attr("r", 5)
    .style("fill", "red")
    .attr("title","test")
		// .attr("stroke", "#69b3a2")
		// .attr("stroke-width", 3)
    .attr("fill-opacity", .4)
    

    //CREATE THE GRAPH PATH
    svg2.append("path")
    .datum(data)
    .attr("fill", "none")
    .attr("stroke", "steelblue")
    .attr("stroke-width", 1.5)
    .attr("d", d3.line()
      .x(function(d) { return x(data[i].date) })
      .y(function(d) { return y(data[i].numMentions) })
      )


    //Add Data to Timeline
    var tr = $("<tr>");
    var eventTD = $("<td>")
    var dateTD = $("<td>")
    var toneTD = $("<td>")
    dateTD.html(data[i].date)
    eventTD.html(data[i].eventDescription)
    toneTD.html(data[i].avgTone)
    tr.append(dateTD);
    tr.append(eventTD);
    tr.append(toneTD);
    $("#timeline_table").append(tr)

	}
	});

})

