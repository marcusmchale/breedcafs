
// setup svg div
var svg = d3.select("svg"),
	width = +svg.attr("width"),
	height = +svg.attr("height"),
	radius = 20,
	node,
	link;

var simulation = d3.forceSimulation()
    .force("center", d3.forceCenter(width/2, height/2))
	.force("link", d3.forceLink().id(function (d) {return d.id;}).distance(50).strength(1))
	.force("charge", d3.forceManyBody().strength(-30).distanceMin(10).distanceMax(300))
	.force("collision", d3.forceCollide().radius(function(d) {
	    return d.radius
	}).strength(1000))


var load_graph = function(data) {
	console.log("LOADs");
	// load graph (nodes,links) json from /graph endpoint
	d3.json(data, function(error, graph) {
		if (error) console.warn(error);
		else if (graph.hasOwnProperty('status')) {
			$("svg").replaceWith("<div class='flash'>" + graph.status + "</div>")
		}
		else {
			update(graph.links, graph.nodes);
		}
	})
}


var update = function(links, nodes){
	console.log("UPDATE");
	link = svg.selectAll(".link")
	.data(links)
	.enter()
	.append("line")
	.attr("class","link")

	link.append("title")
		.text(function (d) {return d.type;});

	node = svg.selectAll(".node")
		.data(nodes)
		.enter()
		.append("g")
		.attr("class","node")
		.call(d3.drag()
			.on("start", dragstarted)
			.on("drag", dragged)
		);

	node.append("circle")
		.attr("r",radius)
		.attr("class", function (d) { return "node " + d.label; })

	node.append("title")
		.text(function (d) {return d.label});

	node.append("text")
		.text(function (d) {return d.name})
		.attr("class", function (d) { return "node " + d.label; })
		.attr("text-anchor", "middle")
		.attr("dy", radius / 2 );

	simulation
		.nodes(nodes)
		.on("tick", ticked);

	simulation.force("link")
		.links(links)
}

var ticked = function() {
	node
		.attr("transform", function(d) {
			return "translate("
			+ Math.max(radius, Math.min(width-radius, d.x))
			+","
			+ Math.max(radius, Math.min(height-radius, d.y))
			+ ")";
		});
	link
	    .attr("x1", function(d) { return Math.max(radius, Math.min(width-radius, d.source.x)); })
	    .attr("y1", function(d) { return Math.max(radius, Math.min(height-radius, d.source.y)); })
	    .attr("x2", function(d) { return Math.max(radius, Math.min(width-radius, d.target.x)); })
	    .attr("y2", function(d) { return Math.max(radius, Math.min(height-radius, d.target.y)); })
		//.attr("x1", function(d) { return d.source.x; })
		//.attr("y1", function(d) { return d.source.y; })
		//.attr("x2", function(d) { return d.target.x; })
		//.attr("y2", function(d) { return d.target.y; });
}

var dragstarted = function(d) {
	if (!d3.event.active) simulation.alphaTarget(0.3).restart()
	d.fx = d.x;
	d.fy = d.y;
}

var dragged = function(d) {
	d.fx = d3.event.x;
	d.fy = d3.event.y;
}

load_graph('/json_submissions');