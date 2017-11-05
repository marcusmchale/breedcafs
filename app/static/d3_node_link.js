// setup svg div
var svg = d3.select("svg"),
	width = +svg.attr("width"),
	height = +svg.attr("height"),
	radius=20,
	node,
	link;

var simulation = d3.forceSimulation()
	.force("link", d3.forceLink().id(function (d) {return d.id;}).distance(50).strength(1))
	.force("charge", d3.forceManyBody().strength(-100).distanceMin(10).distanceMax(300))
	.force("center", d3.forceCenter(width / 2, height / 2));

var load_graph = function() {
	// load graph (nodes,links) json from /graph endpoint
	var data = "/json_submissions";
	d3.json(data, function(error, graph) {
		if (error) throw error;
		update(graph.links, graph.nodes);
	})
}

function update(links, nodes){
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

function ticked() {
	node
		.attr("transform", function(d) { 
			return "translate(" 
			+ Math.max(radius, Math.min(width-radius, d.x))
			+"," 
			+ Math.max(radius, Math.min(height-radius, d.y))
			+ ")"; 
		});
	link
		.attr("x1", function(d) { return d.source.x; })
		.attr("y1", function(d) { return d.source.y; })
		.attr("x2", function(d) { return d.target.x; })
		.attr("y2", function(d) { return d.target.y; });
}

function dragstarted(d) {
	if (!d3.event.active) simulation.alphaTarget(0.3).restart()
	d.fx = d.x;
	d.fy = d.y;
}

function dragged(d) {
	d.fx = d3.event.x;
	d.fy = d3.event.y;
}



load_graph();