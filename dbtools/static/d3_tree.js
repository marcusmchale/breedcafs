//modified from https://bl.ocks.org/d3noob/43a860bc0024792f8803bba8ca0d5ecd
//mostly just added the load from json feature

// setup to selet the svg object in the page and fill it
var svg = d3.select("svg"),
	margin = {top:20, right:50, bottom:30, left:50},
	width = svg.attr("width") - margin.left - margin.right,
	height = svg.attr("height") - margin.top - margin.bottom,
	duration = 1000,
	min_font = 12,
	i=0,
	root,
	treeLayout = d3.tree().size([height, width]);

var svg = svg.append("g")
	.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

//load data - asynchronous so update has to be called here 
//(stored so can be called on button clicks in location_select.js)
var load_chart = function () {
	d3.json(jsonData, function(error, data) {
		if (error) throw error;
		load(data);
		root.children.forEach(collapse);
		root.x0 = height/2;
		root.y0 = 0;
		update(root);
	})
}


//first load it anyway
load_chart();
//and reload it in location_select.js when new locations/trees are submitted

function load(data) {
	//remove countries without region for chart
	var i = data.children.length 
	while (i--) {
		if (data.children[i].children.length === 0) {
			data.children.splice(i, 1);
		}
	}
	//get depth data and sum treecount
	root = d3.hierarchy(data, function(d) {
		return d.children; })
		.sum(function(d) {
			return d.treecount ? d.treecount : 0; });
}

// Collapse the node and all it's children
function collapse(d) {
	if(d.children) {
		d._children = d.children
		d._children.forEach(collapse)
		d.children = null
	}
}

function click(d) {
	 {
		if (d.children) {
			d._children = d.children;
			d.children = null;
		} else {
			d.children = d._children;
			d._children = null;
		}
		update(d);
	}
}

function diagonal(s, d) {
	path = `M ${s.y} ${s.x}
		C ${(s.y + d.y) / 2} ${s.x},
		${(s.y + d.y) / 2} ${d.x},
		${d.y} ${d.x}`
	return path

}

function update(source) {
	//add x and y coords from d3.tree then create nodes and links arrays
	var treeData = treeLayout(root);
	var nodes = treeData.descendants();
	var links = treeData.descendants().slice(1);

	//normalise for fixed depth
	nodes.forEach(function(d){ d.y = d.depth * 80 });

	// **NODES **

	// Update the nodes...
	var node = svg.selectAll("g.node")
		.data(nodes, function(d) { return d.id || (d.id = ++i); });

	// Enter any new modes at the parent's previous position.
	var nodeEnter = node.enter()
		.append("g")
			.attr("class","node")
			.attr("transform", function (d) { 
				return "translate(" + source.y0 + "," + source.x0 + ")";})
		.on("click", click);

	nodeEnter.append("circle")
		.attr("r", 0)
		.style('opacity', 0)	
		.attr("class", function(d) {
			if (d._children) {
				return "node collapsed" + d.data.label
			} else {
				return "node " + d.data.label
			}
		});

	nodeEnter.append("title")
		.text(function (d) { return d.data.label } );

	nodeEnter.append("text")
		.text(function (d) { return d.data.name })
		.attr("class", function (d) { return "node text" + d.data.label; })
		.attr("text-anchor", "middle")
		.attr("dy", function (d) {
			return Math.max(20, Math.log2(d.value)) + "px" ;})
		.style("font-size", function (d) {
			if (d.data.label === "root_node") {
				return min_font + "px" ;
			} else {
				return Math.max(min_font , Math.log2(d.value)) + "px" ; 
			}
		});

	nodeEnter.append("text")
		.text(function (d) { return d.value })
		.attr("class", function (d) { return "node count" + d.data.label; })
		.attr("text-anchor", function (d) { 
			if (d.data.label === "root_node") {
				return "end";
			} else {
				return "middle";
			}
		})
		.attr("dy", "0.4em")
		.style("font-size", function (d) {
			if (d.data.label === "root_node") {
				return min_font + "px" ;
			} else {
				return Math.max(min_font , Math.log2(d.value)) + "px" ; 
			}
		});

	// UPDATE
	var nodeUpdate = nodeEnter.merge(node); 

	//transition to the proper node position
	nodeUpdate.transition().duration(duration)
		.attr("transform", function(d) {
			return "translate(" + d.y + "," + d.x + ")";
		});

	nodeUpdate.select("circle").transition().duration(duration)
		.attr("r", function (d) { return Math.max(10, 3*Math.log2(d.value))} )
		.style('opacity', 1);

	//update node attributes and style
	nodeUpdate.select("circle, text")
		.attr("cursor","pointer")
		.attr("class", function(d) {
			if (d._children) {
				return "node collapsed" + d.data.label
			} else {
				return "node " + d.data.label
			}
		});
	
	// Remove any exiting nodes
	var nodeExit = node.exit().transition().duration(duration)
		.attr("transform", function(d) {
			return "translate(" + source.y + "," + source.x + ")";
		})
		.remove();

  	// On exit reduce the radius and opacity to 0
	nodeExit.select("circle")
		.attr("r", 0)
		.style('opacity', 0);

	// ** LINKS **

	var link = svg.selectAll(".link")
		.data(links, function (d) { return d.id; });

	var linkEnter = link.enter()
		.insert("path", "g")
			.attr("class","link")
			.attr("d", function(d) {
				var o = {x: source.x0, y:source.y0};
				return diagonal(o, o)
			});

	// UPDATE 
	
	var linkUpdate = linkEnter.merge(link);

	//transition back to parent element position

	linkUpdate.transition()
		.duration(duration)
		.attr("d", function(d) {return diagonal(d, d.parent) });

	//remove exiting nodes

	var linkExit =	link.exit().transition()
		.duration(duration)
		.attr("d", function(d) {
			var o = {x: source.x, y:source.y};
			return diagonal(o, o)
		})
		.remove();

	// Store the old positions for transition.
	nodes.forEach(function(d){
		d.x0 = d.x;
		d.y0 = d.y;
	});

}

