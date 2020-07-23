var nodes = null;
var edges = null;
var network = null;

function destroy() {
  if (network !== null) {
    network.destroy();
    network = null;
  }
}

var nodeId = 0;
function DSF(node, parent, level, nodes, edges, limits) {
  // Assign new node id (integer)
  // nodeId is a global variable used in a recursive function
  nodeId++;
  var thisNodeId = nodeId;

  // fix missing fiels in the leaf nodes
  if (!node.splitVar) {
    node.splitVar = "leaf";
  }

  // generates VisJS node and push it to nodes array
  let visjsNode = Object.assign({}, node);
  delete visjsNode.left;
  delete visjsNode.right;
  visjsNode.id = thisNodeId;
  visjsNode.level = level;
  visjsNode.samples = visjsNode.size;

  nodes.push(visjsNode);

  // if this node is not root create edge from parent to this
  if (parent != 0) {
    edges.push({ from: parent, to: thisNodeId });
  }

  // increase tree deppth befor calling the function (recursively) for the child node
  level++;

  // update limits
  if (limits.samples.min > visjsNode.samples)
    limits.samples.min = visjsNode.samples;
  if (limits.samples.max < visjsNode.samples)
    limits.samples.max = visjsNode.samples;
  if (limits.impurity.min > visjsNode.impurity)
    limits.impurity.min = visjsNode.impurity;
  if (limits.impurity.max < visjsNode.impurity)
    limits.impurity.max = visjsNode.impurity;
  if (limits.impurityReduction.min > visjsNode.impurityReduction)
    limits.impurityReduction.min = visjsNode.impurityReduction;
  if (limits.impurityReduction.max < visjsNode.impurityReduction)
    limits.impurityReduction.max = visjsNode.impurityReduction;

  // call the function for child node.
  if (node.right) {
    DSF(node.right, thisNodeId, level, nodes, edges, limits);
  }
  if (node.left) {
    DSF(node.left, thisNodeId, level, nodes, edges, limits);
  }
}

function perc2color(perc) {
  var r,
    g,
    b = 0;
  if (perc < 50) {
    r = 255;
    g = Math.round(5.1 * perc);
  } else {
    g = 255;
    r = Math.round(510 - 5.1 * perc);
  }
  var h = r * 0x10000 + g * 0x100 + b * 0x1;
  return "#" + ("000000" + h.toString(16)).slice(-6);
}

function Coloring(nodes, limits, colorBy) {
  if (colorBy == "NA") return;
  sub = limits[colorBy].min;
  mul = 100 / (limits[colorBy].max - limits[colorBy].min);
  for (i in nodes) {
    var percent = (nodes[i][colorBy] - sub) * mul;
    nodes[i].color = perc2color(percent);
  }
}

function Labeling(nodes, labelBy) {
  if (labelBy == "NA") return;
  for (i in nodes) {
    nodes[i].label = String(nodes[i][labelBy]);
  }
}

function draw() {
  destroy();

  nodes = [];
  edges = [];

  // Limits to generate color scale
  var limits = {
    samples: { min: 1000000000, max: 0 },
    impurity: { min: 1, max: 0 },
    impurityReduction: { min: 1, max: 0 },
  };
  var treeIdToPlot = document.getElementById("treeId").value;
  nodeId = 0;
  DSF(RF.trees[treeIdToPlot].rootNode, nodeId, 0, nodes, edges, limits);

  // color nodes
  var colorBy = document.getElementById("colorBy").value;
  Coloring(nodes, limits, colorBy);

  // label nodes
  var labelBy = document.getElementById("labelBy").value;
  Labeling(nodes, labelBy);

  var data = {
    nodes: nodes,
    edges: edges,
  };

  // create a network
  var container = document.getElementById("mynetwork");

  var options = {
    layout: {
      hierarchical: {
        direction: "UD",
      },
    },
  };

  network = new vis.Network(container, data, options);

  // add event listeners
  network.on("select", function (params) {
    document.getElementById("selection").innerHTML =
      "Selection: " + params.nodes;
  });
}

window.addEventListener("load", () => {
  draw();
});
