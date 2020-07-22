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
function DSF(node, parent, level, nodes, edges) {
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
  visjsNode.label = visjsNode.splitVar;
  visjsNode.level = level;

  nodes.push(visjsNode);

  // if this node is not root create edge from parent to this
  if (parent != 0) {
    edges.push({ from: parent, to: thisNodeId });
  }

  // increase tree deppth befor calling the function (recursively) for the child node
  level++;

  // call the function for child node.
  if (node.right) {
    DSF(node.right, thisNodeId, level, nodes, edges);
  }
  if (node.left) {
    DSF(node.left, thisNodeId, level, nodes, edges);
  }
}

function draw() {
  destroy();

  nodes = [];
  edges = [];

  nodeId = 0;
  DSF(RF.trees[1].rootNode, nodeId, 0, nodes, edges);

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
