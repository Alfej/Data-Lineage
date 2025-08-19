// Global variables
let nodes = [];
let links = [];
let hiddenNodes = new Set();
let hiddenNodeTypes = new Set();
let hiddenRelationshipTypes = new Set();
let relationshipTypes = [];
let relationshipColors = {};

// Set up SVG and tooltip
const svg = d3.select("#graph");
const width = 800;
const height = 600;
svg.attr("viewBox", [0, 0, width, height])
    .style("width", "100%")
    .style("height", "100%");

const tooltip = d3.select("body").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

const zoomGroup = svg.append("g").attr("class", "zoom-group");

// Create groups for links and nodes
const linkGroup = zoomGroup.append("g").attr("class", "links");
const nodeGroup = zoomGroup.append("g").attr("class", "nodes");

svg.call(
    d3.zoom()
        .scaleExtent([0.2, 3]) // min/max zoom
        .on("zoom", (event) => {
            zoomGroup.attr("transform", event.transform);
        })
);

// Create force simulation
const simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(d => d.id).distance(120))
    .force("charge", d3.forceManyBody().strength(-500))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius(35));

function initializeGraph() {
    // Deep copy original data
    nodes = JSON.parse(JSON.stringify(originalNodes));
    links = JSON.parse(JSON.stringify(originalLinks));

    // Extract unique relationship types and assign colors
    relationshipTypes = [...new Set(originalLinks.map(link => link.relationship))];
    relationshipTypes.forEach((rel, index) => {
        relationshipColors[rel] = `relationship-${index % 10}`;
    });

    // Initialize simulation with data
    simulation.nodes(nodes);
    simulation.force("link").links(links);

    updateGraph();
    updateButtons();
    updateTypeButtons();
    updateRelationshipLegend();
}

// Helper function to determine if a node is visible
function isNodeVisible(nodeId) {
    const node = originalNodes.find(n => n.id === nodeId);
    if (!node) return false;
    
    // Simple logic: Node is hidden if EITHER its type is hidden OR it's individually hidden
    const isTypeHidden = hiddenNodeTypes.has(node.type);
    const isIndividuallyHidden = hiddenNodes.has(nodeId);
    
    return !isTypeHidden && !isIndividuallyHidden;
}

// Helper function to get node visibility state for UI
function getNodeVisibilityState(nodeId) {
    const node = originalNodes.find(n => n.id === nodeId);
    if (!node) return 'hidden';
    
    const isTypeHidden = hiddenNodeTypes.has(node.type);
    const isIndividuallyHidden = hiddenNodes.has(nodeId);
    const isVisible = isNodeVisible(nodeId);
    
    if (isVisible) {
        return 'visible';
    } else if (isTypeHidden && !isIndividuallyHidden) {
        return 'hidden-by-type';
    } else if (!isTypeHidden && isIndividuallyHidden) {
        return 'hidden-individual';
    } else {
        return 'hidden-both';
    }
}

// Helper function to calculate small offset for multiple relationships
function calculateCurveOffset(sourceId, targetId, relationship, allLinks) {
    // Find all relationships between these two nodes (in both directions)
    const relationships = allLinks.filter(l => 
        (l.source === sourceId && l.target === targetId) ||
        (l.source === targetId && l.target === sourceId)
    );
    
    if (relationships.length <= 1) return 0;
    
    // Sort relationships for consistent ordering
    relationships.sort((a, b) => a.relationship.localeCompare(b.relationship));
    
    const index = relationships.findIndex(l => 
        l.relationship === relationship && 
        l.source === sourceId && 
        l.target === targetId
    );
    
    if (index === -1) return 0;
    
    // Calculate small offset: much smaller spread for subtle separation
    const totalRelationships = relationships.length;
    const step = 8; // Much smaller step for minimal visual disruption
    const startOffset = -(totalRelationships - 1) * step / 2;
    
    return startOffset + index * step;
}

function updateGraph() {
    // Calculate visible nodes and links using the improved visibility logic
    const visibleNodes = nodes.filter(d => isNodeVisible(d.id));
    const visibleLinks = calculateLinks();

    // Update simulation
    simulation.nodes(visibleNodes);
    simulation.force("link").links(visibleLinks);

    // Update links (reverse direction: parent -> child)
    const link = linkGroup.selectAll(".link")
        .data(visibleLinks, d => `${d.target.id || d.target}-${d.source.id || d.source}-${d.relationship}`);

    link.exit().remove();

    const linkEnter = link.enter().append("line")
        .attr("class", d => `link ${relationshipColors[d.relationship]} ${d.type}`)
        .attr("marker-end", "url(#arrowhead)")
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            // Swap source and target for tooltip
            const parentNode = typeof d.target === 'object' ? d.target : visibleNodes.find(n => n.id === d.target);
            const childNode = typeof d.source === 'object' ? d.source : visibleNodes.find(n => n.id === d.source);
            
            let tooltipHtml = `
                <strong>Relationship:</strong> ${d.relationship}<br>
                <strong>Parent:</strong> ${parentNode ? parentNode.name : 'Unknown'} (${parentNode ? parentNode.type : 'Unknown'})<br>
                <strong>Child:</strong> ${childNode ? childNode.name : 'Unknown'} (${childNode ? childNode.type : 'Unknown'})<br>
                <strong>Connection:</strong> ${d.type}
            `;
            
            // If it's an indirect connection, show the via information
            if (d.type === 'indirect' && d.via) {
                const viaNode = originalNodes.find(n => n.id === d.via);
                tooltipHtml += `<br><strong>Via:</strong> ${viaNode ? viaNode.name : 'Unknown'} (${d.viaRelationship})`;
            }
            
            tooltip.html(tooltipHtml)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 28) + "px");
        })
        .on("mouseout", function (d) {
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
        });

    link.merge(linkEnter);

    // Update nodes
    const node = nodeGroup.selectAll(".node-group")
        .data(visibleNodes, d => d.id);

    node.exit().remove();

    const nodeEnter = node.enter().append("g")
        .attr("class", "node-group")
        .call(d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended));

    nodeEnter.append("circle")
        .attr("class", d => `node ${d.type.toLowerCase()}`)
        .attr("r", 25)
        .on("click", (event, d) => {
            event.stopPropagation();
            toggleNode(d.id);
        })
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);

            // Find direct parents and children using visible links
            const visibleLinks = calculateLinks();
            const parents = visibleLinks
                .filter(l => l.target === d.id && l.type === 'direct')
                .map(l => nodes.find(n => n.id === l.source))
                .filter(n => n);
            const children = visibleLinks
                .filter(l => l.source === d.id && l.type === 'direct')
                .map(l => nodes.find(n => n.id === l.target))
                .filter(n => n);

            let html = `<strong>Name:</strong> ${d.name}<br>
<strong>Type:</strong> ${d.type}<br>`;

            if (parents.length) {
                html += `<strong>Children:</strong><br>`;
                html += parents.map(p => `• ${p.name} (${p.type})`).join('<br>') + '<br>';
            }
            if (children.length) {
                html += `<strong>Parent:</strong><br>`;
                html += children.map(c => `• ${c.name} (${c.type})`).join('<br>') + '<br>';
            }

            tooltip.html(html)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 28) + "px");
        })
        .on("mouseout", function (d) {
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
        });

    nodeEnter.append("text")
        .attr("class", "node-label")
        .attr("dy", "0.35em")
        .text(d => d.name);

    const nodeUpdate = node.merge(nodeEnter);

    // Update positions on tick
    simulation.on("tick", () => {
        // Handle multiple relationships with slight offsets
        linkGroup.selectAll(".link")
            .each(function(d) {
                const source = typeof d.source === 'object' ? d.source : visibleNodes.find(n => n.id === d.source);
                const target = typeof d.target === 'object' ? d.target : visibleNodes.find(n => n.id === d.target);
                
                if (!source || !target) return;
                
                // Check if there are multiple relationships between these nodes
                const multipleRelationships = visibleLinks.filter(l => 
                    (l.source === d.source && l.target === d.target) ||
                    (l.source === d.target && l.target === d.source)
                );
                
                if (multipleRelationships.length > 1) {
                    // Apply small offset for multiple relationships
                    const offset = calculateCurveOffset(d.target.id || d.target, d.source.id || d.source, d.relationship, visibleLinks);
                    const angle = Math.atan2(source.y - target.y, source.x - target.x);
                    const perpAngle = angle + Math.PI / 2;
                    const offsetDistance = offset * 0.3; // Much smaller offset
                    
                    const offsetX = Math.cos(perpAngle) * offsetDistance;
                    const offsetY = Math.sin(perpAngle) * offsetDistance;
                    
                    d3.select(this)
                        .attr("x1", target.x + offsetX)
                        .attr("y1", target.y + offsetY)
                        .attr("x2", source.x + offsetX)
                        .attr("y2", source.y + offsetY);
                } else {
                    // Single relationship - straight line
                    d3.select(this)
                        .attr("x1", target.x)
                        .attr("y1", target.y)
                        .attr("x2", source.x)
                        .attr("y2", source.y);
                }
            });

        nodeUpdate.attr("transform", d => `translate(${d.x},${d.y})`);
    });

    simulation.alpha(1).restart();
    updateStatus();
}

function calculateLinks() {
    const visibleNodeIds = new Set(nodes.filter(d => isNodeVisible(d.id)).map(d => d.id));

    const resultLinks = [];

    // Add direct links between visible nodes - Handle multiple relationships between same nodes
    for (const link of originalLinks) {
        if (visibleNodeIds.has(link.source) &&
            visibleNodeIds.has(link.target) &&
            !hiddenRelationshipTypes.has(link.relationship)) {
            resultLinks.push({
                source: link.source,
                target: link.target,
                relationship: link.relationship,
                type: 'direct'
            });
        }
    }

    // Add indirect links when nodes are hidden
    const allHiddenNodeIds = originalNodes.filter(n => !isNodeVisible(n.id)).map(n => n.id);

    for (const hiddenNodeId of allHiddenNodeIds) {
        const incomingLinks = originalLinks.filter(l => l.target === hiddenNodeId);
        const outgoingLinks = originalLinks.filter(l => l.source === hiddenNodeId);

        for (const incoming of incomingLinks) {
            for (const outgoing of outgoingLinks) {
                if (visibleNodeIds.has(incoming.source) &&
                    visibleNodeIds.has(outgoing.target) &&
                    !hiddenRelationshipTypes.has(incoming.relationship) &&
                    !hiddenRelationshipTypes.has(outgoing.relationship)) {

                    // Create unique key that includes relationship to allow multiple relationships
                    const exists = resultLinks.some(l =>
                        l.source === incoming.source && 
                        l.target === outgoing.target && 
                        l.relationship === incoming.relationship
                    );

                    if (!exists) {
                        resultLinks.push({
                            source: incoming.source,
                            target: outgoing.target,
                            relationship: incoming.relationship,
                            type: 'indirect',
                            via: hiddenNodeId,
                            viaRelationship: outgoing.relationship
                        });
                    }
                }
            }
        }
    }

    return resultLinks;
}

function toggleNode(nodeId) {
    const node = originalNodes.find(n => n.id === nodeId);
    if (!node) return;
    
    const isTypeHidden = hiddenNodeTypes.has(node.type);
    const isIndividuallyHidden = hiddenNodes.has(nodeId);
    const currentlyVisible = isNodeVisible(nodeId);
    
    if (currentlyVisible) {
        // Node is currently visible, hide it
        hiddenNodes.add(nodeId);
    } else {
        // Node is currently hidden, show it
        if (isTypeHidden && isIndividuallyHidden) {
            // Hidden by both, remove individual hiding (this will show it as override)
            hiddenNodes.delete(nodeId);
        } else if (isTypeHidden && !isIndividuallyHidden) {
            // Hidden by type only, already should be visible (this shouldn't happen)
            // Do nothing or ensure it's not in hiddenNodes
            hiddenNodes.delete(nodeId);
        } else {
            // Hidden individually only, remove individual hiding
            hiddenNodes.delete(nodeId);
        }
    }
    
    updateGraph();
    updateButtons();
    updateTypeButtons(); // Update type buttons as well since counts may have changed
}

function toggleNodeType(nodeType) {
    const wasHidden = hiddenNodeTypes.has(nodeType);
    
    if (wasHidden) {
        // Show all nodes of this type
        hiddenNodeTypes.delete(nodeType);
        // Also remove any individual hiding for nodes of this type
        // This ensures "Show All" actually shows ALL nodes of that type
        originalNodes.filter(n => n.type === nodeType).forEach(node => {
            hiddenNodes.delete(node.id);
        });
    } else {
        // Hide all nodes of this type
        hiddenNodeTypes.add(nodeType);
        // No need to add to hiddenNodes - type hiding is enough
    }
    
    updateGraph();
    updateTypeButtons();
    updateButtons(); // Update individual buttons since their states may have changed
}

function toggleRelationshipType(relationshipType) {
    if (hiddenRelationshipTypes.has(relationshipType)) {
        hiddenRelationshipTypes.delete(relationshipType);
    } else {
        hiddenRelationshipTypes.add(relationshipType);
    }
    updateGraph();
    updateTypeButtons();
}

function resetAll() {
    hiddenNodes.clear();
    hiddenNodeTypes.clear();
    hiddenRelationshipTypes.clear();
    updateGraph();
    updateButtons();
    updateTypeButtons();
}

function updateStatus() {
    const visibleNodeCount = originalNodes.filter(n => isNodeVisible(n.id)).length;

    document.getElementById('total-count').textContent = originalNodes.length;
    document.getElementById('visible-count').textContent = visibleNodeCount;
    document.getElementById('hidden-count').textContent = originalNodes.length - visibleNodeCount;

    const hiddenList = document.getElementById('hidden-list');
    const allHiddenNodeIds = originalNodes.filter(n => !isNodeVisible(n.id)).map(n => n.id);

    if (allHiddenNodeIds.length === 0) {
        hiddenList.textContent = 'None';
    } else {
        const hiddenNames = allHiddenNodeIds.map(id => {
            const node = originalNodes.find(n => n.id === id);
            return node ? node.name : id;
        });
        hiddenList.textContent = hiddenNames.join(', ');
    }
}

function updateButtons() {
    const buttonsContainer = document.getElementById('node-buttons');
    buttonsContainer.innerHTML = '';

    for (const node of originalNodes) {
        const visibilityState = getNodeVisibilityState(node.id);
        const isVisible = isNodeVisible(node.id);
        
        const button = document.createElement('button');
        
        // Set button appearance and behavior based on visibility state
        switch (visibilityState) {
            case 'visible':
                button.className = 'btn btn-hide';
                button.textContent = `Hide ${node.name}`;
                button.onclick = () => toggleNode(node.id);
                break;
                
            case 'hidden-individual':
                button.className = 'btn btn-show';
                button.textContent = `Show ${node.name}`;
                button.onclick = () => toggleNode(node.id);
                break;
                
            case 'hidden-by-type':
                button.className = 'btn btn-show btn-type-override';
                button.textContent = `Show ${node.name} (Type Hidden)`;
                button.onclick = () => toggleNode(node.id);
                break;
                
            case 'hidden-both':
                button.className = 'btn btn-show';
                button.textContent = `Show ${node.name} (Type + Individual)`;
                button.onclick = () => toggleNode(node.id);
                break;
                
            default:
                button.className = 'btn btn-show';
                button.textContent = `Show ${node.name}`;
                button.onclick = () => toggleNode(node.id);
        }
        
        buttonsContainer.appendChild(button);

        const typeSpan = document.createElement('span');
        typeSpan.textContent = ` (${node.type})`;
        typeSpan.style.fontSize = '10px';
        typeSpan.style.opacity = '0.7';
        
        // Add visual indicator for type status
        const isTypeHidden = hiddenNodeTypes.has(node.type);
        if (isTypeHidden) {
            typeSpan.style.color = '#E74C3C';
            typeSpan.textContent += ' - TYPE HIDDEN';
        }
        
        buttonsContainer.appendChild(typeSpan);
        buttonsContainer.appendChild(document.createElement('br'));
    }
}

function updateTypeButtons() {
    // Update node type buttons
    const nodeTypes = [...new Set(originalNodes.map(n => n.type))];
    const nodeTypeContainer = document.getElementById('node-type-buttons');
    nodeTypeContainer.innerHTML = '';

    nodeTypes.forEach(type => {
        const button = document.createElement('button');
        const isTypeHidden = hiddenNodeTypes.has(type);
        const nodesOfType = originalNodes.filter(n => n.type === type);
        const visibleNodesOfType = nodesOfType.filter(n => isNodeVisible(n.id));
        const hiddenNodesOfType = nodesOfType.length - visibleNodesOfType.length;
        
        button.className = `btn btn-type-control ${isTypeHidden ? '' : 'active'}`;
        
        if (isTypeHidden) {
            button.textContent = `Show All ${type}s (${hiddenNodesOfType}/${nodesOfType.length} hidden)`;
        } else {
            button.textContent = `Hide All ${type}s (${visibleNodesOfType.length}/${nodesOfType.length} visible)`;
        }
        
        button.onclick = () => toggleNodeType(type);
        nodeTypeContainer.appendChild(button);
    });

    // Update relationship type buttons
    const relationshipTypeContainer = document.getElementById('relationship-type-buttons');
    relationshipTypeContainer.innerHTML = '';

    relationshipTypes.forEach(relationship => {
        const button = document.createElement('button');
        const isRelationshipHidden = hiddenRelationshipTypes.has(relationship);
        
        button.className = `btn btn-type-control ${isRelationshipHidden ? '' : 'active'}`;
        
        if (isRelationshipHidden) {
            button.textContent = `Show ${relationship} Relationships`;
        } else {
            button.textContent = `Hide ${relationship} Relationships`;
        }
        
        button.onclick = () => toggleRelationshipType(relationship);
        relationshipTypeContainer.appendChild(button);
    });
}

function updateRelationshipLegend() {
    const legendContainer = document.getElementById('relationship-legend');
    legendContainer.innerHTML = '';

    relationshipTypes.forEach(relationship => {
        const item = document.createElement('div');
        item.className = 'relationship-legend-item';

        const colorBox = document.createElement('div');
        colorBox.className = `relationship-color-box ${relationshipColors[relationship]}`;

        const label = document.createElement('span');
        label.textContent = relationship;

        item.appendChild(colorBox);
        item.appendChild(label);
        legendContainer.appendChild(item);
    });
}

// Drag functions
function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}

function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}

// Collapsible functionality
function toggleCollapsible(element) {
    element.classList.toggle('active');
}

// Add arrow marker
svg.append("defs").append("marker")
    .attr("id", "arrowhead")
    .attr("viewBox", "-0 -3 6 6")
    .attr("refX", 18)
    .attr("refY", 0)
    .attr("orient", "auto")
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("xoverflow", "visible")
    .append("svg:path")
    .attr("d", "M 0,-3 L 6,0 L 0,3")
    .attr("fill", "#666")
    .style("stroke", "none");



async function fetchAndDisplayCsv(filePath) {
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const csvText = await response.text();
        const rows = csvText.trim().split('\n'); // Split into lines
        const headers = rows[0].split(','); // Get headers from the first line

        const tableBody = document.getElementById('lineage-table').querySelector('tbody');
        tableBody.innerHTML = ''; // Clear any existing rows

        for (let i = 1; i < rows.length; i++) {
            const values = rows[i].split(',');
            if (values.length === headers.length) { // Ensure row has expected number of columns
                const row = tableBody.insertRow();
                headers.forEach((header, index) => {
                    const cell = row.insertCell();
                    cell.textContent = values[index].trim(); // Add text content and trim whitespace
                });
            } else {
                console.warn(`Skipping row ${i + 1} due to incorrect column count:`, rows[i]);
            }
        }
    } catch (error) {
        console.error("Error fetching or displaying CSV:", error);
        // Optionally display an error message to the user
        const tableBody = document.getElementById('lineage-table').querySelector('tbody');
        const row = tableBody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 5; // Span across all columns
        cell.textContent = `Error loading data: ${error.message}`;
        cell.style.color = 'red';
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function () {
    // Wait a bit for the data script to load
    setTimeout(initializeGraph, 100);
    fetchAndDisplayCsv('parent_child_lineage.csv');
});