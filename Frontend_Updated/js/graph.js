let nodes = [];
let links = [];
let columnFilters = {};
let relationshipTypes = [];
let relationshipColors = {};
let nodePositions = new Map(); // Store fixed positions

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
        .scaleExtent([0.2, 3])
        .on("zoom", (event) => {
            zoomGroup.attr("transform", event.transform);
        })
);

// Create force simulation with reduced forces for stability
const simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(d => d.id).distance(120).strength(0.5))
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius(35))
    .alphaDecay(0.05) // Slower decay for smoother settling
    .velocityDecay(0.8); // Higher velocity decay for stability

// Parse CSV and initialize
function parseCsvData(csvText) {
    const lines = csvText.trim().split('\n');
    const headers = lines[0].split(',');
    const data = [];

    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        if (values.length === headers.length) {
            const row = {};
            headers.forEach((header, index) => {
                row[header.trim()] = values[index].trim();
            });
            data.push(row);
        }
    }
    console.log("Parsed CSV Headers:", headers);
    console.log("Parsed CSV Data:", data);
    return { headers: headers.map(h => h.trim()), data };
}

function initializeData() {
    const parsed = parseCsvData(csvData);
    originalData = parsed.data;

    // Create nodes and links from data
    const nodeSet = new Set();
    const linkArray = [];

    originalData.forEach(row => {
        nodeSet.add(JSON.stringify({
            id: row.childTableName,
            name: row.childTableName,
            type: row.childTableType
        }));
        nodeSet.add(JSON.stringify({
            id: row.parentTableName,
            name: row.parentTableName,
            type: row.parentTableType
        }));

        linkArray.push({
            source: row.parentTableName,
            target: row.childTableName,
            relationship: row.relationship
        });
    });

    originalNodes = Array.from(nodeSet).map(nodeStr => JSON.parse(nodeStr));
    originalLinks = linkArray;

    // Extract unique relationship types and assign colors
    relationshipTypes = [...new Set(originalLinks.map(link => link.relationship))];
    relationshipTypes.forEach((rel, index) => {
        relationshipColors[rel] = `relationship-${index % 10}`;
    });

    // Initialize column filters
    parsed.headers.forEach(header => {
        const uniqueValues = [...new Set(originalData.map(row => row[header]))];
        columnFilters[header] = {
            selected: [...uniqueValues],
            options: uniqueValues
        };
    });

    createFilterControls(parsed.headers);
    updateGraph();
    updateRelationshipLegend();
    displayTable(originalData);
}

function createFilterControls(headers) {
    const container = document.getElementById('filter-controls');
    container.innerHTML = '';

    headers.forEach(header => {
        const filterGroup = document.createElement('div');
        filterGroup.className = 'filter-group';

        const label = document.createElement('label');
        label.textContent = header.charAt(0).toUpperCase() + header.slice(1).replace(/([A-Z])/g, ' $1');

        const multiselect = document.createElement('div');
        multiselect.className = 'multiselect';

        const dropdown = document.createElement('div');
        dropdown.className = 'multiselect-dropdown';
        dropdown.onclick = () => toggleDropdown(header);

        const selectedText = document.createElement('span');
        selectedText.id = `selected-${header}`;

        const selectedCount = document.createElement('span');
        selectedCount.className = 'selected-count';
        selectedCount.id = `count-${header}`;

        dropdown.appendChild(selectedText);
        dropdown.appendChild(selectedCount);

        const options = document.createElement('div');
        options.className = 'multiselect-options';
        options.id = `options-${header}`;

        columnFilters[header].options.forEach(option => {
            const optionDiv = document.createElement('div');
            optionDiv.className = 'multiselect-option';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.value = option;
            checkbox.checked = true;
            checkbox.onchange = () => updateFilter(header, option, checkbox.checked);

            const label = document.createElement('span');
            label.textContent = option;

            optionDiv.appendChild(checkbox);
            optionDiv.appendChild(label);
            options.appendChild(optionDiv);
        });

        multiselect.appendChild(dropdown);
        multiselect.appendChild(options);

        filterGroup.appendChild(label);
        filterGroup.appendChild(multiselect);
        container.appendChild(filterGroup);

        updateDropdownText(header);
    });
}

function toggleDropdown(header) {
    const dropdown = document.querySelector(`#options-${header}`);
    const button = dropdown.previousElementSibling;

    dropdown.classList.toggle('show');
    button.classList.toggle('open');

    // Close other dropdowns
    document.querySelectorAll('.multiselect-options').forEach(opt => {
        if (opt !== dropdown) {
            opt.classList.remove('show');
            opt.previousElementSibling.classList.remove('open');
        }
    });
}

function updateFilter(header, value, checked) {
    if (checked) {
        if (!columnFilters[header].selected.includes(value)) {
            columnFilters[header].selected.push(value);
        }
    } else {
        columnFilters[header].selected = columnFilters[header].selected.filter(v => v !== value);
    }

    updateDropdownText(header);
    updateGraph();
}

function updateDropdownText(header) {
    const selectedText = document.getElementById(`selected-${header}`);
    const selectedCount = document.getElementById(`count-${header}`);
    const selected = columnFilters[header].selected;
    const total = columnFilters[header].options.length;

    if (selected.length === total) {
        selectedText.textContent = 'All selected';
    } else if (selected.length === 0) {
        selectedText.textContent = 'None selected';
    } else if (selected.length === 1) {
        selectedText.textContent = selected[0];
    } else {
        selectedText.textContent = `${selected.length} items selected`;
    }

    selectedCount.textContent = `${selected.length}/${total}`;
}

function getFilteredData() {
    return originalData.filter(row => {
        return Object.keys(columnFilters).every(header => {
            return columnFilters[header].selected.includes(row[header]);
        });
    });
}

function updateGraph() {
    const filteredData = getFilteredData();

    // Create nodes and links from filtered data
    const nodeSet = new Set();
    const linkArray = [];

    filteredData.forEach(row => {
        nodeSet.add(JSON.stringify({
            id: row.childTableName,
            name: row.childTableName,
            type: row.childTableType
        }));
        nodeSet.add(JSON.stringify({
            id: row.parentTableName,
            name: row.parentTableName,
            type: row.parentTableType
        }));

        linkArray.push({
            source: row.parentTableName,
            target: row.childTableName,
            relationship: row.relationship
        });
    });

    const newNodes = Array.from(nodeSet).map(nodeStr => JSON.parse(nodeStr));
    const newLinks = linkArray;

    // Preserve positions of existing nodes
    newNodes.forEach(newNode => {
        const existingNode = nodes.find(n => n.id === newNode.id);
        if (existingNode) {
            // Keep existing position and fix it
            newNode.x = existingNode.x;
            newNode.y = existingNode.y;
            newNode.fx = existingNode.fx;
            newNode.fy = existingNode.fy;
        } else if (nodePositions.has(newNode.id)) {
            // Use stored position
            const pos = nodePositions.get(newNode.id);
            newNode.x = pos.x;
            newNode.y = pos.y;
            newNode.fx = pos.x;
            newNode.fy = pos.y;
        }
    });

    nodes = newNodes;
    links = newLinks;

    // Update simulation
    simulation.nodes(nodes);
    simulation.force("link").links(links);

    // Update links
    const link = linkGroup.selectAll(".link")
        .data(links, d => `${d.source.id || d.source}-${d.target.id || d.target}-${d.relationship}`);

    link.exit().remove();

    const linkEnter = link.enter().append("line")
        .attr("class", d => `link ${relationshipColors[d.relationship]}`)
        .attr("marker-end", "url(#arrowhead)")
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);

            const sourceNode = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
            const targetNode = typeof d.target === 'object' ? d.target : nodes.find(n => n.id === d.target);

            const tooltipHtml = `
                        <strong>Relationship:</strong> ${d.relationship}<br>
                        <strong>Parent:</strong> ${sourceNode ? sourceNode.name : 'Unknown'} (${sourceNode ? sourceNode.type : 'Unknown'})<br>
                        <strong>Child:</strong> ${targetNode ? targetNode.name : 'Unknown'} (${targetNode ? targetNode.type : 'Unknown'})
                    `;

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
        .data(nodes, d => d.id);

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
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);

            const parents = links
                .filter(l => l.target === d.id || (typeof l.target === 'object' && l.target.id === d.id))
                .map(l => typeof l.source === 'object' ? l.source : nodes.find(n => n.id === l.source))
                .filter(n => n);

            const children = links
                .filter(l => l.source === d.id || (typeof l.source === 'object' && l.source.id === d.id))
                .map(l => typeof l.target === 'object' ? l.target : nodes.find(n => n.id === l.target))
                .filter(n => n);

            let html = `<strong>Name:</strong> ${d.name}<br><strong>Type:</strong> ${d.type}<br>`;

            if (parents.length) {
                html += `<strong>Parents:</strong><br>`;
                html += parents.map(p => `• ${p.name} (${p.type})`).join('<br>') + '<br>';
            }
            if (children.length) {
                html += `<strong>Children:</strong><br>`;
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
        linkGroup.selectAll(".link")
            .attr("x1", d => {
                const source = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
                return source ? source.x : 0;
            })
            .attr("y1", d => {
                const source = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
                return source ? source.y : 0;
            })
            .attr("x2", d => {
                const target = typeof d.target === 'object' ? d.target : nodes.find(n => n.id === d.target);
                return target ? target.x : 0;
            })
            .attr("y2", d => {
                const target = typeof d.target === 'object' ? d.target : nodes.find(n => n.id === d.target);
                return target ? target.y : 0;
            });

        nodeUpdate.attr("transform", d => `translate(${d.x},${d.y})`);
    });

    // Only restart simulation if we have new nodes that need positioning
    const hasNewNodes = nodes.some(n => n.x === undefined || n.y === undefined);
    if (hasNewNodes) {
        simulation.alpha(0.3).restart();
    } else {
        simulation.alpha(0).restart();
    }

    updateStatus();
}

function updateStatus() {
    const filteredData = getFilteredData();
    const totalNodes = originalNodes.length;
    const visibleNodes = nodes.length;
    const filteredNodes = totalNodes - visibleNodes;

    document.getElementById('total-count').textContent = totalNodes;
    document.getElementById('visible-count').textContent = visibleNodes;
    document.getElementById('filtered-count').textContent = filteredNodes;
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

function displayTable(data) {
    const tableBody = document.getElementById('lineage-table').querySelector('tbody');
    tableBody.innerHTML = '';

    data.forEach(row => {
        const tr = tableBody.insertRow();
        tr.insertCell().textContent = row.childTableName;
        tr.insertCell().textContent = row.childTableType;
        tr.insertCell().textContent = row.relationship;
        tr.insertCell().textContent = row.parentTableName;
        tr.insertCell().textContent = row.parentTableType;
    });
}

function resetAllFilters() {
    Object.keys(columnFilters).forEach(header => {
        columnFilters[header].selected = [...columnFilters[header].options];

        // Update checkboxes
        document.querySelectorAll(`#options-${header} input[type="checkbox"]`).forEach(checkbox => {
            checkbox.checked = true;
        });

        updateDropdownText(header);
    });

    updateGraph();
}

// Drag functions with position persistence
function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.1).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}

function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    // Keep the node fixed at its final position
    d.fx = event.x;
    d.fy = event.y;
    // Store position for later use
    nodePositions.set(d.id, { x: event.x, y: event.y });
}

// Close dropdowns when clicking outside
document.addEventListener('click', function (event) {
    if (!event.target.closest('.multiselect')) {
        document.querySelectorAll('.multiselect-options').forEach(opt => {
            opt.classList.remove('show');
            opt.previousElementSibling.classList.remove('open');
        });
    }
});

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

// Initialize everything when the page loads
document.addEventListener('DOMContentLoaded', function () {
    initializeData();
});