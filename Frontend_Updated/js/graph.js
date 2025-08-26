let nodes = [];
let links = [];
let columnFilters = {};
let relationshipTypes = [];
let relationshipColors = {};
let nodePositions = new Map(); // Store fixed positions
let hiddenNodes = new Set(); // Track individually hidden nodes
let hiddenNodeTypes = new Set(); // Track hidden node types
let hiddenRelationshipTypes = new Set(); // Track hidden relationship types
let csvHeaders = []; // Store CSV headers for dynamic table display

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

// Helper function to determine if a node is visible based on filters and hiding
function isNodeVisible(nodeId) {
    const node = originalNodes.find(n => n.id === nodeId);
    if (!node) return false;
    
    // Check if node passes column filters
    const filteredData = getFilteredData();
    const nodeInFilteredData = filteredData.some(row => 
        row.childTableName === nodeId || row.parentTableName === nodeId
    );
    
    if (!nodeInFilteredData) return false;
    
    // Check hiding states
    const isTypeHidden = hiddenNodeTypes.has(node.type);
    const isIndividuallyHidden = hiddenNodes.has(nodeId);
    
    return !isTypeHidden && !isIndividuallyHidden;
}

function initializeData() {
    const parsed = parseCsvData(csvData);
    originalData = parsed.data;
    csvHeaders = parsed.headers; // Store headers for table display

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

    // Initialize column filters with all values selected
    parsed.headers.forEach(header => {
        const uniqueValues = [...new Set(originalData.map(row => row[header]))];
        columnFilters[header] = {
            selected: [...uniqueValues],
            options: uniqueValues,
            allOptions: [...uniqueValues]
        };
    });

    createFilterControls(parsed.headers);
    createNodeHidingControls();
    createTableHeaders(); // Create dynamic table headers
    updateGraph();
    updateRelationshipLegend();
    displayTable();
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
        selectedText.className = 'selected-text';

        const selectedCount = document.createElement('span');
        selectedCount.className = 'selected-count';
        selectedCount.id = `count-${header}`;

        dropdown.appendChild(selectedText);
        dropdown.appendChild(selectedCount);

        const options = document.createElement('div');
        options.className = 'multiselect-options';
        options.id = `options-${header}`;

        // Add "Select All" option at the top
        const selectAllDiv = document.createElement('div');
        selectAllDiv.className = 'multiselect-option select-all';

        const selectAllCheckbox = document.createElement('input');
        selectAllCheckbox.type = 'checkbox';
        selectAllCheckbox.id = `selectAll-${header}`;
        selectAllCheckbox.checked = true;
        selectAllCheckbox.addEventListener('change', (e) => {
            handleSelectAll(header, e.target.checked);
        });

        const selectAllLabel = document.createElement('span');
        selectAllLabel.textContent = 'Select All';

        selectAllDiv.appendChild(selectAllCheckbox);
        selectAllDiv.appendChild(selectAllLabel);
        options.appendChild(selectAllDiv);

        multiselect.appendChild(dropdown);
        multiselect.appendChild(options);
        filterGroup.appendChild(label);
        filterGroup.appendChild(multiselect);
        container.appendChild(filterGroup);

        // Initial population of options
        updateFilterOptions(header);
        updateDropdownText(header);
    });
}

function createNodeHidingControls() {
    const container = document.getElementById('filter-controls');
    
    // Add node hiding section
    const hidingSection = document.createElement('div');
    hidingSection.className = 'section';
    hidingSection.innerHTML = `
        <h4>Node Controls</h4>
        <div class="node-controls">
            <div id="node-type-buttons" class="type-buttons"></div>
            <div id="individual-node-buttons" class="individual-buttons"></div>
        </div>
    `;
    
    container.appendChild(hidingSection);
    
    updateNodeControls();
}

function updateNodeControls() {
    // Update node type buttons
    const nodeTypes = [...new Set(originalNodes.map(n => n.type))];
    const nodeTypeContainer = document.getElementById('node-type-buttons');
    nodeTypeContainer.innerHTML = '<strong>By Type:</strong><br>';

    nodeTypes.forEach(type => {
        const button = document.createElement('button');
        const isTypeHidden = hiddenNodeTypes.has(type);
        const nodesOfType = originalNodes.filter(n => n.type === type);
        const visibleNodesOfType = nodesOfType.filter(n => isNodeVisible(n.id));
        
        button.className = `btn btn-type-control ${isTypeHidden ? 'btn-show' : 'btn-hide'}`;
        button.textContent = isTypeHidden ? `Show ${type}s` : `Hide ${type}s`;
        button.onclick = () => toggleNodeType(type);
        nodeTypeContainer.appendChild(button);
    });

    // Update individual node buttons for visible filtered nodes
    const filteredData = getFilteredData();
    const filteredNodeIds = new Set();
    
    filteredData.forEach(row => {
        filteredNodeIds.add(row.childTableName);
        filteredNodeIds.add(row.parentTableName);
    });

    const individualContainer = document.getElementById('individual-node-buttons');
    individualContainer.innerHTML = '<br><strong>Individual Nodes:</strong><br>';
    
    Array.from(filteredNodeIds).forEach(nodeId => {
        const node = originalNodes.find(n => n.id === nodeId);
        if (!node) return;
        
        const button = document.createElement('button');
        const isHidden = hiddenNodes.has(nodeId);
        
        button.className = `btn btn-individual ${isHidden ? 'btn-show' : 'btn-hide'}`;
        button.textContent = `${isHidden ? 'Show' : 'Hide'} ${node.name}`;
        button.onclick = () => toggleIndividualNode(nodeId);
        individualContainer.appendChild(button);
    });
}

function handleSelectAll(header, checked) {
    const availableOptions = getFilteredOptionsForColumn(header);
    
    if (checked) {
        columnFilters[header].selected = [...availableOptions];
    } else {
        columnFilters[header].selected = [];
    }
    
    const optionsContainer = document.getElementById(`options-${header}`);
    const checkboxes = optionsContainer.querySelectorAll('.multiselect-option:not(.select-all) input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
        checkbox.checked = checked;
    });

    updateDropdownText(header);
    updateGraph();
    displayTable();
    refreshAllFilterOptions();
    updateNodeControls();
}

function getFilteredOptionsForColumn(targetHeader) {
    let filteredData = originalData;

    Object.entries(columnFilters).forEach(([header, filter]) => {
        if (header !== targetHeader && filter.selected.length > 0) {
            filteredData = filteredData.filter(row => filter.selected.includes(row[header]));
        }
    });

    return [...new Set(filteredData.map(row => row[targetHeader]))].sort();
}

function updateFilterOptions(header) {
    const availableOptions = getFilteredOptionsForColumn(header);
    const optionsContainer = document.getElementById(`options-${header}`);
    
    const existingOptions = optionsContainer.querySelectorAll('.multiselect-option:not(.select-all)');
    existingOptions.forEach(option => option.remove());

    availableOptions.forEach(option => {
        const optionDiv = document.createElement('div');
        optionDiv.className = 'multiselect-option';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = option;
        checkbox.checked = columnFilters[header].selected.includes(option);
        checkbox.addEventListener('change', (e) => {
            updateFilter(header, option, e.target.checked);
        });

        const label = document.createElement('span');
        label.textContent = option;

        optionDiv.appendChild(checkbox);
        optionDiv.appendChild(label);
        optionsContainer.appendChild(optionDiv);
    });

    columnFilters[header].options = availableOptions;
    
    const selectAllCheckbox = document.getElementById(`selectAll-${header}`);
    const selectedCount = columnFilters[header].selected.filter(item => availableOptions.includes(item)).length;
    const allSelected = selectedCount === availableOptions.length && availableOptions.length > 0;
    const noneSelected = selectedCount === 0;
    
    selectAllCheckbox.checked = allSelected;
    selectAllCheckbox.indeterminate = !allSelected && !noneSelected;
}

function refreshAllFilterOptions() {
    Object.keys(columnFilters).forEach(header => {
        updateFilterOptions(header);
        updateDropdownText(header);
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

    const availableOptions = getFilteredOptionsForColumn(header);
    const selectAllCheckbox = document.getElementById(`selectAll-${header}`);
    const selectedCount = columnFilters[header].selected.filter(item => availableOptions.includes(item)).length;
    const allSelected = selectedCount === availableOptions.length && availableOptions.length > 0;
    const noneSelected = selectedCount === 0;
    
    selectAllCheckbox.checked = allSelected;
    selectAllCheckbox.indeterminate = !allSelected && !noneSelected;

    updateDropdownText(header);
    updateGraph();
    displayTable();
    refreshAllFilterOptions();
    updateNodeControls();
}

function toggleDropdown(header) {
    const options = document.getElementById(`options-${header}`);
    const isOpen = options.classList.contains('show');
    
    document.querySelectorAll('.multiselect-options').forEach(opt => {
        opt.classList.remove('show');
        opt.previousElementSibling.classList.remove('open');
    });

    if (!isOpen) {
        updateFilterOptions(header);
        options.classList.add('show');
        options.previousElementSibling.classList.add('open');
    }
}

function updateDropdownText(header) {
    const selectedText = document.getElementById(`selected-${header}`);
    const selectedCount = document.getElementById(`count-${header}`);
    const availableOptions = getFilteredOptionsForColumn(header);
    const selected = columnFilters[header].selected.filter(item => availableOptions.includes(item));
    const total = availableOptions.length;

    if (selected.length === total && total > 0) {
        selectedText.textContent = 'All selected';
    } else if (selected.length === 0) {
        selectedText.textContent = 'None selected';
    } else if (selected.length === 1) {
        selectedText.textContent = selected[0];
    } else {
        selectedText.textContent = 'Multiple selected';
    }

    selectedCount.textContent = `${selected.length}/${total}`;
}

function getFilteredData() {
    return originalData.filter(row => {
        return Object.keys(columnFilters).every(header => {
            if (columnFilters[header].selected.length === 0) {
                return true;
            }
            return columnFilters[header].selected.includes(row[header]);
        });
    });
}

// Create dynamic table headers based on CSV headers
function createTableHeaders() {
    const table = document.getElementById('lineage-table');
    const thead = table.querySelector('thead');
    
    // Clear existing headers
    thead.innerHTML = '';
    
    const headerRow = thead.insertRow();
    csvHeaders.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header.charAt(0).toUpperCase() + header.slice(1).replace(/([A-Z])/g, ' $1');
        headerRow.appendChild(th);
    });
}

// Updated displayTable function to show all columns
function displayTable() {
    const filteredData = getFilteredData();
    const tableBody = document.getElementById('lineage-table').querySelector('tbody');
    tableBody.innerHTML = '';

    filteredData.forEach(row => {
        const tr = tableBody.insertRow();
        // Add cells for all columns in the CSV
        csvHeaders.forEach(header => {
            const cell = tr.insertCell();
            cell.textContent = row[header] || ''; // Handle missing values
        });
    });
}

function calculateLinks() {
    const filteredData = getFilteredData();
    const visibleNodeIds = new Set();
    
    // Get all nodes that should be visible based on filters and hiding
    filteredData.forEach(row => {
        if (isNodeVisible(row.childTableName)) {
            visibleNodeIds.add(row.childTableName);
        }
        if (isNodeVisible(row.parentTableName)) {
            visibleNodeIds.add(row.parentTableName);
        }
    });

    const resultLinks = [];

    // Add direct links between visible nodes
    filteredData.forEach(row => {
        if (visibleNodeIds.has(row.parentTableName) && 
            visibleNodeIds.has(row.childTableName) &&
            !hiddenRelationshipTypes.has(row.relationship)) {
            resultLinks.push({
                source: row.parentTableName,
                target: row.childTableName,
                relationship: row.relationship,
                type: 'direct'
            });
        }
    });

    // Add indirect links when nodes are hidden (connect parent's parent to hidden node's children)
    const allFilteredNodeIds = new Set();
    filteredData.forEach(row => {
        allFilteredNodeIds.add(row.childTableName);
        allFilteredNodeIds.add(row.parentTableName);
    });

    for (const hiddenNodeId of allFilteredNodeIds) {
        if (isNodeVisible(hiddenNodeId)) continue; // Skip visible nodes
        
        const incomingLinks = filteredData.filter(row => row.childTableName === hiddenNodeId);
        const outgoingLinks = filteredData.filter(row => row.parentTableName === hiddenNodeId);

        for (const incoming of incomingLinks) {
            for (const outgoing of outgoingLinks) {
                if (visibleNodeIds.has(incoming.parentTableName) &&
                    visibleNodeIds.has(outgoing.childTableName) &&
                    !hiddenRelationshipTypes.has(incoming.relationship)) {

                    const exists = resultLinks.some(l =>
                        l.source === incoming.parentTableName && 
                        l.target === outgoing.childTableName && 
                        l.relationship === incoming.relationship
                    );

                    if (!exists) {
                        resultLinks.push({
                            source: incoming.parentTableName,
                            target: outgoing.childTableName,
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

function updateGraph() {
    const filteredData = getFilteredData();

    // Create nodes from filtered and visible data
    const nodeSet = new Set();
    const visibleLinks = calculateLinks();

    // Add nodes that appear in visible links
    visibleLinks.forEach(link => {
        const sourceNode = originalNodes.find(n => n.id === link.source);
        const targetNode = originalNodes.find(n => n.id === link.target);
        
        if (sourceNode) {
            nodeSet.add(JSON.stringify(sourceNode));
        }
        if (targetNode) {
            nodeSet.add(JSON.stringify(targetNode));
        }
    });

    const newNodes = Array.from(nodeSet).map(nodeStr => JSON.parse(nodeStr));

    // Preserve positions of existing nodes
    newNodes.forEach(newNode => {
        const existingNode = nodes.find(n => n.id === newNode.id);
        if (existingNode) {
            newNode.x = existingNode.x;
            newNode.y = existingNode.y;
            newNode.fx = existingNode.fx;
            newNode.fy = existingNode.fy;
        } else if (nodePositions.has(newNode.id)) {
            const pos = nodePositions.get(newNode.id);
            newNode.x = pos.x;
            newNode.y = pos.y;
            newNode.fx = pos.x;
            newNode.fy = pos.y;
        }
    });

    nodes = newNodes;
    links = visibleLinks;

    // Update simulation
    simulation.nodes(nodes);
    simulation.force("link").links(links);

    // Update links
    const link = linkGroup.selectAll(".link")
        .data(links, d => `${d.source.id || d.source}-${d.target.id || d.target}-${d.relationship}`);

    link.exit().remove();

    const linkEnter = link.enter().append("line")
        .attr("class", d => `link ${relationshipColors[d.relationship]} ${d.type}`)
        .attr("marker-end", "url(#arrowhead)")
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);

            const sourceNode = typeof d.source === 'object' ? d.source : nodes.find(n => n.id === d.source);
            const targetNode = typeof d.target === 'object' ? d.target : nodes.find(n => n.id === d.target);

            let tooltipHtml = `
                <strong>Relationship:</strong> ${d.relationship}<br>
                <strong>Parent:</strong> ${sourceNode ? sourceNode.name : 'Unknown'} (${sourceNode ? sourceNode.type : 'Unknown'})<br>
                <strong>Child:</strong> ${targetNode ? targetNode.name : 'Unknown'} (${targetNode ? targetNode.type : 'Unknown'})<br>
                <strong>Connection:</strong> ${d.type}
            `;
            
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
        .on("click", (event, d) => {
            event.stopPropagation();
            toggleIndividualNode(d.id);
        })
        .on("mouseover", function (event, d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);

            const parents = links
                .filter(l => (l.target === d.id || (typeof l.target === 'object' && l.target.id === d.id)) && l.type === 'direct')
                .map(l => typeof l.source === 'object' ? l.source : nodes.find(n => n.id === l.source))
                .filter(n => n);

            const children = links
                .filter(l => (l.source === d.id || (typeof l.source === 'object' && l.source.id === d.id)) && l.type === 'direct')
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

            html += '<br><em>Click to hide/show this node</em>';

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

function toggleNodeType(nodeType) {
    const wasHidden = hiddenNodeTypes.has(nodeType);
    
    if (wasHidden) {
        hiddenNodeTypes.delete(nodeType);
        // Remove individual hiding for nodes of this type to ensure they show
        originalNodes.filter(n => n.type === nodeType).forEach(node => {
            hiddenNodes.delete(node.id);
        });
    } else {
        hiddenNodeTypes.add(nodeType);
    }
    
    updateGraph();
    updateNodeControls();
}

function toggleIndividualNode(nodeId) {
    if (hiddenNodes.has(nodeId)) {
        hiddenNodes.delete(nodeId);
    } else {
        hiddenNodes.add(nodeId);
    }
    
    updateGraph();
    updateNodeControls();
}

function updateStatus() {
    const filteredData = getFilteredData();
    const totalFilteredNodes = new Set();
    
    filteredData.forEach(row => {
        totalFilteredNodes.add(row.childTableName);
        totalFilteredNodes.add(row.parentTableName);
    });
    
    const visibleNodes = Array.from(totalFilteredNodes).filter(nodeId => isNodeVisible(nodeId));

    document.getElementById('total-count').textContent = originalNodes.length;
    document.getElementById('visible-count').textContent = visibleNodes.length;
    document.getElementById('filtered-count').textContent = originalNodes.length - visibleNodes.length;
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

function resetAllFilters() {
    // Reset column filters
    Object.keys(columnFilters).forEach(header => {
        columnFilters[header].selected = [...columnFilters[header].allOptions];
    });

    // Reset hiding states
    hiddenNodes.clear();
    hiddenNodeTypes.clear();
    hiddenRelationshipTypes.clear();

    // Refresh all filter options and update UI
    refreshAllFilterOptions();
    
    // Update all "Select All" checkboxes
    Object.keys(columnFilters).forEach(header => {
        const selectAllCheckbox = document.getElementById(`selectAll-${header}`);
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = true;
            selectAllCheckbox.indeterminate = false;
        }
    });

    updateGraph();
    displayTable();
    updateNodeControls();
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
    d.fx = event.x;
    d.fy = event.y;
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