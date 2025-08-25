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

    // Initialize column filters with all values selected
    parsed.headers.forEach(header => {
        const uniqueValues = [...new Set(originalData.map(row => row[header]))];
        columnFilters[header] = {
            selected: [...uniqueValues],
            options: uniqueValues,
            allOptions: [...uniqueValues] // Keep original options for reference
        };
    });

    createFilterControls(parsed.headers);
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

function handleSelectAll(header, checked) {
    const availableOptions = getFilteredOptionsForColumn(header);
    
    if (checked) {
        // Select all available options
        columnFilters[header].selected = [...availableOptions];
    } else {
        // Deselect all
        columnFilters[header].selected = [];
    }
    
    // Update all checkboxes in this dropdown
    const optionsContainer = document.getElementById(`options-${header}`);
    const checkboxes = optionsContainer.querySelectorAll('.multiselect-option:not(.select-all) input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
        checkbox.checked = checked;
    });

    updateDropdownText(header);
    updateGraph();
    displayTable();
    refreshAllFilterOptions();
}

function getFilteredOptionsForColumn(targetHeader) {
    let filteredData = originalData;

    // Apply filters from other columns (not the target column)
    Object.entries(columnFilters).forEach(([header, filter]) => {
        if (header !== targetHeader && filter.selected.length > 0) {
            filteredData = filteredData.filter(row => filter.selected.includes(row[header]));
        }
    });

    // Get unique values for target column from filtered data
    return [...new Set(filteredData.map(row => row[targetHeader]))].sort();
}

function updateFilterOptions(header) {
    const availableOptions = getFilteredOptionsForColumn(header);
    const optionsContainer = document.getElementById(`options-${header}`);
    
    // Remove existing individual options (keep Select All)
    const existingOptions = optionsContainer.querySelectorAll('.multiselect-option:not(.select-all)');
    existingOptions.forEach(option => option.remove());

    // Add individual options based on filtered data
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

    // Update the available options in columnFilters
    columnFilters[header].options = availableOptions;
    
    // Update "Select All" checkbox state
    const selectAllCheckbox = document.getElementById(`selectAll-${header}`);
    const selectedCount = columnFilters[header].selected.filter(item => availableOptions.includes(item)).length;
    const allSelected = selectedCount === availableOptions.length && availableOptions.length > 0;
    const noneSelected = selectedCount === 0;
    
    selectAllCheckbox.checked = allSelected;
    selectAllCheckbox.indeterminate = !allSelected && !noneSelected;
}

function refreshAllFilterOptions() {
    // Update available options for all dropdowns based on current filters
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

    // Update "Select All" checkbox state
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
}

function toggleDropdown(header) {
    const options = document.getElementById(`options-${header}`);
    const isOpen = options.classList.contains('show');
    
    // Close all dropdowns first
    document.querySelectorAll('.multiselect-options').forEach(opt => {
        opt.classList.remove('show');
        opt.previousElementSibling.classList.remove('open');
    });

    // Toggle current dropdown
    if (!isOpen) {
        // Update options before showing
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
        selectedText.textContent = `Multiple selected`;
    }

    selectedCount.textContent = `${selected.length}/${total}`;
}

// Get filtered data based on current filter selections
function getFilteredData() {
    return originalData.filter(row => {
        return Object.keys(columnFilters).every(header => {
            // If no items are selected for this column, don't filter by it
            if (columnFilters[header].selected.length === 0) {
                return true;
            }
            return columnFilters[header].selected.includes(row[header]);
        });
    });
}

// Update displayTable to use filtered data
function displayTable() {
    const filteredData = getFilteredData();
    const tableBody = document.getElementById('lineage-table').querySelector('tbody');
    tableBody.innerHTML = '';

    filteredData.forEach(row => {
        const tr = tableBody.insertRow();
        tr.insertCell().textContent = row.childTableName;
        tr.insertCell().textContent = row.childTableType;
        tr.insertCell().textContent = row.relationship;
        tr.insertCell().textContent = row.parentTableName;
        tr.insertCell().textContent = row.parentTableType;
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

function resetAllFilters() {
    // Reset all filters to select all original options
    Object.keys(columnFilters).forEach(header => {
        columnFilters[header].selected = [...columnFilters[header].allOptions];
    });

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