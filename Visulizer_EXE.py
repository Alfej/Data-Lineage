import pandas as pd
import networkx as nx
import json
import webbrowser
import os
import sys
import http.server
import socketserver
import threading
import time

# HTML template string
print("📥 Loading CSV data...")
data = pd.read_csv("C:\\DataLineage\\parent_child_lineage.csv")
df = pd.DataFrame(data)
print(f"✅ Loaded CSV with shape: {df.shape}")
CSS_CONTENT = '''
<style>
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    margin: 0;
    padding: 20px;
    background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    min-height: 100vh;
}

h1 {
    text-align: center;
    color: #2c3e50;
    margin-bottom: 10px;
    text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
}

.container {
    display: flex;
    gap: 20px;
    max-width: 1400px;
    margin: 0 auto;
}

.controls {
    width: 350px;
    background: rgba(255, 255, 255, 0.95);
    padding: 20px;
    border-radius: 12px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    backdrop-filter: blur(10px);
    height: fit-content;
    max-height: 80vh;
    overflow-y: auto;
    border: 1px solid rgba(255,255,255,0.2);
}

.graph-container {
    flex: 1;
    background: rgba(255, 255, 255, 0.95);
    border-radius: 12px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    backdrop-filter: blur(10px);
    position: relative;
    border: 1px solid rgba(255,255,255,0.2);
    overflow: visible;
}

#graph {
    width: 100%;
    height: 100%;
    border-radius: 12px;
}

.node {
    cursor: pointer;
    stroke-width: 3px;
    transition: all 0.3s ease;
    filter: drop-shadow(2px 2px 4px rgba(0,0,0,0.2));
}

.node:hover {
    stroke-width: 5px;
    filter: drop-shadow(4px 4px 8px rgba(0,0,0,0.3)) brightness(1.1);
    transform: scale(1.1);
}

.node.cte {
    fill: #4A90E2;
    stroke: #2E5FCC;
}

.node.table {
    fill: #7ED321;
    stroke: #5CB818;
}

.node.view {
    fill: #F5A623;
    stroke: #D1890B;
}

.link {
    stroke-width: 3px;
    fill: none;
    transition: all 0.3s ease;
    filter: drop-shadow(1px 1px 2px rgba(0,0,0,0.1));
}

.link:hover {
    stroke-width: 5px;
    filter: drop-shadow(2px 2px 4px rgba(0,0,0,0.2));
}

/* Relationship-specific colors */
.link.relationship-0 { stroke: #E74C3C; } /* Red */
.link.relationship-1 { stroke: #3498DB; } /* Blue */
.link.relationship-2 { stroke: #2ECC71; } /* Green */
.link.relationship-3 { stroke: #F39C12; } /* Orange */
.link.relationship-4 { stroke: #9B59B6; } /* Purple */
.link.relationship-5 { stroke: #1ABC9C; } /* Turquoise */
.link.relationship-6 { stroke: #34495E; } /* Dark Gray */
.link.relationship-7 { stroke: #E67E22; } /* Carrot */
.link.relationship-8 { stroke: #95A5A6; } /* Gray */
.link.relationship-9 { stroke: #8E44AD; } /* Violet */

.link.indirect {
    stroke-dasharray: 8,4;
    opacity: 0.7;
}

.node-label {
    font-size: 11px;
    font-weight: 600;
    text-anchor: middle;
    pointer-events: none;
    fill: #2c3e50;
    text-shadow: 1px 1px 2px rgba(255,255,255,0.8);
}

.tooltip {
    position: absolute;
    background: rgba(44, 62, 80, 0.95);
    color: white;
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 13px;
    pointer-events: none;
    z-index: 1000;
    max-width: 320px;
    word-wrap: break-word;
    box-shadow: 0 4px 16px rgba(0,0,0,0.3);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,0.1);
}

.tooltip strong {
    color: #3498DB;
}

.btn {
    display: inline-block;
    padding: 8px 14px;
    margin: 3px 2px;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-size: 12px;
    font-weight: 500;
    text-align: center;
    text-decoration: none;
    transition: all 0.3s ease;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
}

.btn:active {
    transform: translateY(0);
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.btn-reset {
    background: linear-gradient(135deg, #F39C12, #E67E22);
    color: white;
    width: 100%;
    margin-top: 15px;
    font-weight: 600;
}

.section {
    margin-top: 20px;
    padding: 16px;
    background: rgba(248, 249, 250, 0.8);
    border-radius: 8px;
    border-left: 4px solid #3498DB;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}

.section h4 {
    margin-top: 0;
    margin-bottom: 12px;
    color: #2c3e50;
}

.filter-group {
    margin-bottom: 15px;
}

.filter-group label {
    display: block;
    margin-bottom: 5px;
    font-weight: 600;
    color: #2c3e50;
    font-size: 13px;
}

.multiselect {
    position: relative;
    width: 100%;
}

.multiselect-dropdown {
    width: 100%;
    padding: 10px;
    border: 2px solid #ddd;
    border-radius: 6px;
    background: white;
    cursor: pointer;
    font-size: 13px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: border-color 0.3s ease;
}

.multiselect-dropdown:hover {
    border-color: #3498DB;
}

.multiselect-dropdown::after {
    content: '▼';
    font-size: 12px;
    color: #666;
    transition: transform 0.3s ease;
}

.multiselect-dropdown.open::after {
    content: '▲';
    transform: rotate(180deg);
}

.multiselect-options {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    background: white;
    border: 2px solid #ddd;
    border-top: none;
    border-radius: 0 0 6px 6px;
    max-height: 200px;
    overflow-y: auto;
    z-index: 1000;
    display: none;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

.multiselect-options.show {
    display: block;
    animation: slideDown 0.2s ease-out;
}

@keyframes slideDown {
    from {
        opacity: 0;
        transform: translateY(-5px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.multiselect-option {
    padding: 10px;
    cursor: pointer;
    font-size: 13px;
    border-bottom: 1px solid #eee;
    display: flex;
    align-items: center;
    transition: background-color 0.2s ease;
}

.multiselect-option:hover {
    background-color: #f5f5f5;
}

.multiselect-option:last-child {
    border-bottom: none;
}

.multiselect-option input[type="checkbox"] {
    margin-right: 8px;
    cursor: pointer;
    accent-color: #3498DB;
}

/* Enhanced styling for indeterminate checkboxes */
.multiselect-option input[type="checkbox"]:indeterminate {
    accent-color: #F39C12;
}

.multiselect-option input[type="checkbox"]:indeterminate::before {
    content: '−';
    color: white;
    font-weight: bold;
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
}

.selected-count {
    background: #3498DB;
    color: white;
    padding: 2px 6px;
    border-radius: 12px;
    font-size: 11px;
    margin-left: 5px;
    font-weight: 600;
}

.status {
    margin-top: 20px;
    padding: 12px;
    background: rgba(236, 240, 241, 0.9);
    border-radius: 6px;
    font-size: 13px;
    color: #2c3e50;
    border-left: 4px solid #3498DB;
}

.legend {
    margin-top: 20px;
    padding: 16px;
    background: rgba(248, 249, 250, 0.9);
    border-radius: 8px;
    border-left: 4px solid #2ECC71;
    font-size: 12px;
    color: #2c3e50;
}

.relationship-legend-item {
    display: flex;
    align-items: center;
    margin: 4px 0;
    font-size: 11px;
}

.relationship-color-box {
    width: 20px;
    height: 3px;
    margin-right: 8px;
    border-radius: 2px;
}

#lineage-table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 20px;
    font-family: sans-serif;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
}

#lineage-table th,
#lineage-table td {
    border: 1px solid #ddd;
    padding: 12px 10px;
    text-align: left;
}

#lineage-table thead th {
    background-color: #f2f2f2;
    color: #333;
    font-weight: bold;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

#lineage-table tbody tr:nth-child(even) {
    background-color: #f9f9f9;
}

#lineage-table tbody tr:hover {
    background-color: #e9e9e9;
    transition: background-color 0.2s ease;
}

#lineage-table tbody td {
    font-size: 13px;
}

#lineage-data {
    margin-top: 20px;
    padding: 15px;
    background-color: #fff;
    border-radius: 8px;
    box-shadow: 0 0 10px rgba(0,0,0,0.05);
}

#lineage-data h2 {
    margin-top: 0;
    color: #333;
    border-bottom: 2px solid #3498DB;
    padding-bottom: 10px;
    margin-bottom: 15px;
    font-size: 18px;
}

.multiselect-option.select-all {
    border-bottom: 2px solid #3498DB;
    font-weight: bold;
    padding: 12px 10px;
    background-color: #f8f9fa;
    position: sticky;
    top: 0;
    z-index: 1;
}

.multiselect-option.select-all:hover {
    background-color: #e9ecef;
}

.multiselect-option.select-all input[type="checkbox"] {
    transform: scale(1.1);
}

/* Scrollbar styling for dropdown */
.multiselect-options::-webkit-scrollbar {
    width: 6px;
}

.multiselect-options::-webkit-scrollbar-track {
    background: #f1f1f1;
}

.multiselect-options::-webkit-scrollbar-thumb {
    background: #888;
    border-radius: 3px;
}

.multiselect-options::-webkit-scrollbar-thumb:hover {
    background: #555;
}

/* Enhanced status indicators */
.status strong {
    color: #2c3e50;
}

.status span {
    font-weight: 600;
    color: #3498DB;
}

/* Improved button styling */
.btn-reset:hover {
    background: linear-gradient(135deg, #E67E22, #D35400);
    transform: translateY(-2px);
}

.btn-reset:active {
    transform: translateY(0);
}

/* Loading states and transitions */
.multiselect-dropdown.loading {
    opacity: 0.7;
    pointer-events: none;
}

.multiselect-dropdown.loading::after {
    content: '⟳';
    animation: spin 1s linear infinite;
}

@keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}

/* Responsive design improvements */
@media (max-width: 768px) {
    .container {
        flex-direction: column;
        gap: 15px;
    }
    
    .controls {
        width: 100%;
        max-height: none;
    }
    
    .multiselect-options {
        max-height: 150px;
    }
}

/* Focus states for accessibility */
.multiselect-dropdown:focus {
    outline: 2px solid #3498DB;
    outline-offset: 2px;
}

.multiselect-option input[type="checkbox"]:focus {
    outline: 2px solid #3498DB;
    outline-offset: 1px;
}

/* Enhanced table styling */
#lineage-table tbody tr {
    transition: all 0.2s ease;
}

#lineage-table tbody tr:hover {
    transform: translateX(2px);
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
}

</style>'''
JS_CONTENT = r'''
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
'''
data_script = f"""const csvData = {json.dumps(df.to_csv(index=False))};"""
HTML_TEMPLATE = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced Interactive Graph Visualization</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
        {CSS_CONTENT}
</head>
<body>
    <h1>Data Lineage Visualization</h1>
    <p>Use the column-based filters below to explore the data. Drag nodes to reposition them - they'll stay where you place them!</p>
    
    <div class="container">
        <div class="controls">
            <h3>Column Filters</h3>
            
            <!-- Dynamic filter controls will be inserted here -->
            <div id="filter-controls"></div>
            
            <button class="btn btn-reset" onclick="resetAllFilters()">🔄 Reset All Filters</button>
            
            <div class="status">
                <strong>Status:</strong><br>
                Total Nodes: <span id="total-count">0</span><br>
                Visible: <span id="visible-count">0</span><br>
                Filtered: <span id="filtered-count">0</span>
            </div>
            
            <div class="legend">
                <strong>Node Types:</strong><br>
                🔵 Blue circles = CTE<br>
                🟢 Green circles = Tables<br>
                🟡 Yellow circles = Views<br><br>
                
                <strong>Relationship Colors:</strong><br>
                <div id="relationship-legend"></div>
                <br>
                
                <strong>Interactions:</strong><br>
                • Drag nodes to reposition<br>
                • Hover for details<br>
                • Use filters to show/hide
            </div>
        </div>
        
        <div class="graph-container">
            <svg id="graph"></svg>
        </div>
    </div>
    
    <div id="lineage-data">
        <h2>Parent-Child Lineage</h2>
        <div class="table-container">
            <table id="lineage-table">
                <thead>
                    <tr>
                        <th>Child Table Name</th>
                        <th>Child Table Type</th>
                        <th>Relationship</th>
                        <th>Parent Table Name</th>
                        <th>Parent Table Type</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- Rows will be loaded here by JavaScript -->
                </tbody>
            </table>
        </div>
    </div>

    <script id="graph-data">
    {data_script}
    </script>
    
    <script>
    {JS_CONTENT}
    </script>
</body>
</html>

'''

# Read CSS and JS content - these will be embedded in the Python script

# Sample data
# Ensure the CSV is in the same directory as this script, or provide the full path.


def process_data():
    """Process CSV data and prepare for visualization"""
    
    # Create NetworkX graph
    G = nx.DiGraph()
    all_nodes = set(df['childTableName'].tolist() + df['parentTableName'].tolist())
    
    # Add nodes with types
    for node in all_nodes:
        if node in df['childTableName'].values:
            node_type = df[df['childTableName'] == node]['childTableType'].iloc[0]
        else:
            node_type = df[df['parentTableName'] == node]['parentTableType'].iloc[0]
        
        G.add_node(node, node_type=node_type)
    
    # Add edges with relationship information from CSV
    for _, row in df.iterrows():
        relationship = row.get('relationship', 'unknown')  # Use actual relationship column
        G.add_edge(
            row['childTableName'], 
            row['parentTableName'], 
            relationship=relationship
        )
    
    # Convert to data for D3.js
    nodes_data = []
    for node in G.nodes(data=True):
        display_name = node[0]
        
        # Get relationships for this node
        incoming_edges = []
        outgoing_edges = []
        
        for _, row in df.iterrows():
            if row['parentTableName'] == node[0]:  # This node is a parent
                relationship = row.get('relationship', 'depends on')
                child_name = row['childTableName'].split('.')[-1] if '.' in row['childTableName'] else row['childTableName']
                child_type = row['childTableType']
                incoming_edges.append(f"← {child_type}: {child_name} ({relationship})")
            
            if row['childTableName'] == node[0]:  # This node is a child
                relationship = row.get('relationship', 'depends on')
                parent_name = row['parentTableName'].split('.')[-1] if '.' in row['parentTableName'] else row['parentTableName']
                parent_type = row['parentTableType']
                outgoing_edges.append(f"→ {parent_type}: {parent_name} ({relationship})")
        
        nodes_data.append({
            'id': node[0],
            'name': display_name,
            'fullName': node[0],
            'type': node[1].get('node_type', 'Unknown'),
            'incoming': incoming_edges,
            'outgoing': outgoing_edges,
            'hidden': False
        })
    
    edges_data = []
    for _, row in df.iterrows():
        relationship = row.get('relationship', 'unknown')
        edges_data.append({
            'source': row['childTableName'],
            'target': row['parentTableName'],
            'relationship': relationship,
            'type': 'direct'
        })
    
    return nodes_data, edges_data

def get_base_path():
    """Get the base path for the application"""
    if getattr(sys, 'frozen', False):
        # If the application is run as a bundle (exe)
        return sys._MEIPASS
    else:
        # If the application is run from a Python interpreter
        return os.path.dirname(os.path.abspath(__file__))

def inject_data_into_html():
    """Create HTML file with embedded CSS and JS"""
    try:
        # Inject the data

        # Create temp directory if needed
        temp_dir = os.path.join(get_base_path(), 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        # Write the complete HTML file
        output_file = os.path.join(temp_dir, 'visualization.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(HTML_TEMPLATE)

        print(f"✅ Visualization HTML created at: {output_file}")
        return output_file

    except Exception as e:
        print(f"❌ Error creating visualization: {str(e)}")
        raise

# --- MODIFIED FUNCTION ---
def create_interactive_html_graph():
    """Main function to create interactive graph and serve it locally"""
    base_path = get_base_path()
    
    # Update directory to serve
    temp_dir = os.path.join(base_path, 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    
    # Process and create visualization
    nodes_data, edges_data = process_data()
    output_file = inject_data_into_html()
    
    # Server setup
    PORT = 8000
    DIRECTORY_TO_SERVE = os.path.dirname(output_file)
    FILE_TO_OPEN = os.path.basename(output_file)

    # Create a custom handler to serve files from the Frontend directory
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DIRECTORY_TO_SERVE, **kwargs)

    httpd = socketserver.TCPServer(("", PORT), Handler)
    
    # Start the server in a background thread
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    # Give the server a moment to start up
    time.sleep(1)

    url = f"http://localhost:{PORT}/{FILE_TO_OPEN}"
    webbrowser.open(url)

    print(f"\n✅ Enhanced interactive graph created!")
    print("📁 File structure:")
    print("   Frontend/")
    print("   ├── index.html              (Static template)")
    print("   ├── graph_visualization.html (Generated with data)")
    print("   ├── css/")
    print("   │   └── styles.css          (Static styles)")
    print("   └── js/")
    print("       └── graph.js            (Static logic)")

    print(f"\n🌐 Server running at: {url}")
    print("   The script has started a local web server.")
    
    try:
        # Keep the main script alive to allow the server thread to run
        input("   Press Enter or close this terminal to stop the server...\n")
    except KeyboardInterrupt:
        print("\nCaught keyboard interrupt, shutting down.")
    finally:
        # Cleanly shut down the server
        print("🛑 Stopping server...")
        httpd.shutdown()
        httpd.server_close()
        
    return str(output_file)

def check_csv_structure():
    """Check if CSV has required columns"""
    
    required_columns = ['childTableName', 'parentTableName', 'childTableType', 'parentTableType']
    
    print("🔍 Checking CSV structure...")
    print(f"📋 Available columns: {list(df.columns)}")
    
    missing_required = [col for col in required_columns if col not in df.columns]
    if missing_required:
        print(f"❌ Missing required columns: {missing_required}")
        return False
    
    if 'relationship' in df.columns:
        print("✅ Relationship column found!")
        print(f"🔗 Unique relationships: {list(df['relationship'].unique())}")
    else:
        print("⚠️  No 'relationship' column found. Will use 'unknown' as default.")
    
    print(f"📊 Data shape: {df.shape}")
    return True

if __name__ == "__main__":
    if check_csv_structure():
        create_interactive_html_graph()
    else:
        print("❌ Please check your CSV file structure and try again.")