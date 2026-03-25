# Geometry Helper

Draw a geometry on the map below to generate input for `fetch_tiles`.

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha384-sHL9NAb7lN7rfvG5lfHpm643Xkcjzp4jFvuavGOndn6pjVqS6ny56CAt3nsEVT4H" crossorigin="anonymous" />
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.css" integrity="sha384-NZLkVuBRMEeB4VeZz27WwTRvlhec30biQ8Xx7zG7JJnkvEKRg5qi6BNbEXo9ydwv" crossorigin="anonymous" />
<style>
.leaflet-draw-actions li a {
    background: rgba(50, 50, 50, 0.9);
    color: #f0f0f0;
    border-radius: 3px;
    font-size: 12px;
}
.leaflet-draw-actions li a:hover {
    background: rgba(80, 80, 80, 0.95);
}
#clear-btn {
    display: none;
    position: absolute;
    top: 10px;
    right: 10px;
    z-index: 1000;
    cursor: pointer;
    padding: 4px 10px;
    background: rgba(255, 255, 255, 0.95);
    border: 2px solid rgba(0, 0, 0, 0.2);
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    color: #444;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.15);
    transition: background 0.15s;
}
#clear-btn:hover {
    background: rgba(240, 240, 240, 0.95);
}
</style>

<div style="position: relative; margin-bottom: 16px;">
<div id="map" style="height: 500px; width: 100%; border: 1px solid #ccc; border-radius: 6px;"></div>
<button id="clear-btn" onclick="clearDrawing()">Clear</button>
</div>

<div id="output-section" style="display: none;">

<strong>GeoJSON</strong> <button onclick="copyText('geojson-output')" style="font-size: 12px; cursor: pointer; margin-left: 8px;">Copy</button>
<pre id="geojson-output" style="background: #1a1a2e; color: #e0e0e0; padding: 12px; border-radius: 4px; overflow-x: auto; user-select: all;"></pre>

<strong>Bounding Box</strong> <button onclick="copyText('bbox-output')" style="font-size: 12px; cursor: pointer; margin-left: 8px;">Copy</button>
<pre id="bbox-output" style="background: #1a1a2e; color: #e0e0e0; padding: 12px; border-radius: 4px; overflow-x: auto; user-select: all;"></pre>

<strong>WKT</strong> <button onclick="copyText('wkt-output')" style="font-size: 12px; cursor: pointer; margin-left: 8px;">Copy</button>
<pre id="wkt-output" style="background: #1a1a2e; color: #e0e0e0; padding: 12px; border-radius: 4px; overflow-x: auto; user-select: all;"></pre>

<div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-top: 24px;">

<h3 style="margin-top: 0; border-bottom: 1px solid #30363d; padding-bottom: 10px;">Usage examples</h3>

<div style="display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px;">
<div style="flex: 1; min-width: 200px;">
<label for="path-input" style="font-weight: bold; font-size: 13px;">Project directory</label>
<input id="path-input" type="text" value="/path/to/project" oninput="refreshExamples()" style="width: 100%; padding: 8px; margin-top: 4px; border: 1px solid #30363d; border-radius: 4px; background: #0d1117; color: #58a6ff; font-family: monospace; font-size: 14px; box-sizing: border-box; outline: none;" />
<span id="path-warning" style="display: none; color: #d29922; font-size: 12px; margin-top: 4px;"></span>
</div>
<div style="display: flex; align-items: flex-end; gap: 8px; padding-bottom: 2px;">
<label style="font-size: 13px; display: flex; align-items: center; gap: 4px; cursor: pointer;">
<input type="checkbox" id="include-build-vrt" onchange="refreshExamples()" /> Include build_vrt
</label>
</div>
</div>

<strong>Python</strong> <button onclick="copyText('python-output')" style="font-size: 12px; cursor: pointer; margin-left: 8px;">Copy</button>
<pre id="python-output" style="background: #0d1117; color: #e0e0e0; padding: 12px; border-radius: 4px; overflow-x: auto; user-select: all; border: 1px solid #30363d;"></pre>

<strong>CLI</strong> <button onclick="copyText('cli-output')" style="font-size: 12px; cursor: pointer; margin-left: 8px;">Copy</button>
<pre id="cli-output" style="background: #0d1117; color: #e0e0e0; padding: 12px; border-radius: 4px; overflow-x: auto; user-select: all; border: 1px solid #30363d;"></pre>

</div>

</div>

<p id="instructions" style="color: #888; font-style: italic;">Use the draw tools on the left side of the map to draw a rectangle or polygon.</p>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha384-cxOPjt7s7Iz04uaHJceBmS+qpjv2JkIHNVcuOrM+YHwZOmJGBXI00mdUXEq65HTH" crossorigin="anonymous"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.js" integrity="sha384-JP5UPxIO2Tm2o79Fb0tGYMa44jkWar53aBoCbd8ah0+LcCDoohTIYr+zIXyfGIJN" crossorigin="anonymous"></script>

<script>
var map = L.map('map').setView([30, -80], 5);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18
}).addTo(map);

var drawnItems = new L.FeatureGroup();
map.addLayer(drawnItems);

var drawControl = new L.Control.Draw({
    draw: {
        polygon: true,
        rectangle: true,
        circle: false,
        circlemarker: false,
        marker: false,
        polyline: false
    },
    edit: false
});
map.addControl(drawControl);

// Hide actions toolbar for rectangle (Cancel is useless for click-drag)
map.on('draw:drawstart', function(e) {
    if (e.layerType === 'rectangle') {
        document.querySelectorAll('.leaflet-draw-actions').forEach(function(el) {
            el.style.display = 'none';
        });
    }
});

function updateOutput(layer) {
    var outputSection = document.getElementById('output-section');
    var instructions = document.getElementById('instructions');
    outputSection.style.display = 'block';
    instructions.style.display = 'none';

    var geojson = layer.toGeoJSON();
    var coords = geojson.geometry.coordinates[0];

    // Bounding box
    var lngs = coords.map(function(c) { return c[0]; });
    var lats = coords.map(function(c) { return c[1]; });
    var xmin = Math.min.apply(null, lngs);
    var xmax = Math.max.apply(null, lngs);
    var ymin = Math.min.apply(null, lats);
    var ymax = Math.max.apply(null, lats);
    var bbox = xmin.toFixed(4) + ',' + ymin.toFixed(4) + ',' + xmax.toFixed(4) + ',' + ymax.toFixed(4);
    document.getElementById('bbox-output').textContent = bbox;

    // GeoJSON
    var geojsonStr = JSON.stringify(geojson.geometry);
    document.getElementById('geojson-output').textContent = geojsonStr;

    // WKT
    var wktCoords = coords.map(function(c) {
        return c[0].toFixed(6) + ' ' + c[1].toFixed(6);
    }).join(', ');
    var wkt = 'POLYGON((' + wktCoords + '))';
    document.getElementById('wkt-output').textContent = wkt;

    window._lastBbox = bbox;
    refreshExamples();
}

function validatePath(dir) {
    var warning = document.getElementById('path-warning');
    var input = document.getElementById('path-input');
    if (!dir.trim()) {
        warning.textContent = 'Path cannot be empty.';
        warning.style.display = 'block';
        input.style.borderColor = '#d29922';
        return false;
    }
    if (!/^(\/|~|[A-Za-z]:[\\\/]|\\\\)/.test(dir.trim())) {
        warning.textContent = 'Path should be absolute (start with /, ~, or a drive letter).';
        warning.style.display = 'block';
        input.style.borderColor = '#d29922';
        return false;
    }
    warning.style.display = 'none';
    input.style.borderColor = '#30363d';
    return true;
}

function refreshExamples() {
    var bbox = window._lastBbox;
    if (!bbox) return;
    var dir = document.getElementById('path-input').value;
    validatePath(dir);
    var includeBuild = document.getElementById('include-build-vrt').checked;

    var geojson = document.getElementById('geojson-output').textContent;
    var py = "from nbs.bluetopo import fetch_tiles" + (includeBuild ? ", build_vrt" : "") +
        "\nfetch_result = fetch_tiles('" + dir + "', geometry='" + geojson + "')";
    if (includeBuild) py += "\nbuild_result = build_vrt('" + dir + "')";
    document.getElementById('python-output').textContent = py;

    var cli = 'fetch_tiles -d "' + dir + '" -g \'' + geojson + "'";
    if (includeBuild) cli += '\nbuild_vrt -d "' + dir + '"';
    document.getElementById('cli-output').textContent = cli;
}

map.on(L.Draw.Event.CREATED, function(event) {
    drawnItems.clearLayers();
    var layer = event.layer;
    drawnItems.addLayer(layer);
    updateOutput(layer);
    document.getElementById('clear-btn').style.display = 'block';
});


function copyText(elementId) {
    var text = document.getElementById(elementId).textContent;
    navigator.clipboard.writeText(text);
}

function clearDrawing() {
    drawnItems.clearLayers();
    document.getElementById('output-section').style.display = 'none';
    document.getElementById('instructions').style.display = 'block';
    document.getElementById('clear-btn').style.display = 'none';
}
</script>
