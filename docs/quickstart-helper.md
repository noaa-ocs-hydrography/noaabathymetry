# Quickstart Helper

Draw your area of interest on the map below to generate usage examples.

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha384-sHL9NAb7lN7rfvG5lfHpm643Xkcjzp4jFvuavGOndn6pjVqS6ny56CAt3nsEVT4H" crossorigin="anonymous" />
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.css" integrity="sha384-NZLkVuBRMEeB4VeZz27WwTRvlhec30biQ8Xx7zG7JJnkvEKRg5qi6BNbEXo9ydwv" crossorigin="anonymous" />
<style>
#path-input::placeholder {
    color: #555;
    opacity: 1;
    font-size: 14px;
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
}
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
.copy-btn {
    font-size: 12px;
    cursor: pointer;
    margin-left: 8px;
    background: none;
    border: 1px solid #30363d;
    border-radius: 4px;
    color: #888;
    padding: 2px 8px;
    transition: color 0.15s, border-color 0.15s;
}
.copy-btn:hover {
    color: #e0e0e0;
    border-color: #888;
}
.kw { color: #cba6f7; }
.fn { color: #89b4fa; }
.str { color: #a6e3a1; }
.op { color: #89dceb; }
.cm { color: #6c7086; }
.mod { color: #f9e2af; }
.var { color: #cdd6f4; }
.param { color: #fab387; }
.punc { color: #6c7086; }
</style>

<div style="position: relative; margin-bottom: 16px;">
<div id="map" style="height: 500px; width: 100%; border: 1px solid #ccc; border-radius: 6px;"></div>
<button id="clear-btn" onclick="clearDrawing()">Clear</button>
</div>

<div id="output-section" style="display: none;">

<div style="display: flex; gap: 4px; margin-bottom: -1px; position: relative; z-index: 1;">
<button class="fmt-tab active-tab" onclick="switchTab('geojson')" id="tab-geojson" style="padding: 6px 14px; cursor: pointer; border: 1px solid #30363d; border-bottom: none; border-radius: 6px 6px 0 0; background: #1a1a2e; color: #e0e0e0; font-size: 13px;">GeoJSON</button>
<button class="fmt-tab" onclick="switchTab('bbox')" id="tab-bbox" style="padding: 6px 14px; cursor: pointer; border: 1px solid #30363d; border-bottom: none; border-radius: 6px 6px 0 0; background: #0d1117; color: #888; font-size: 13px;">Bounding Box</button>
<button class="fmt-tab" onclick="switchTab('wkt')" id="tab-wkt" style="padding: 6px 14px; cursor: pointer; border: 1px solid #30363d; border-bottom: none; border-radius: 6px 6px 0 0; background: #0d1117; color: #888; font-size: 13px;">WKT</button>
<button id="copy-fmt-btn" class="copy-btn" onclick="copyActiveFormat(this)" style="margin-left: auto; align-self: center;">Copy</button>
</div>
<div style="border: 1px solid #30363d; border-radius: 0 0 4px 4px; background: #1a1a2e;">
<pre id="geojson-output" class="fmt-output" style="display: block; background: transparent; color: #e0e0e0; padding: 12px; margin: 0; overflow-x: auto; user-select: all;"></pre>
<pre id="bbox-output" class="fmt-output" style="display: none; background: transparent; color: #e0e0e0; padding: 12px; margin: 0; overflow-x: auto; user-select: all;"></pre>
<pre id="wkt-output" class="fmt-output" style="display: none; background: transparent; color: #e0e0e0; padding: 12px; margin: 0; overflow-x: auto; user-select: all;"></pre>
</div>

<div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-top: 24px;">

<h3 style="margin-top: 0; border-bottom: 1px solid #30363d; padding-bottom: 10px;">Usage examples for your selected area</h3>

<div style="display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px;">
<div style="flex: 1; min-width: 200px;">
<label for="path-input" style="font-weight: bold; font-size: 13px; color: #aaa;">What's your project directory?</label>
<input id="path-input" type="text" value="~/my_bathymetry" placeholder="Enter your project directory" oninput="refreshExamples()" onblur="validatePath(this.value)" onfocus="clearValidation()" style="width: 100%; padding: 8px; margin-top: 4px; border: 1px solid #30363d; border-radius: 4px; background: #0d1117; color: #58a6ff; font-family: monospace; font-size: 14px; box-sizing: border-box; outline: none;" />
<span id="path-warning" style="display: none; color: #d29922; font-size: 12px; margin-top: 4px;"></span>
</div>
<div style="display: flex; align-items: flex-end; gap: 8px; padding-bottom: 2px;">
<label style="font-size: 13px; display: flex; align-items: center; gap: 4px; cursor: pointer;">
<input type="checkbox" id="include-mosaic-tiles" onchange="refreshExamples()" /> Include mosaic_tiles
</label>
</div>
</div>

<strong>Python</strong> <button class="copy-btn" onclick="copyText('python-output', this)">Copy</button>
<pre id="python-output" style="background: #0d1117; color: #e0e0e0; padding: 8px; border-radius: 4px; overflow-x: auto; user-select: text; border: 1px solid #30363d; font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-all;"></pre>

<strong>CLI</strong> <button class="copy-btn" onclick="copyText('cli-output', this)">Copy</button>
<pre id="cli-output" style="background: #0d1117; color: #e0e0e0; padding: 8px; border-radius: 4px; overflow-x: auto; user-select: text; border: 1px solid #30363d; font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-all;"></pre>

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
    validatePath(document.getElementById('path-input').value);
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
        warning.textContent = 'Path should be absolute (start with /, \\\\, ~, or a drive letter).';
        warning.style.display = 'block';
        input.style.borderColor = '#d29922';
        return false;
    }
    warning.style.display = 'none';
    input.style.borderColor = '#30363d';
    return true;
}

function clearValidation() {
    document.getElementById('path-warning').style.display = 'none';
    document.getElementById('path-input').style.borderColor = '#30363d';
}

function refreshExamples() {
    var bbox = window._lastBbox;
    if (!bbox) return;
    var dir = document.getElementById('path-input').value;
    var includeBuild = document.getElementById('include-mosaic-tiles').checked;

    var geojson = document.getElementById('geojson-output').textContent;

    // Python with syntax highlighting
    var ed = esc(dir);
    var eg = esc(geojson);
    var py = '<span class="kw">from</span> <span class="mod">nbs.noaabathymetry</span> <span class="kw">import</span> <span class="fn">fetch_tiles</span>' +
        (includeBuild ? '<span class="punc">,</span> <span class="fn">mosaic_tiles</span>' : '') +
        '\n<span class="var">fetch_result</span> <span class="op">=</span> <span class="fn">fetch_tiles</span><span class="punc">(</span><span class="str">\'' + ed + '\'</span><span class="punc">,</span> <span class="param">geometry</span><span class="op">=</span><span class="str">\'' + eg + '\'</span><span class="punc">)</span>';
    if (includeBuild) py += '\n<span class="var">mosaic_result</span> <span class="op">=</span> <span class="fn">mosaic_tiles</span><span class="punc">(</span><span class="str">\'' + ed + '\'</span><span class="punc">)</span>';
    document.getElementById('python-output').innerHTML = py;

    // CLI with highlighting
    var cli = '<span class="fn">fetch_tiles</span> <span class="param">-d</span> <span class="str">"' + ed + '"</span> <span class="param">-g</span> <span class="str">\'' + eg + '\'</span>';
    if (includeBuild) cli += '\n<span class="fn">mosaic_tiles</span> <span class="param">-d</span> <span class="str">"' + ed + '"</span>';
    document.getElementById('cli-output').innerHTML = cli;
}

map.on(L.Draw.Event.CREATED, function(event) {
    drawnItems.clearLayers();
    var layer = event.layer;
    drawnItems.addLayer(layer);
    updateOutput(layer);
    document.getElementById('clear-btn').style.display = 'block';
});


function esc(s) {
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

var activeTab = 'geojson';
var tabMap = { geojson: 'geojson-output', bbox: 'bbox-output', wkt: 'wkt-output' };

function switchTab(name) {
    activeTab = name;
    document.querySelectorAll('.fmt-output').forEach(function(el) { el.style.display = 'none'; });
    document.getElementById(tabMap[name]).style.display = 'block';
    document.querySelectorAll('.fmt-tab').forEach(function(el) {
        el.style.background = '#0d1117';
        el.style.color = '#888';
    });
    var tab = document.getElementById('tab-' + name);
    tab.style.background = '#1a1a2e';
    tab.style.color = '#e0e0e0';
}

function copyActiveFormat(btn) {
    var text = document.getElementById(tabMap[activeTab]).textContent;
    navigator.clipboard.writeText(text);
    flashButton(btn);
}

function copyText(elementId, btn) {
    var el = document.getElementById(elementId);
    var text = el.textContent || el.innerText;
    navigator.clipboard.writeText(text);
    flashButton(btn);
}

function flashButton(btn) {
    btn.textContent = 'Copied!';
    btn.style.color = '#3fb950';
    btn.style.borderColor = '#3fb950';
    setTimeout(function() {
        btn.textContent = 'Copy';
        btn.style.color = '';
        btn.style.borderColor = '';
    }, 1000);
}

function clearDrawing() {
    drawnItems.clearLayers();
    document.getElementById('output-section').style.display = 'none';
    document.getElementById('instructions').style.display = 'block';
    document.getElementById('clear-btn').style.display = 'none';
}
</script>
