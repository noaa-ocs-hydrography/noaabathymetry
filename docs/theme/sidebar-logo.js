// Inject clickable NOAA logo at top of sidebar navigation
(function () {
    var scrollbox = document.querySelector(".sidebar .sidebar-scrollbox");
    if (!scrollbox) return;

    var link = document.createElement("a");
    link.href = "index.html";
    link.className = "sidebar-logo";

    var img = document.createElement("img");
    img.src = "images/NOAA-1.png";
    img.alt = "NOAA";

    link.appendChild(img);
    scrollbox.insertBefore(link, scrollbox.firstChild);
})();
