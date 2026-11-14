// The map console's page: a Leaflet map and a panel, fed by the console's server-sent events.
//
// Names, order references and messages come from customer systems and the server, so each one
// goes into the page through createElement and textContent, never through an HTML parser. The
// page's own URLs are relative, because it lives under /console/.
"use strict";

// This script runs in <head>, so the theme is set before the first paint. The rest starts once
// the page is parsed, after Leaflet, which is deferred.
const THEME = "pourline-theme";
const systemDark = matchMedia("(prefers-color-scheme: dark)");
let chosen = storedTheme();

function storedTheme() {
  try {
    const stored = localStorage.getItem(THEME);
    return stored === "light" || stored === "dark" ? stored : null;
  } catch {
    return null;
  }
}

const theme = () => chosen ?? (systemDark.matches ? "dark" : "light");

document.documentElement.dataset.theme = theme();
document.addEventListener("DOMContentLoaded", start);

const STATUSES = {
  available: "Available",
  loading: "Loading",
  en_route: "En route",
  on_site: "On site",
  discharging: "Discharging",
  returning: "Returning",
  out_of_service: "Out of service",
};
/** A truck fades after this long without a report, on the world clock. Parked ones report every 60 s. */
const QUIET = 2 * 60 * 1000;
/** How long a truck takes to glide to its next fix. */
const GLIDE = 1500;
/** A stream without any event for this long is dead: the heartbeat comes every 15 s. */
const SILENCE = 45 * 1000;
const NOTICE = 8000;
const POPUP = { minWidth: 280, maxWidth: 280, closeButton: false, autoPanPadding: [16, 16] };
const TILES = "https://tiles.stadiamaps.com/tiles/{style}/{z}/{x}/{y}{r}.png";
// Leaflet renders this as HTML. It is a constant, never data.
const ATTRIBUTION =
  '&copy; <a href="https://stadiamaps.com/" target="_blank" rel="noopener">Stadia Maps</a> ' +
  '&copy; <a href="https://openmaptiles.org/" target="_blank" rel="noopener">OpenMapTiles</a> ' +
  '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a>';

let map;
let tiles;
let offset = 0; // the world clock minus this browser's clock, in ms
let clockKnown = false;
let features = { edge: false, chaos: false };
let links = {};
let edgePlant = "";
let plan = null;
let edge = null;
let uplink = { available: false, cut: false, run: null };
let log = [];
let named = new Set(); // trucks the latest diff names: they keep their label
let markedSpots = new Set(); // where plants and sites sit
let cutTotal = 0; // ms
let pop = null; // the open popup: { item, refresh }
let everLive = false;
let fitted = false;
const plants = new Map();
const sites = new Map();
const trucks = new Map();
const customers = new Map();
const busy = new Set();
const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)");

// Small helpers

const $ = (id) => document.getElementById(id);
const worldNow = () => Date.now() + offset;
const berlin = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/Berlin",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});
const hhmm = (time) => {
  const date = new Date(time);
  return Number.isNaN(date.getTime()) ? "--:--" : berlin.format(date);
};
const at = (time) => (time ? Date.parse(time) || 0 : 0);
const words = (s) => String(s).replaceAll("_", " ");
const plural = (n, one) => `${n} ${one}${n === 1 ? "" : "s"}`;
const num = (v) => String(Math.round(v * 10) / 10);
const secs = (s) => (s < 120 ? `${s} s` : s < 7200 ? `${Math.round(s / 60)} min` : `${Math.round(s / 3600)} h`);
const ago = (time) => `${secs(Math.max(0, Math.round((worldNow() - at(time)) / 1000)))} ago`;
const enc = encodeURIComponent;
// `10001001/BW-24117`: our customer number, then the customer's own order number.
const orderRef = (id) => String(id).slice(String(id).indexOf("/") + 1);
const plantName = (id) => plants.get(id)?.view.name ?? id;
const spot = (p) => `${p.lat.toFixed(5)},${p.lon.toFixed(5)}`;

function orderLabel(id, sequence) {
  const customer = customers.get(String(id).split("/")[0]);
  return `${orderRef(id)}${sequence ? ` #${sequence}` : ""}${customer ? ` · ${customer}` : ""}`;
}

/** An element with attributes (`text` sets its text, `on…` adds a listener) and children. */
function h(tag, props = {}, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value == null || value === false) continue;
    if (key === "text") el.textContent = value;
    else if (key.startsWith("on")) el.addEventListener(key.slice(2), value);
    else el.setAttribute(key, value);
  }
  el.append(...children.flat().filter((child) => child != null && child !== false));
  return el;
}

function svg(tag, attrs = {}, ...children) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, value);
  el.append(...children);
  return el;
}

// Clicks on a button marked aria-disabled do nothing. Unlike `disabled`, aria-disabled leaves the
// focus on the button while its request runs.
function press(el, action) {
  el.addEventListener("click", () => {
    if (el.getAttribute("aria-disabled") !== "true") action();
  });
  return el;
}

const button = (text, cls, action) => press(h("button", { type: "button", class: cls, text }), action);
const disable = (el, off) => el.setAttribute("aria-disabled", String(!!off));
const notice = (text) => h("p", { class: "notice pill", text });

// Only http(s) links from the snapshot: a javascript: URL from a broken setting would run here.
function safeUrl(url) {
  try {
    const parsed = new URL(url, location.href);
    return parsed.protocol === "http:" || parsed.protocol === "https:" ? parsed.href : null;
  } catch {
    return null;
  }
}

const traceUrl = (id) => `${links.jaeger}/trace/${enc(id)}`;

function traceLink(a, id, what) {
  a.hidden = !id || !links.jaeger;
  if (a.hidden) return;
  a.href = traceUrl(id);
  a.setAttribute("aria-label", `Trace of ${what}`);
}

function traceAnchor(id, what) {
  if (!id || !links.jaeger) return null;
  const a = h("a", { target: "_blank", rel: "noopener" }, "Trace ", h("span", { "aria-hidden": "true", text: "↗" }));
  traceLink(a, id, what);
  return a;
}

// The truck icon: a ready-mix truck seen from the side, cab to the right. Drum, chassis, cab and
// wheels take the status colour; a halo in the map's colour lets it read on both basemaps; out of
// service adds the × badge, so colour is never the only signal.
const BODY =
  "M2.35 5.57A8.4 4.5 14 0 1 18.65 9.63A8.4 4.5 14 0 1 2.35 5.57Z" + // drum
  "M3 11.6H26.5V13.6H3Z" + // chassis
  "M19.5 13V6.2Q19.5 5 20.7 5H23.6L26.6 9V13Z"; // cab

function truckIcon() {
  const shape = (cls) =>
    svg("g", { class: cls }, svg("path", { d: BODY }), ...[7, 11.6, 22.8].map((cx) => svg("circle", { cx, cy: 15.3, r: 2.3 })));
  return svg(
    "svg",
    { class: "truck-icon", viewBox: "0 0 29 20", width: 29, height: 20, "aria-hidden": "true", focusable: "false" },
    shape("t-halo"),
    shape("t-body"),
    svg("path", { class: "t-window", d: "M21 6.5H23.2L25.3 9.2H21Z" }),
    svg("path", { class: "t-fins", d: "M7.4 3.1Q9.3 7.4 7.9 11.5M12.7 3.8Q14.6 8 13.2 12.2" }),
    svg("g", { class: "t-badge" }, svg("circle", { cx: 24.2, cy: 3.4, r: 4.8 }), svg("path", { d: "M22.3 1.5L26.1 5.3M26.1 1.5L22.3 5.3" })),
  );
}

const truckClass = (status) => `truck s-${status}${status === "out_of_service" ? " oos" : ""}`;

// Start

function start() {
  applyTheme();
  press($("theme"), () => {
    chosen = theme() === "dark" ? "light" : "dark";
    // Back on the system's choice, the page follows the system again.
    if (chosen === (systemDark.matches ? "dark" : "light")) chosen = null;
    try {
      if (chosen) localStorage.setItem(THEME, chosen);
      else localStorage.removeItem(THEME);
    } catch {
      // Remembered until the page reloads.
    }
    applyTheme();
  });
  systemDark.addEventListener("change", applyTheme);
  press($("replan"), () => act("replan", "Re-plan", "POST", "api/plans"));
  press($("sync"), syncNow);
  press($("chaos"), () => act("chaos", "Make chaos", "POST", "api/chaos", null, "started"));
  press($("reconnect"), () => act("reconnect", "Reconnect", "DELETE", "api/chaos"));
  setUpTabs();
  $("legend-trucks").replaceChildren(
    ...Object.entries(STATUSES).map(([status, word]) => h("li", {}, h("span", { class: truckClass(status) }, truckIcon()), word)),
  );
  // Without Leaflet the panel and its actions still work.
  if (window.L) startMap();
  else $("map-notices").replaceChildren(notice("The map didn't load: this browser can't reach unpkg.com, where Leaflet comes from."));
  renderPlan();
  renderLog();
  renderEdge();
  // Idle tabs let go of the stream, so they don't hold the browser's few connections to the host.
  document.addEventListener("visibilitychange", () => (document.hidden ? disconnect() : connect()));
  if (!document.hidden) connect();
  setInterval(tick, 1000);
}

function applyTheme() {
  const current = theme();
  document.documentElement.dataset.theme = current;
  $("theme").setAttribute("aria-label", current === "dark" ? "Switch to the light theme" : "Switch to the dark theme");
  tiles?.setUrl(tileUrl());
}

const tileUrl = () => TILES.replace("{style}", theme() === "dark" ? "alidade_smooth_dark" : "alidade_smooth");

function setUpTabs() {
  const tabs = [...document.querySelectorAll('[role="tab"]')];
  const select = (tab) => {
    for (const each of tabs) {
      const selected = each === tab;
      each.setAttribute("aria-selected", String(selected));
      each.tabIndex = selected ? 0 : -1;
      $(each.getAttribute("aria-controls")).hidden = !selected;
    }
  };
  tabs.forEach((tab, i) => {
    tab.addEventListener("click", () => select(tab));
    tab.addEventListener("keydown", (e) => {
      const to = { ArrowRight: i + 1, ArrowLeft: i - 1, Home: 0, End: tabs.length - 1 }[e.key];
      if (to === undefined) return;
      e.preventDefault();
      const next = tabs[(to + tabs.length) % tabs.length];
      select(next);
      next.focus();
    });
  });
}

function tick() {
  renderClock();
  trucks.forEach(paintTruck);
  renderEdge(); // ages and the cut's countdown; it refreshes an open popup as well
}

// The map

function startMap() {
  const still = reduceMotion.matches;
  map = L.map("map", {
    zoomControl: false,
    attributionControl: false,
    minZoom: 7,
    zoomAnimation: !still,
    fadeAnimation: !still,
    markerZoomAnimation: !still,
  }).setView([49.4, 8.65], 10);
  L.control.attribution({ prefix: false }).addTo(map);
  zoomControl().addTo(map);
  tiles = L.tileLayer(tileUrl(), { maxZoom: 20, attribution: ATTRIBUTION }).addTo(map);
  let failed = 0;
  tiles.on("loading", () => {
    failed = 0;
  });
  tiles.on("tileerror", () => {
    failed += 1;
    basemapNotice(true);
  });
  tiles.on("load", () => {
    if (!failed) basemapNotice(false);
  });
  // Only the opening pans the map: a gliding truck would drag it along on every frame.
  map.on("popupopen", (e) => {
    e.popup.options.autoPan = false;
    e.popup.getElement()?.querySelector(".pop-title")?.focus({ preventScroll: true });
  });
  map.on("popupclose", (e) => {
    e.popup.options.autoPan = true;
    pop = null;
  });
}

/** How far the legend's top sits above the map's bottom edge. Leaflet doesn't know it's there. */
function legendClear() {
  const legend = $("legend");
  return legend.offsetParent.clientHeight - legend.offsetTop;
}

function zoomControl() {
  const control = L.control({ position: "topleft" });
  control.onAdd = () => {
    const zoomIn = h("button", { type: "button", "aria-label": "Zoom in", text: "+" });
    const zoomOut = h("button", { type: "button", "aria-label": "Zoom out", text: "−" });
    press(zoomIn, () => map.zoomIn());
    press(zoomOut, () => map.zoomOut());
    const limits = () => {
      disable(zoomIn, map.getZoom() >= map.getMaxZoom());
      disable(zoomOut, map.getZoom() <= map.getMinZoom());
    };
    map.on("zoomend", limits);
    limits();
    const box = h("div", { class: "zoom pill" }, zoomIn, h("span", { class: "zoom-rule", "aria-hidden": "true" }), zoomOut);
    L.DomEvent.disableClickPropagation(box);
    return box;
  };
  return control;
}

function basemapNotice(show) {
  const box = $("map-notices");
  if (show === box.hasChildNodes()) return;
  box.replaceChildren(...(show ? [notice("Basemap unavailable: Stadia Maps sends no tiles. Trucks still update.")] : []));
}

/** A marker whose icon is `html`, with a popup built by `content` each time it opens. */
function marker(latlng, html, anchor, zIndexOffset, content) {
  const m = L.marker(latlng, {
    icon: L.divIcon({ html, className: "pl-marker", iconSize: null, iconAnchor: anchor, popupAnchor: [0, -anchor[1]] }),
    zIndexOffset,
    riseOnHover: true,
  });
  m.bindPopup(content, POPUP).addTo(map);
  // Leaflet makes the icon a focusable role=button that opens on Enter; a button opens on Space too.
  m.getElement().addEventListener("keydown", (e) => {
    if (e.key !== " ") return;
    e.preventDefault();
    m.fire("click", { originalEvent: e, latlng: m.getLatLng() });
  });
  // A marker focused under the legend moves up, clear of it.
  m.getElement().addEventListener("focus", () => {
    const a = m.getElement().firstElementChild.getBoundingClientRect();
    const b = $("legend").getBoundingClientRect();
    if (a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top) map.panBy([0, a.bottom - b.top + 16]);
  });
  return m;
}

function label(m, text) {
  const el = m.getElement();
  if (el && el.getAttribute("aria-label") !== text) el.setAttribute("aria-label", text);
}

/** A popup's content. Escape closes it and gives the focus back to its marker. */
function popupRoot(m, ...children) {
  // Runs before Leaflet's opening pan, which then keeps the popup above the legend.
  m.getPopup().options.autoPanPaddingBottomRight = [16, legendClear() + 8];
  const onkeydown = (e) => {
    if (e.key !== "Escape") return;
    m.closePopup();
    m.getElement()?.focus();
  };
  return h("div", { class: "pop", onkeydown }, ...children);
}

async function read(path) {
  const response = await fetch(path);
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.error ?? `the console answered ${response.status}`);
  return body;
}

// Plants

function addPlant(view) {
  const name = h("span");
  const cut = h("span", { class: "plant-cut", text: "· uplink cut" });
  const el = h("div", { class: "plant" }, h("span", { class: "plant-mark" }), h("span", { class: "plant-label pill" }, name, cut));
  const p = { view, el, name, cut };
  p.marker = marker([view.location.lat, view.location.lon], el, [8, 8], 500, () => plantPopup(p));
  return p;
}

function paintPlant(p) {
  const v = p.view;
  const isCut = v.id === edgePlant && uplink.cut;
  p.el.classList.toggle("offline", v.status === "offline");
  p.name.textContent = v.name;
  p.cut.hidden = !isCut;
  label(p.marker, `Plant ${v.name}, ${v.status}${isCut ? ", uplink cut" : ""}`);
  if (pop?.item === p) pop.refresh();
}

function plantPopup(p) {
  const id = p.view.id;
  const mark = h("span", { class: "plant-mark", "aria-hidden": "true" });
  const chip = h("span", { class: "chip" });
  const summary = h("p", { class: "pop-sub" });
  const loads = h("div", {}, h("p", { class: "pop-sub", text: "Loading the next loads…" }));
  const toggle = button("", "btn secondary", () => {
    const off = p.view.status === "online";
    act(`status:${id}`, off ? "Take offline" : "Bring online", "PUT", `api/plants/${enc(id)}/status`, {
      status: off ? "offline" : "online",
    });
  });
  const sync = id === edgePlant && features.edge ? button("Sync now", "btn primary", syncNow) : null;
  const el = popupRoot(
    p.marker,
    h("div", { class: "pop-head" }, mark, h("h3", { class: "pop-title", tabindex: "-1", text: p.view.name }), chip),
    summary,
    loads,
    h("div", { class: "actions" }, toggle, sync),
  );
  const refresh = () => {
    const v = p.view;
    const ticket = v.last_ticket;
    mark.classList.toggle("offline", v.status === "offline");
    chip.textContent = v.status;
    summary.textContent =
      `${plural(v.bays, "bay")} · ${num(v.rate_m3_per_h)} m³/h · last ticket ` +
      (ticket ? `${words(ticket.event)}, ${ticket.truck}, ${hhmm(ticket.at)}` : "none yet");
    toggle.textContent = v.status === "online" ? "Take offline" : "Bring online";
    disable(toggle, busy.has(`status:${id}`));
    if (sync) disable(sync, busy.has("sync") || !canSync());
  };
  pop = { item: p, refresh };
  refresh();
  read(`api/plants/${enc(id)}/plan`).then(
    (slice) => {
      // Frozen loads are batching already; the rest wait for the plant.
      const next = slice.loads.filter((load) => !load.frozen).slice(0, 5);
      loads.replaceChildren(
        slice.version == null
          ? h("p", { class: "pop-sub", text: "No plan yet." })
          : next.length
            ? loadTable(next, slice.version)
            : h("p", { class: "pop-sub", text: `Nothing left to batch here in plan v${slice.version}.` }),
      );
    },
    (error) => loads.replaceChildren(h("p", { class: "pop-sub", text: error.message })),
  );
  return el;
}

function loadTable(next, version) {
  const cell = (tag, text, cls) => h(tag, { class: cls, text, scope: tag === "th" ? "col" : null });
  return h(
    "table",
    { class: "loads" },
    h("caption", { text: `Next loads from plan v${version}` }),
    h("thead", {}, h("tr", {}, cell("th", "Batch"), cell("th", "Order"), cell("th", "Truck"), cell("th", "m³", "num"))),
    h(
      "tbody",
      {},
      next.map((load) =>
        h(
          "tr",
          {},
          cell("td", hhmm(load.planned_batch_at)),
          cell("td", `${orderRef(load.order)} #${load.sequence}`),
          cell("td", load.planned_truck),
          cell("td", num(load.volume_m3), "num"),
        ),
      ),
    ),
  );
}

// Sites

function addSite(view) {
  const s = { view };
  s.marker = marker([view.location.lat, view.location.lon], h("span", { class: "site-mark" }), [5, 5], 0, () => sitePopup(s));
  return s;
}

const paintSite = (s) => label(s.marker, `Site ${s.view.name}, ${s.view.customer_name}`);

function sitePopup(s) {
  const v = s.view;
  const key = `delay:${v.id}`;
  const orders = h("ul", { class: "orders" }, h("li", { class: "muted", text: "Loading today's orders…" }));
  const delay = button("Delay 30 min", "btn secondary", () =>
    act(key, "Delay 30 min", "POST", `api/sites/${enc(v.id)}/delay`, null, "nothing changed"),
  );
  const el = popupRoot(
    s.marker,
    h("div", {}, h("h3", { class: "pop-title", tabindex: "-1", text: v.name }), h("p", { class: "pop-sub", text: `${v.customer_name} · ship-to ${v.id}` })),
    orders,
    h("div", {}, delay),
  );
  pop = { item: s, refresh: () => disable(delay, busy.has(key)) };
  pop.refresh();
  read(`api/sites/${enc(v.id)}/orders`).then(
    (list) =>
      orders.replaceChildren(
        ...(list.length
          ? list.map((o) =>
              h(
                "li",
                {},
                h("span", { class: "ref", text: orderRef(o.id) }),
                h("span", { class: "detail", text: `${num(o.total_volume_m3)} m³ · from ${hhmm(o.requested_start)} · ${words(o.status)}` }),
              ),
            )
          : [h("li", { class: "muted", text: "No orders today." })]),
      ),
    (error) => orders.replaceChildren(h("li", { class: "muted", text: error.message })),
  );
  return el;
}

// Trucks

/** Where a truck is drawn: its last fix, or its home plant before it has one. */
const placeOf = (v) => v.position ?? plants.get(v.home)?.view.location ?? { lat: 49.4, lon: 8.65 };

function addTruck(view) {
  const el = h("div", { class: truckClass(view.status) }, truckIcon(), h("span", { class: "truck-id", text: view.id }));
  const t = { view, el, west: false };
  const place = placeOf(view);
  t.marker = marker([place.lat, place.lon], el, [14.5, 10], 1000, () => truckPopup(t));
  return t;
}

const quiet = (v) => {
  const last = Math.max(at(v.status_at), at(v.position_at));
  return !last || worldNow() - last > QUIET;
};

function paintTruck(t) {
  const v = t.view;
  const out = v.status === "out_of_service";
  const faded = quiet(v);
  // In a crowded parking grid a label would cover the neighbours; there only out of service keeps it.
  const cls =
    truckClass(v.status) +
    (t.west ? " west" : "") +
    (out || (named.has(v.id) && !t.crowded) ? " labelled" : "") +
    (faded ? " stale" : "");
  if (t.el.className !== cls) t.el.className = cls;
  label(t.marker, `${v.id}, ${words(v.status)}${faded ? ", no recent report" : ""}`);
}

function setTruck(t, view) {
  const from = placeOf(t.view);
  const to = placeOf(view);
  t.view = view;
  if (from.lat !== to.lat || from.lon !== to.lon) {
    if (from.lon !== to.lon) t.west = to.lon < from.lon;
    glide(t, to);
    park();
  }
  paintTruck(t);
  if (pop?.item === t) pop.refresh();
}

// Glides with requestAnimationFrame: a CSS transition on the marker would fight Leaflet's zoom.
const glides = new Map();
let gliding = false;

function glide(t, to) {
  const target = L.latLng(to.lat, to.lon);
  if (reduceMotion.matches || document.hidden) {
    glides.delete(t);
    t.marker.setLatLng(target);
    return;
  }
  glides.set(t, { from: t.marker.getLatLng(), to: target, start: performance.now() });
  if (!gliding) {
    gliding = true;
    requestAnimationFrame(frame);
  }
}

function frame(now) {
  for (const [t, g] of glides) {
    const f = Math.min(1, Math.max(0, (now - g.start) / GLIDE));
    const eased = f * (2 - f);
    t.marker.setLatLng([g.from.lat + (g.to.lat - g.from.lat) * eased, g.from.lng + (g.to.lng - g.from.lng) * eased]);
    if (f === 1) glides.delete(t);
  }
  gliding = glides.size > 0;
  if (gliding) requestAnimationFrame(frame);
}

// Trucks that share a point (parked at a plant, pouring at a site, not reported yet) park in a grid
// left of it, clear of a plant's label, instead of hiding each other and the marker underneath.
function park() {
  const groups = new Map();
  for (const t of trucks.values()) {
    const key = spot(placeOf(t.view));
    groups.set(key, [...(groups.get(key) ?? []), t]);
  }
  for (const [key, group] of groups) {
    const alone = group.length === 1 && !markedSpots.has(key);
    const cols = Math.ceil(Math.sqrt(group.length));
    const rows = Math.ceil(group.length / cols);
    group.sort((a, b) => (a.view.id < b.view.id ? -1 : 1));
    group.forEach((t, i) => {
      t.crowded = group.length > 1;
      const dx = alone ? 0 : -(30 + 31 * (i % cols));
      const dy = alone ? 0 : (Math.floor(i / cols) - (rows - 1) / 2) * 22;
      t.el.style.transform = dx || dy ? `translate(${dx}px, ${dy}px)` : "";
      t.marker.getPopup().options.offset = [dx, 7 + dy];
    });
  }
}

function truckPopup(t) {
  const icon = h("span", {}, truckIcon());
  const chip = h("span", { class: "chip" });
  const report = h("dd");
  const breakDown = button("Break down", "btn secondary", () =>
    act(`breakdown:${t.view.id}`, "Break down", "POST", `api/trucks/${enc(t.view.id)}/breakdown`, null, "sent"),
  );
  const breakRow = h("div", {}, breakDown);
  const note = h("p", { class: "pop-sub", tabindex: "-1", text: "Break down comes back once the truck is repaired." });
  const el = popupRoot(
    t.marker,
    h("div", { class: "pop-head" }, icon, h("h3", { class: "pop-title", tabindex: "-1", text: t.view.id }), chip),
    h(
      "dl",
      { class: "facts" },
      h("dt", { text: "Capacity" }),
      h("dd", { text: `${num(t.view.capacity_m3)} m³` }),
      h("dt", { text: "Home plant" }),
      h("dd", { text: plantName(t.view.home) }),
      h("dt", { text: "Last report" }),
      report,
    ),
    breakRow,
    note,
  );
  const refresh = () => {
    const v = t.view;
    const out = v.status === "out_of_service";
    icon.className = truckClass(v.status);
    chip.textContent = words(v.status);
    const gps = at(v.position_at);
    const status = at(v.status_at);
    report.textContent = !gps && !status ? "none yet" : gps >= status ? `GPS, ${ago(v.position_at)}` : `status, ${ago(v.status_at)}`;
    note.hidden = !out;
    // Break down leaves while it may have the focus: the note takes it, inside the popup.
    if (out && breakRow.contains(document.activeElement)) note.focus({ preventScroll: true });
    breakRow.hidden = out;
    disable(breakDown, busy.has(`breakdown:${v.id}`));
  };
  pop = { item: t, refresh };
  refresh();
  return el;
}

// Actions

const canSync = () => !!edge?.reachable && edge.status?.mqtt === true && !uplink.cut;
const syncNow = () => act("sync", "Sync now", "POST", `api/plants/${enc(edgePlant)}/sync`, null, "sent");

/** Sends a change and says how it went. Its buttons stay disabled while it runs. */
async function act(key, title, method, path, body = null, done = "done") {
  if (busy.has(key)) return;
  busy.add(key);
  renderControls();
  try {
    const headers = { "X-Pourline-Console": "1" };
    if (body) headers["Content-Type"] = "application/json";
    const response = await fetch(path, { method, headers, body: body && JSON.stringify(body) });
    const answer = await response.json().catch(() => ({}));
    if (response.ok) {
      notify(`${title}: ${answer.version != null ? `plan v${answer.version}` : done}`);
    } else {
      const verb = response.status >= 500 ? "failed" : "refused";
      notify(`${title} ${verb}: ${answer.error ?? `the console answered ${response.status}`}`);
    }
  } catch {
    notify(`${title} failed: the console doesn't answer.`);
  } finally {
    busy.delete(key);
    renderControls();
  }
}

let noticeTimer = 0;

function notify(text) {
  clearTimeout(noticeTimer);
  $("notices").replaceChildren(notice(text));
  noticeTimer = setTimeout(() => $("notices").replaceChildren(), NOTICE);
}

const running = (run) => !!run && run.step !== "done" && run.step !== "failed";

function renderControls() {
  disable($("replan"), busy.has("replan"));
  const run = uplink.run;
  const active = running(run);
  const reconnect = features.chaos && ((active && run.step === "cut") || (uplink.cut && !active));
  disable($("sync"), busy.has("sync") || !canSync());
  // Make chaos and Reconnect take turns in one place, and the focus goes with them.
  const pair = [$("chaos"), $("reconnect")];
  const had = pair.indexOf(document.activeElement);
  pair[0].hidden = !features.chaos || reconnect;
  disable(pair[0], busy.has("chaos") || active || uplink.cut || !edge?.reachable);
  pair[1].hidden = !reconnect;
  disable(pair[1], busy.has("reconnect"));
  if (had >= 0 && pair[had].hidden) pair[1 - had].focus();
  pop?.refresh();
}

// The panel

function setClock(now) {
  offset = Date.parse(now) - Date.now();
  clockKnown = !Number.isNaN(offset);
  if (!clockKnown) offset = 0;
  renderClock();
}

function renderClock() {
  if (!clockKnown) return;
  $("world-time").textContent = hhmm(worldNow());
  const hours = Math.round(offset / 3_600_000);
  $("world-shift").textContent = hours ? `world time, ${hours < 0 ? "−" : "+"}${Math.abs(hours)} h` : "world time";
}

const namedIn = (diff) =>
  new Set((diff?.changes ?? []).flatMap((c) => [c.before?.planned_truck, c.after?.planned_truck]).filter(Boolean));

function renderPlan() {
  $("plan-time").textContent = plan ? `${hhmm(plan.created_at)} world time` : "Waiting for the first plan";
  $("plan-version").textContent = plan ? `Plan v${plan.version}` : "No plan yet";
  traceLink($("plan-trace"), plan?.trace_id, plan ? `plan v${plan.version}` : "");
  named = namedIn(plan?.diff);
  trucks.forEach(paintTruck);
  renderChanges();
}

function renderChanges() {
  const panel = $("changes");
  const diff = plan?.diff;
  if (!plan) return panel.replaceChildren(h("p", { class: "muted", text: "No plan yet." }));
  if (!diff) {
    return panel.replaceChildren(h("p", { class: "muted", text: `Plan v${plan.version} is the first: there is nothing to compare it with.` }));
  }
  const unplanned = diff.newly_unplanned ?? [];
  panel.replaceChildren(
    ...(diff.triggers ?? []).map((trigger) => h("p", { class: "trigger" }, h("span", { class: "dot", "aria-hidden": "true" }), trigger)),
    diff.changes?.length ? h("ol", { class: "rows" }, diff.changes.map(changeItem)) : null,
    unplanned.length
      ? h(
          "div",
          {},
          h("h3", { class: "sub", text: "Unplanned now" }),
          h(
            "ul",
            { class: "rows" },
            unplanned.map((u) => h("li", {}, h("span", { class: "ref", text: orderLabel(u.order) }), h("span", { class: "reason", text: u.reason }))),
          ),
        )
      : h("p", { class: "muted small", text: "No order became unplanned." }),
  );
}

function changeItem(change) {
  const { before: b, after: a } = change;
  const eta = b && a && b.eta_on_site !== a.eta_on_site ? `${hhmm(b.eta_on_site)} → ${hhmm(a.eta_on_site)}` : hhmm((a ?? b).eta_on_site);
  return h(
    "li",
    {},
    h(
      "div",
      { class: "change-head" },
      h("span", { class: "ref", text: orderLabel(change.order, change.sequence) }),
      h("span", { class: "when" }, h("span", { class: "sr-only", text: "On site " }), eta),
    ),
    h("span", { text: changeWhat(b, a) }),
    h("span", { class: "reason", text: change.reason }),
  );
}

function changeWhat(b, a) {
  if (!b) return `New: ${a.planned_truck}, from ${plantName(a.planned_plant)}`;
  if (!a) return `Dropped: ${b.planned_truck}, from ${plantName(b.planned_plant)}`;
  const either = (x, y, show = String) => (x === y ? show(y) : `${show(x)} → ${show(y)}`);
  return `${either(b.planned_truck, a.planned_truck)}, from ${either(b.planned_plant, a.planned_plant, plantName)}`;
}

// The log names a site or plant by its id; the page shows its name.
function shownTarget(target) {
  const [id, ...rest] = String(target).split(", ");
  const name = plants.get(id)?.view.name ?? sites.get(id)?.view.name ?? id;
  return [name, ...rest].join(", ");
}

function logItem(entry) {
  const target = shownTarget(entry.target);
  return h(
    "li",
    {},
    h("time", { datetime: entry.at, text: hhmm(entry.at) }),
    h(
      "span",
      { class: "log-what" },
      h("span", { class: "log-action", text: entry.action }),
      target ? ` ${target}` : null,
      entry.outcome === "done" ? null : h("span", { class: "log-outcome", text: entry.outcome }),
    ),
    traceAnchor(entry.trace_id, `${entry.action} ${target}`.trim()),
  );
}

function renderLog() {
  $("log-list").replaceChildren(...(log.length ? log.map(logItem) : [h("li", { class: "muted", text: "No actions yet." })]));
}

const LINE = { ordering: 0, planning: 1, syncing: 1, cut: 2, draining: 3, done: 4 };
const MARKS = { done: ["✓", "Done: "], active: ["●", "Now: "], failed: ["×", "Failed: "], todo: ["○", "Next: "] };
const cutLeft = () => (uplink.run?.cut_until ? Math.max(0, at(uplink.run.cut_until) - worldNow()) : 0);

function renderEdge() {
  $("edge").hidden = !features.edge;
  if (features.edge) {
    const plant = plants.get(edgePlant)?.view;
    const name = plant?.name ?? edgePlant;
    const status = edge?.status;
    const up = !!edge?.reachable;
    $("edge-title").textContent = `${name} edge`;
    const [chip, tone] = !edge
      ? ["Waiting", ""]
      : !up
        ? ["Unreachable", ""]
        : uplink.cut
          ? ["Uplink cut", "bad"]
          : status?.mqtt && status?.dispatch
            ? ["Links up", "good"]
            : [status?.mqtt ? "Dispatch link down" : status?.dispatch ? "MQTT link down" : "Links down", "warn"];
    $("edge-chip").className = `chip ${tone}`;
    $("edge-chip-text").textContent = chip;
    const message = !edge
      ? `Waiting for ${name}'s first status.`
      : up
        ? ""
        : status && edge.at
          ? `${name} doesn't answer. These are its last values, from ${ago(edge.at)}.`
          : `${name} doesn't answer.`;
    $("edge-message").textContent = message;
    $("edge-message").hidden = !message;
    const ticket = plant?.last_ticket;
    const stats = status
      ? [
          ["Outbox", plural(status.outbox, "ticket")],
          ["Sync lag", secs(status.sync_lag_seconds)],
          ["Plan", status.plan_version != null ? `v${status.plan_version} · ${secs(status.plan_age_seconds ?? 0)} old` : "none yet"],
          ["Last ticket", ticket ? `${words(ticket.event)} · ${ticket.truck}` : "none yet"],
        ]
      : [];
    $("edge-stats").classList.toggle("stale", !up);
    $("edge-stats").replaceChildren(...stats.map(([term, value]) => h("div", {}, h("dt", { text: term }), h("dd", { text: value }))));
    renderRun(name);
  }
  renderControls();
}

function renderRun(name) {
  const run = features.chaos ? uplink.run : null;
  $("edge-run").hidden = !run;
  if (!run) return;
  const failed = run.step === "failed";
  const over = failed || run.step === "done";
  // A failed run tells how far it got only through what it had filled in.
  const reached = LINE[run.step] ?? (run.held != null ? 3 : run.cut_until ? 2 : run.orders.length ? 1 : 0);
  const left = cutLeft();
  const lines = [
    [`Placing orders near ${name}`, `${plural(run.orders.length, "order")} placed near ${name}`, `Place orders near ${name}`],
    [
      run.plan_version == null ? "Waiting for a plan with the orders" : `Waiting for ${name} to hold plan v${run.plan_version}`,
      `${name} ${over ? "held" : "holds"} plan v${run.plan_version}`,
      `${name} holds their plan`,
    ],
    [`Uplink cut · ${Math.ceil(left / 1000)} s left`, "Uplink cut, then reconnected", "Cut the uplink"],
    [run.held == null ? "Draining the outbox" : `Draining the outbox · ${plural(run.held, "ticket")} held`, "Outbox drained", "Drain the outbox and report"],
  ];
  $("edge-steps").replaceChildren(
    ...lines.map(([now, done, next], i) => {
      const state = i < reached ? "done" : i > reached ? "todo" : failed ? "failed" : "active";
      const [mark, said] = MARKS[state];
      return h(
        "li",
        { class: `step ${state}` },
        h("span", { class: "mark", "aria-hidden": "true", text: mark }),
        h("span", {}, h("span", { class: "sr-only", text: said }), state === "done" ? done : state === "active" ? now : next),
      );
    }),
  );
  $("edge-bar").hidden = run.step !== "cut";
  // ponytail: the run doesn't carry the cut's length, so the bar measures from when this page
  // first saw the cut: exact when it saw it start, shorter when it joined midway.
  $("edge-bar-fill").style.width = `${cutTotal ? Math.min(100, 100 * (1 - left / cutTotal)) : 0}%`;
  $("edge-report").textContent = run.report ?? "";
  $("edge-report").hidden = !run.report;
  traceLink($("edge-trace"), run.trace_id, "Make chaos");
}

// The live stream

function applySnapshot(s) {
  setClock(s.now);
  features = s.features;
  links = { grafana: safeUrl(s.links.grafana), jaeger: safeUrl(s.links.jaeger), api: safeUrl(s.links.api) };
  edgePlant = s.edge_plant;
  customers.clear();
  for (const site of s.sites) customers.set(site.customer, site.customer_name);
  plan = s.plan;
  edge = s.edge;
  log = s.log;
  setUplink(s.uplink);
  named = namedIn(plan?.diff);
  markedSpots = new Set([...s.plants, ...s.sites].map((m) => spot(m.location)));
  // Plants first, for the trucks that start at home; trucks before sites, for the tab order.
  if (map) {
    replace(plants, s.plants, addPlant, (p, view) => {
      p.view = view;
    });
    replace(trucks, s.trucks, addTruck, setTruck);
    replace(sites, s.sites, addSite, (site, view) => {
      site.view = view;
    });
  }
  park();
  plants.forEach(paintPlant);
  sites.forEach(paintSite);
  trucks.forEach(paintTruck);
  for (const name of ["grafana", "jaeger", "api"]) {
    $(name).hidden = !links[name];
    if (links[name]) $(name).href = links[name];
  }
  renderPlan();
  renderLog();
  renderEdge();
  if (map && !fitted && markedSpots.size) {
    fitted = true;
    const bounds = L.latLngBounds([...s.plants, ...s.sites].map((m) => [m.location.lat, m.location.lon]));
    // Clear of the legend: beside it or above it, whichever keeps the closer zoom.
    const beside = { paddingTopLeft: [$("legend").offsetWidth + 32, 32], paddingBottomRight: [32, 32] };
    const above = { paddingTopLeft: [32, 32], paddingBottomRight: [32, legendClear() + 16] };
    const zoom = (o) => map.getBoundsZoom(bounds, false, L.point(o.paddingTopLeft).add(o.paddingBottomRight));
    map.fitBounds(bounds, zoom(beside) >= zoom(above) ? beside : above);
  }
}

/** Updates what the snapshot still has, adds what's new and removes what's gone. */
function replace(items, views, add, update) {
  const ids = new Set(views.map((view) => view.id));
  for (const [id, item] of items) {
    if (ids.has(id)) continue;
    item.marker.remove();
    glides.delete(item);
    items.delete(id);
  }
  for (const view of views) {
    const item = items.get(view.id);
    if (item) update(item, view);
    else items.set(view.id, add(view));
  }
}

function setUplink(next) {
  const wasCut = uplink.run?.step === "cut";
  uplink = next;
  if (next.run?.step !== "cut") cutTotal = 0;
  else if (!wasCut || !cutTotal) cutTotal = cutLeft();
}

const newer = (time, held) => !!time && (!held || at(time) > at(held));

/** A truck event: each field keeps its newest report. */
function truckEvent(view) {
  const t = trucks.get(view.id);
  if (!t) return;
  const next = { ...t.view };
  if (newer(view.status_at, next.status_at)) Object.assign(next, { status: view.status, status_at: view.status_at });
  if (newer(view.position_at, next.position_at)) Object.assign(next, { position: view.position, position_at: view.position_at });
  setTruck(t, next);
}

function plantEvent(view) {
  const p = plants.get(view.id);
  if (!p) return;
  p.view = view;
  paintPlant(p);
  if (view.id === edgePlant) renderEdge();
}

/** A plan version, unless it isn't newer than the page's; or the same version with its trace. */
function planEvent(next) {
  const taken = !plan || next.version > plan.version || (next.version === plan.version && !plan.trace_id && next.trace_id);
  if (!taken) return;
  plan = next;
  renderPlan();
}

function logEvent(entry) {
  const list = $("log-list");
  if (!log.length) list.replaceChildren();
  log = [entry, ...log].slice(0, 50);
  // Added at the top rather than rebuilt, so a focused trace link stays where it is.
  list.prepend(logItem(entry));
  while (list.children.length > log.length) list.lastElementChild.remove();
}

function uplinkEvent(next) {
  const was = uplink.run;
  setUplink(next);
  // A run's end is announced; the card alone isn't a live region.
  if (running(was) && next.run?.report) notify(`Make chaos${next.run.step === "failed" ? " failed" : ""}: ${next.run.report}`);
  renderEdge();
  const p = plants.get(edgePlant);
  if (p) paintPlant(p);
}

let stream = null;
let watchdog = 0;
let retry = 0;

function connect() {
  disconnect();
  setLive(false);
  const source = new EventSource("events");
  stream = source;
  const handlers = {
    snapshot: (s) => {
      applySnapshot(s);
      setLive(true);
    },
    truck: truckEvent,
    plant: plantEvent,
    plan: planEvent,
    edge: (next) => {
      edge = next;
      renderEdge();
    },
    uplink: uplinkEvent,
    log: logEvent,
    heartbeat: (beat) => setClock(beat.now),
    // Dispatch's plans were reset: start over from a fresh snapshot.
    reset: connect,
  };
  for (const [name, handle] of Object.entries(handlers)) {
    source.addEventListener(name, (e) => {
      if (source !== stream) return;
      alive();
      let data;
      try {
        data = JSON.parse(e.data);
      } catch {
        return;
      }
      handle(data);
    });
  }
  source.onerror = () => {
    if (source !== stream) return;
    setLive(false);
    // The browser retries a dropped stream itself, but an error answer (a 502 or 503 while the
    // console restarts) ends an EventSource for good: open a new one after a moment.
    if (source.readyState === EventSource.CLOSED) {
      clearTimeout(retry);
      retry = setTimeout(connect, 1000 + Math.random() * 4000);
    }
  };
  alive();
}

function disconnect() {
  stream?.close();
  stream = null;
  clearTimeout(watchdog);
  clearTimeout(retry);
}

/** A stream that stays silent past the heartbeat is dead, even if the browser doesn't know it. */
function alive() {
  clearTimeout(watchdog);
  watchdog = setTimeout(connect, SILENCE);
}

function setLive(up) {
  everLive ||= up;
  $("live").classList.toggle("up", up);
  const text = up ? "Live" : everLive ? "Reconnecting…" : "Connecting…";
  if ($("live-text").textContent !== text) $("live-text").textContent = text;
}
