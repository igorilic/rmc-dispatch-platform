// The bare page: the live stream as text. The map comes with the approved design.
"use strict";

const trucks = new Map();
let world = { log: [] };

function render() {
  const list = document.getElementById("trucks");
  list.replaceChildren(
    ...[...trucks.values()].map((t) => {
      const li = document.createElement("li");
      li.textContent = `${t.id}: ${t.status}${t.position ? ` at ${t.position.lat.toFixed(4)}, ${t.position.lon.toFixed(4)}` : ""}`;
      return li;
    }),
  );
  const log = document.getElementById("log");
  log.replaceChildren(
    ...world.log.map((e) => {
      const li = document.createElement("li");
      li.textContent = `${e.at} ${e.action} ${e.target}: ${e.outcome}`;
      return li;
    }),
  );
  document.getElementById("plan").textContent = world.plan ? `Plan v${world.plan.version}` : "No plan yet";
}

function connect() {
  const stream = new EventSource("events");
  stream.addEventListener("snapshot", (e) => {
    world = JSON.parse(e.data);
    trucks.clear();
    world.trucks.forEach((t) => trucks.set(t.id, t));
    document.getElementById("status").textContent = "Live";
    render();
  });
  stream.addEventListener("truck", (e) => {
    const t = JSON.parse(e.data);
    trucks.set(t.id, t);
    render();
  });
  stream.addEventListener("plan", (e) => {
    world.plan = JSON.parse(e.data);
    render();
  });
  stream.addEventListener("log", (e) => {
    world.log.unshift(JSON.parse(e.data));
    render();
  });
  stream.onerror = () => {
    document.getElementById("status").textContent = "Reconnecting…";
    if (stream.readyState === EventSource.CLOSED) {
      setTimeout(connect, 1000 + Math.random() * 4000);
    }
  };
}

connect();
