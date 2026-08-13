// web/icons.js
const Icons = {
  paths: {
    satellite: "M4 15a8 8 0 0116 0M12 15v6M9 21h6M12 3v4M9 7l3-3 3 3",
    grid: "M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z",
    sliders: "M4 6h16M4 12h16M4 18h16M8 6a1 1 0 100-2 1 1 0 000 2zM16 12a1 1 0 100-2 1 1 0 000 2zM10 18a1 1 0 100-2 1 1 0 000 2z",
    database: "M4 5c0-1 3.6-2 8-2s8 1 8 2-3.6 2-8 2-8-1-8-2zM4 5v6c0 1 3.6 2 8 2s8-1 8-2V5M4 11v6c0 1 3.6 2 8 2s8-1 8-2v-6",
    image: "M4 5h16v14H4zM8 10a1 1 0 100-2 1 1 0 000 2zM4 17l5-5 3 3 4-4 4 4",
    check: "M5 13l4 4L19 7",
    alert: "M12 3L2 20h20zM12 9v5M12 17h.01",
    x: "M6 6l12 12M18 6L6 18",
    refresh: "M4 12a8 8 0 0114-5.3M20 12a8 8 0 01-14 5.3M14 3h5v5M10 21H5v-5",
    download: "M12 3v13M7 11l5 5 5-5M4 20h16",
    clock: "M12 21a9 9 0 100-18 9 9 0 000 18zM12 7v5l3 3",
    pin: "M12 21s7-7.5 7-12a7 7 0 10-14 0c0 4.5 7 12 7 12zM12 12a2 2 0 100-4 2 2 0 000 4z",
    trash: "M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 11v6M14 11v6",
    search: "M11 19a8 8 0 100-16 8 8 0 000 16zM21 21l-4.3-4.3",
    plug: "M9 2v6M15 2v6M6 8h12v5a5 5 0 01-5 5h-2a5 5 0 01-5-5V8zM12 18v4",
    info: "M12 21a9 9 0 100-18 9 9 0 000 18zM12 11v6M12 7h.01",
    droplet: "M12 3s7 7.6 7 12a7 7 0 11-14 0c0-4.4 7-12 7-12z",
    folder: "M3 6h6l2 2h10v11H3z",
    play: "M7 4l13 8-13 8z",
    layers: "M12 3l9 5-9 5-9-5zM3 13l9 5 9-5M3 18l9 5 9-5",
    pulse: "M13 2L4 14h6l-1 8 9-12h-6z",
    chevron: "M6 9l6 6 6-6",
    map: "M9 20l-6-3V4l6 3 6-3 6 3v13l-6-3-6 3zM9 4v13M15 7v13",
    calendar: "M4 5h16v16H4zM4 9h16M8 3v4M16 3v4",
  },
  svg(name, size, strokeWidth) {
    const s = size || 18;
    const w = strokeWidth || 2;
    const d = this.paths[name] || "";
    const parts = d.split(/(?=M)/).map((seg) => `<path d="${seg}"/>`).join("");
    return `<svg viewBox="0 0 24 24" width="${s}" height="${s}" fill="none" stroke="currentColor" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round">${parts}</svg>`;
  },
};