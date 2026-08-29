const STAGE_ORDER = ['DOWNLOAD','CROP','LEE_FILTER','QUALITY_ANALYTICS','FUSION','CLEANUP'];
const TIER_STAGE = { RAW: 'DOWNLOAD', BRONZE: 'CROP', SILVER: 'LEE_FILTER', GOLD: 'FUSION' };
const TIER_COLORS = { RAW: '#8B7FE8', BRONZE: '#C97C4B', SILVER: '#9FB0C9', GOLD: '#35D0C0' };
const ACTIVE_STATUSES = new Set(['QUEUED','PREPARING','DOWNLOADING','PROCESSING','PAUSED','CLEANUP','DELETING']);

const state = { datasets: [], progress: {}, logs: {}, pollTimer: null, livePollTimer: null, regions: [], selectedRegionId: null, openScenes: new Set() };

async function api(path, options) {
  const opts = Object.assign({ headers: { 'Content-Type': 'application/json' } }, options || {});
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch (e) {}
  if (!res.ok) {
    const msg = (data && data.detail) ? data.detail : ('Permintaan gagal (' + res.status + ')');
    throw new Error(msg);
  }
  return data;
}

function showToast(message, kind) {
  const stack = document.getElementById('toastStack');
  const el = document.createElement('div');
  el.className = 'toast ' + (kind || '');
  el.textContent = message;
  stack.appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function escapeHTML(s) {
  const d = document.createElement('div');
  d.textContent = s == null ? '' : String(s);
  return d.innerHTML;
}

function humanBytes(n) {
  if (!n) return '0 MB';
  const gb = n / 1e9;
  if (gb >= 1) return gb.toFixed(2) + ' GB';
  return (n / 1e6).toFixed(1) + ' MB';
}

function statusToClass(status) {
  if (['COMPLETED'].includes(status)) return 'ok';
  if (['PAUSED', 'QUEUED', 'PREPARING', 'PENDING'].includes(status)) return 'warn';
  if (['FAILED', 'CANCELLED', 'DELETING'].includes(status)) return 'danger';
  return 'active';
}

function buildRingSVG(ratios, size) {
  size = size || 96;
  const tiers = ['RAW', 'BRONZE', 'SILVER', 'GOLD'];
  const cx = size / 2, cy = size / 2;
  const baseR = size * 0.14;
  const step = size * 0.11;
  let circles = '';
  tiers.forEach((t, i) => {
    const r = baseR + step * i;
    const circumference = 2 * Math.PI * r;
    const ratio = ratios[t] || 0;
    const dash = circumference * ratio;
    circles += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + TIER_COLORS[t] + '" stroke-opacity="0.16" stroke-width="4"></circle>';
    circles += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + TIER_COLORS[t] + '" stroke-width="4" stroke-linecap="round" stroke-dasharray="' + dash + ' ' + (circumference - dash) + '" transform="rotate(-90 ' + cx + ' ' + cy + ')"></circle>';
  });
  return '<svg viewBox="0 0 ' + size + ' ' + size + '" width="' + size + '" height="' + size + '">' + circles + '</svg>';
}

function tierCompletionRatios(scenes, requiredTiers) {
  const tiers = ['RAW', 'BRONZE', 'SILVER', 'GOLD'];
  const ratios = {};
  const total = scenes.length;
  tiers.forEach(t => {
    if (!requiredTiers.includes(t)) { ratios[t] = 0; return; }
    if (total === 0) { ratios[t] = 0; return; }
    const stageIdx = STAGE_ORDER.indexOf(TIER_STAGE[t]);
    let reached = 0;
    scenes.forEach(s => {
      const curIdx = STAGE_ORDER.indexOf(s.current_stage);
      if (curIdx > stageIdx) reached++;
      else if (curIdx === stageIdx && s.stage_status === 'COMPLETED') reached++;
    });
    ratios[t] = reached / total;
  });
  return ratios;
}

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
  document.querySelectorAll('.view').forEach(v => v.classList.toggle('hidden', v.id !== ('view-' + name)));
  if (name === 'datasets') { loadDatasets(); startDatasetPolling(); } else { stopDatasetPolling(); }
  if (name === 'live') { loadLive(); startLivePolling(); } else { stopLivePolling(); }
}
document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));

const floatNav = document.getElementById('floatNav');
floatNav.querySelector('.floatnav-brand').addEventListener('click', () => floatNav.classList.toggle('expanded'));

async function checkHealth() {
  try {
    const h = await api('/api/health');
    setStatus(h.db_connected ? 'ok' : 'degraded', h.db_connected ? 'Terhubung' : 'Basis data bermasalah');
  } catch (e) {
    setStatus('down', 'Tidak terhubung');
  }
}
function setStatus(kind, label) {
  document.getElementById('statusDot').className = 'status-dot ' + kind;
  document.getElementById('statusLabel').textContent = label;
}
checkHealth();
setInterval(checkHealth, 15000);

document.getElementById('tierAll').addEventListener('change', (e) => {
  document.querySelectorAll('.tier-check').forEach(c => c.checked = e.target.checked);
});

const COLOR_TILE_URL = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}';
const COLOR_TILE_ATTR = 'Tiles &copy; Esri &mdash; Esri, HERE, Garmin, USGS, Intermap, NRCan, METI, OpenStreetMap contributors, GIS User Community';

let locationMap = null;
let mapActivated = false;
let selectedBBox = null; // [minLon, minLat, maxLon, maxLat]
let boxDrag = null;

function initLocationMap() {
  locationMap = L.map('locationMap', { zoomControl: true, attributionControl: false }).setView([-6.3, 106.9], 9);
  locationMap.on('move zoom', syncSelectionBoxFromBBox);
  window.addEventListener('resize', () => locationMap.invalidateSize());
  initSelectionBoxDrag();
  requestAnimationFrame(() => locationMap.invalidateSize());
}

function activateMap() {
  if (mapActivated) return;
  mapActivated = true;
  L.tileLayer(COLOR_TILE_URL, { maxZoom: 19, attribution: COLOR_TILE_ATTR }).addTo(locationMap);
  document.getElementById('mapPlaceholder').classList.add('hidden');
  document.getElementById('mapSelectionBox').classList.add('active');
}

function updateMapPreview(bbox) {
  if (!locationMap) initLocationMap();
  activateMap();
  selectedBBox = bbox.slice();
  locationMap.invalidateSize();
  const minLon = bbox[0], minLat = bbox[1], maxLon = bbox[2], maxLat = bbox[3];
  locationMap.fitBounds([[minLat, minLon], [maxLat, maxLon]], { padding: [24, 24] });
  syncSelectionBoxFromBBox();
  renderBBoxReadout();
  setTimeout(() => {
    if (!locationMap) return;
    locationMap.invalidateSize();
    locationMap.fitBounds([[minLat, minLon], [maxLat, maxLon]], { padding: [24, 24], animate: false });
    syncSelectionBoxFromBBox();
  }, 200);
}

function clearMapPreview() {
  selectedBBox = null;
  document.getElementById('mapBboxReadout').textContent = '';
  const box = document.getElementById('mapSelectionBox');
  box.classList.remove('active');
  box.style.transform = ''; box.style.left = ''; box.style.top = ''; box.style.width = ''; box.style.height = '';
  if (locationMap) locationMap.setView([-6.3, 106.9], 9);
}

function syncSelectionBoxFromBBox() {
  if (!selectedBBox || !locationMap) return;
  const box = document.getElementById('mapSelectionBox');
  const minLon = selectedBBox[0], minLat = selectedBBox[1], maxLon = selectedBBox[2], maxLat = selectedBBox[3];
  const p1 = locationMap.latLngToContainerPoint([maxLat, minLon]);
  const p2 = locationMap.latLngToContainerPoint([minLat, maxLon]);
  box.style.transform = 'none';
  box.style.left = Math.min(p1.x, p2.x) + 'px';
  box.style.top = Math.min(p1.y, p2.y) + 'px';
  box.style.width = Math.max(24, Math.abs(p2.x - p1.x)) + 'px';
  box.style.height = Math.max(24, Math.abs(p2.y - p1.y)) + 'px';
}

function renderBBoxReadout() {
  if (!selectedBBox) return;
  const [minLon, minLat, maxLon, maxLat] = selectedBBox;
  document.getElementById('mapBboxReadout').innerHTML =
    'Area: <span class="val">' + minLat.toFixed(3) + ', ' + minLon.toFixed(3) + '</span> &rarr; <span class="val">' + maxLat.toFixed(3) + ', ' + maxLon.toFixed(3) + '</span>';
}

function initSelectionBoxDrag() {
  const box = document.getElementById('mapSelectionBox');
  box.addEventListener('pointerdown', (e) => {
    if (!mapActivated || !selectedBBox) return;
    box.setPointerCapture(e.pointerId);
    box.classList.add('dragging');
    boxDrag = { startX: e.clientX, startY: e.clientY, startBBox: selectedBBox.slice() };
  });
  box.addEventListener('pointermove', (e) => {
    if (!boxDrag) return;
    const dx = e.clientX - boxDrag.startX;
    const dy = e.clientY - boxDrag.startY;
    const origin = locationMap.latLngToContainerPoint([boxDrag.startBBox[1], boxDrag.startBBox[0]]);
    const shifted = locationMap.containerPointToLatLng([origin.x + dx, origin.y + dy]);
    const lonDelta = shifted.lng - boxDrag.startBBox[0];
    const latDelta = shifted.lat - boxDrag.startBBox[1];
    selectedBBox = [
      boxDrag.startBBox[0] + lonDelta, boxDrag.startBBox[1] + latDelta,
      boxDrag.startBBox[2] + lonDelta, boxDrag.startBBox[3] + latDelta,
    ];
    syncSelectionBoxFromBBox();
  });
  const endDrag = (e) => {
    if (!boxDrag) return;
    box.classList.remove('dragging');
    boxDrag = null;
    renderBBoxReadout();
  };
  box.addEventListener('pointerup', endDrag);
  box.addEventListener('pointercancel', endDrag);
}

let bgMap = null;
function initBgMap() {
  bgMap = L.map('bgMap', {
    zoomControl: false,
    attributionControl: false,
    dragging: false,
    scrollWheelZoom: false,
    doubleClickZoom: false,
    boxZoom: false,
    keyboard: false,
    touchZoom: false,
    tap: false
  }).setView([-6.28, 106.85], 10);
  L.tileLayer(COLOR_TILE_URL, { maxZoom: 19, attribution: COLOR_TILE_ATTR }).addTo(bgMap);
  window.addEventListener('resize', () => bgMap.invalidateSize());
}
initBgMap();

function renderRegionCards() {
  const grid = document.getElementById('regionGrid');
  grid.innerHTML = state.regions.map(r =>
    '<div class="region-card" data-region-id="' + r.region_id + '">' +
      '<div class="region-check"></div>' +
      '<div class="region-name">' + escapeHTML(r.name) + '</div>' +
      '<div class="region-area">' + (r.area_km2 ? r.area_km2.toFixed(1) + ' km2' : '') + '</div>' +
    '</div>'
  ).join('');
  grid.querySelectorAll('.region-card').forEach(card => {
    card.addEventListener('click', () => selectRegion(Number(card.dataset.regionId)));
  });
}

function selectRegion(id) {
  state.selectedRegionId = id;
  document.querySelectorAll('.region-card').forEach(c => c.classList.toggle('selected', Number(c.dataset.regionId) === id));
  const region = state.regions.find(r => r.region_id === id);
  if (region) {
    document.getElementById('fLocation').value = region.name;
    updateMapPreview(region.bbox);
  }
}

function clearRegionSelection() {
  state.selectedRegionId = null;
  document.querySelectorAll('.region-card').forEach(c => c.classList.remove('selected'));
  clearMapPreview();
}

document.getElementById('fLocation').addEventListener('input', () => {
  const val = document.getElementById('fLocation').value.trim().toLowerCase();
  const match = state.regions.find(r => r.name.toLowerCase() === val);
  if (match) {
    state.selectedRegionId = match.region_id;
    document.querySelectorAll('.region-card').forEach(c => c.classList.toggle('selected', Number(c.dataset.regionId) === match.region_id));
    updateMapPreview(match.bbox);
  } else {
    document.querySelectorAll('.region-card').forEach(c => c.classList.remove('selected'));
    state.selectedRegionId = null;
    if (val === '') clearMapPreview();
  }
});

initLocationMap();

async function loadRegions() {
  try {
    const result = await api('/api/regions');
    state.regions = result.items;
    renderRegionCards();
  } catch (e) {
    document.getElementById('regionGrid').innerHTML = '<div class="empty-small">Gagal memuat daftar wilayah</div>';
  }
}
loadRegions();

document.getElementById('createForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const tiers = Array.from(document.querySelectorAll('.tier-check:checked')).map(c => c.value);
  if (tiers.length === 0) { showToast('Pilih minimal satu tier data', 'error'); return; }
  const location = document.getElementById('fLocation').value.trim();
  const dateStart = document.getElementById('fDateStart').value;
  const dateEnd = document.getElementById('fDateEnd').value;
  const name = document.getElementById('fName').value.trim();
  if (!location || !dateStart || !dateEnd || !name) { showToast('Lengkapi lokasi, tanggal, dan nama dataset', 'error'); return; }
  const qs = {};
  const cloud = document.getElementById('fMinCloud').value;
  const qual = document.getElementById('fMinQuality').value;
  const res = document.getElementById('fResolution').value;
  if (cloud !== '') qs.min_cloud_cover = Number(cloud);
  if (qual !== '') qs.min_quality_score = Number(qual);
  if (res !== '') qs.resolution_m = Number(res);
  const body = {
    location: location, date_start: dateStart, date_end: dateEnd, tiers: tiers, name: name,
    description: document.getElementById('fDescription').value.trim() || null,
    quality_settings: Object.keys(qs).length ? qs : null,
  };
  const submitBtn = document.getElementById('createSubmit');
  submitBtn.disabled = true; submitBtn.textContent = 'Membuat...';
  try {
    const result = await api('/api/datasets', { method: 'POST', body: JSON.stringify(body) });
    showToast('Dataset dibuat (status: ' + result.status + ')', 'success');
    e.target.reset();
    document.getElementById('fLocation').value = '';
    document.querySelectorAll('.tier-check').forEach(c => c.checked = c.value === 'GOLD');
    clearRegionSelection();
    switchTab('datasets');
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    submitBtn.disabled = false; submitBtn.textContent = 'Buat Dataset';
  }
});

async function loadDatasets() {
  try {
    const result = await api('/api/datasets?limit=50');
    state.datasets = result.items;
    await refreshProgress();
  } catch (err) { showToast(err.message, 'error'); }
}
async function loadDatasetsQuiet() {
  try {
    const result = await api('/api/datasets?limit=50');
    state.datasets = result.items;
  } catch (e) {}
}
async function refreshProgress() {
  for (const ds of state.datasets) {
    if (ACTIVE_STATUSES.has(ds.status) || !state.progress[ds.dataset_id]) {
      try { state.progress[ds.dataset_id] = await api('/api/datasets/' + ds.dataset_id + '/status'); }
      catch (e) {}
    }
    if (ACTIVE_STATUSES.has(ds.status) || !state.logs[ds.dataset_id]) {
      try { state.logs[ds.dataset_id] = (await api('/api/datasets/' + ds.dataset_id + '/logs?limit=5')).logs; }
      catch (e) {}
    }
  }
  renderDatasets();
}
function startDatasetPolling() {
  stopDatasetPolling();
  state.pollTimer = setInterval(async () => { await loadDatasetsQuiet(); await refreshProgress(); }, 2000);
}
function stopDatasetPolling() { if (state.pollTimer) clearInterval(state.pollTimer); state.pollTimer = null; }
document.getElementById('refreshDatasets').addEventListener('click', loadDatasets);

function renderDatasets() {
  const container = document.getElementById('datasetList');
  if (state.datasets.length === 0) {
    container.innerHTML = '<div class="empty">Belum ada dataset. Buat satu di tab Buat Dataset.</div>';
    return;
  }
  container.innerHTML = '';
  state.datasets.forEach(ds => container.appendChild(renderDatasetCard(ds)));
}

function renderDatasetCard(ds) {
  const el = document.createElement('div');
  el.className = 'card';
  const prog = state.progress[ds.dataset_id];
  const scenes = prog ? prog.scenes : [];
  const ratios = tierCompletionRatios(scenes, ds.required_tiers);
  const ringHTML = buildRingSVG(ratios);
  const statusClass = statusToClass(ds.status);
  const canPause = ['QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canResume = ds.status === 'PAUSED';
  const canRetry = ds.status === 'FAILED';
  const canCancel = ['DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canDownload = ds.total_size_bytes > 0;
  const spinning = ACTIVE_STATUSES.has(ds.status) && ds.status !== 'PAUSED';
  el.innerHTML =
    '<div class="card-head">' +
      '<div class="card-ring' + (spinning ? ' spinning' : '') + '">' + ringHTML + '</div>' +
      '<div class="card-info">' +
        '<div class="card-title-row">' +
          '<span class="card-name">' + escapeHTML(ds.name) + '</span>' +
          '<span class="badge ' + statusClass + '">' + ds.status + '</span>' +
        '</div>' +
        '<div class="card-meta">' + escapeHTML(ds.location_label || '-') + ' &middot; ' + ds.date_start + ' - ' + ds.date_end + '</div>' +
        '<div class="card-tiers">' + ds.required_tiers.map(t => '<span class="chip" style="--chip-color:' + TIER_COLORS[t] + '">' + t + '</span>').join('') + '</div>' +
      '</div>' +
    '</div>' +
    '<div class="card-stats">' +
      '<div><span class="stat-num">' + ds.total_scenes + '</span><span class="stat-label">scene</span></div>' +
      '<div><span class="stat-num">' + ds.completed_scenes + '</span><span class="stat-label">selesai</span></div>' +
      '<div><span class="stat-num">' + ds.failed_scenes + '</span><span class="stat-label">gagal</span></div>' +
      '<div><span class="stat-num">' + humanBytes(ds.total_size_bytes) + '</span><span class="stat-label">ukuran</span></div>' +
    '</div>' +
    renderLogPanel(ds.dataset_id) +
    '<div class="card-actions">' +
      (canPause ? '<button class="btn btn-warn" data-action="pause">Jeda</button>' : '') +
      (canResume ? '<button class="btn btn-accent" data-action="resume">Lanjutkan</button>' : '') +
      (canRetry ? '<button class="btn btn-accent" data-action="retry">Coba lagi</button>' : '') +
      (canCancel ? '<button class="btn btn-danger" data-action="cancel">Batalkan</button>' : '') +
      (canDownload ? '<a class="btn btn-ghost" href="/api/datasets/' + ds.dataset_id + '/download">Unduh</a>' : '') +
      '<button class="btn btn-danger" data-action="delete">Hapus</button>' +
      '<button class="btn btn-ghost" data-action="toggle-scenes">Detail</button>' +
    '</div>' +
    '<div class="card-scenes' + (state.openScenes.has(ds.dataset_id) ? '' : ' hidden') + '" id="scenes-' + ds.dataset_id + '"></div>';
  el.querySelectorAll('[data-action]').forEach(btn => btn.addEventListener('click', () => handleCardAction(btn.dataset.action, ds.dataset_id)));
  if (state.openScenes.has(ds.dataset_id)) renderSceneTable(el.querySelector('.card-scenes'), ds.dataset_id);
  return el;
}

function renderLogPanel(id) {
  const logs = state.logs[id];
  if (!logs || logs.length === 0) return '';
  return '<div class="live-logs-title">Live Logs (terbaru)</div>' +
    '<div class="live-logs">' +
      logs.map(l => {
        const t = new Date(l.timestamp).toLocaleTimeString('id-ID', { hour12: false });
        const detail = formatLogDetail(l);
        return '<div class="log-row ' + logStatusClass(l.status) + '">' +
          '<span class="log-time">' + t + '</span>' +
          '<span class="log-scene">' + escapeHTML(shortenSceneId(l.scene_id)) + '</span>' +
          '<span class="log-stage">' + escapeHTML(l.stage) + '</span>' +
          '<span class="log-status">' + escapeHTML(l.status) + '</span>' +
          '<span class="log-msg">' + escapeHTML(l.message || '') + '</span>' +
        '</div>' +
        (detail ? '<div class="log-detail">' + detail + '</div>' : '');
      }).join('') +
    '</div>';
}
function formatLogDetail(l) {
  const d = l.details || {};
  const parts = [];
  if (d.progress_percent !== undefined && l.status === 'RUNNING') parts.push('Progress: ' + d.progress_percent + '%');
  if (d.attempt !== undefined && d.max_retries !== undefined) parts.push('Attempt: ' + d.attempt + '/' + d.max_retries);
  if (d.duration_seconds !== undefined) parts.push('Duration: ' + formatDuration(d.duration_seconds));
  if (d.file_size_mb !== undefined && d.file_size_mb !== null) parts.push('Size: ' + d.file_size_mb.toFixed(1) + ' MB');
  if (d.quality_score !== undefined && d.quality_score !== null) parts.push('Quality: ' + d.quality_score + '/100');
  if (d.memory_peak_mb !== undefined) parts.push('Mem peak: ' + d.memory_peak_mb.toFixed(0) + ' MB');
  if (l.status === 'FAILED' && d.error_type) parts.push(d.error_type + ': ' + (d.error_message || ''));
  return escapeHTML(parts.join('  ·  '));
}
function formatDuration(seconds) {
  if (seconds === undefined || seconds === null) return '-';
  const s = Math.round(seconds);
  if (s < 60) return s + 's';
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return m + 'm ' + rem + 's';
}
function logStatusClass(status) {
  if (status === 'COMPLETED') return 'log-ok';
  if (status === 'RUNNING') return 'log-progress';
  if (status === 'FAILED') return 'log-error';
  return 'log-muted';
}
function shortenSceneId(id) {
  if (!id) return '-';
  return id.length > 28 ? id.slice(0, 25) + '...' : id;
}

async function handleCardAction(action, id) {
  if (action === 'pause') {
    try { await api('/api/datasets/' + id + '/pause', { method: 'POST', body: JSON.stringify({}) }); showToast('Dataset dijeda', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'resume') {
    try { await api('/api/datasets/' + id + '/resume', { method: 'POST' }); showToast('Dataset dilanjutkan', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'retry') {
    try { await api('/api/pipeline/trigger?dataset_id=' + id, { method: 'POST' }); showToast('Job dijalankan ulang', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'cancel') {
    openCancelModal(id);
  } else if (action === 'delete') {
    openDeleteModal(id);
  } else if (action === 'toggle-scenes') {
    const box = document.getElementById('scenes-' + id);
    box.classList.toggle('hidden');
    if (!box.classList.contains('hidden')) { state.openScenes.add(id); renderSceneTable(box, id); }
    else { state.openScenes.delete(id); }
  }
}

function renderSceneTable(box, id) {
  const prog = state.progress[id];
  if (!prog || prog.scenes.length === 0) { box.innerHTML = '<div class="empty-small">Belum ada scene</div>'; return; }
  box.innerHTML = '<table class="scene-table"><thead><tr><th>Scene</th><th>Tahap</th><th>Status</th><th>Catatan</th></tr></thead><tbody>' +
    prog.scenes.map(s => '<tr><td class="mono">' + escapeHTML(s.product_identifier) + '</td><td>' + (s.current_stage || '-') + '</td><td><span class="badge ' + statusToClass(s.stage_status) + '">' + s.stage_status + '</span></td><td class="mono small">' + escapeHTML(s.last_error || '') + '</td></tr>').join('') +
    '</tbody></table>';
}

let pendingDeleteId = null;
function openDeleteModal(id) { pendingDeleteId = id; document.getElementById('deleteModal').classList.remove('hidden'); }
function closeDeleteModal() { document.getElementById('deleteModal').classList.add('hidden'); pendingDeleteId = null; document.getElementById('deleteForce').checked = false; }
document.getElementById('deleteCancel').addEventListener('click', closeDeleteModal);
document.getElementById('deleteConfirm').addEventListener('click', async () => {
  const force = document.getElementById('deleteForce').checked;
  try {
    await api('/api/datasets/' + pendingDeleteId + '?force=' + force, { method: 'DELETE' });
    showToast('Penghapusan dimulai', 'success');
    closeDeleteModal();
    await loadDatasets();
  } catch (e) { showToast(e.message, 'error'); }
});

let pendingCancelId = null;
function openCancelModal(id) { pendingCancelId = id; document.getElementById('cancelModal').classList.remove('hidden'); }
function closeCancelModal() { document.getElementById('cancelModal').classList.add('hidden'); pendingCancelId = null; }
document.getElementById('cancelModalCancel').addEventListener('click', closeCancelModal);
document.getElementById('cancelModalConfirm').addEventListener('click', async () => {
  const btn = document.getElementById('cancelModalConfirm');
  btn.disabled = true; btn.textContent = 'Membatalkan...';
  try {
    const r = await api('/api/datasets/' + pendingCancelId + '/cancel', { method: 'POST', body: JSON.stringify({ cascade_delete: true }) });
    showToast('Dataset dibatalkan (' + r.deleted_files + ' file dihapus, tier ' + r.retained_tier + ' disimpan)', 'success');
    closeCancelModal();
    await loadDatasets();
  } catch (e) { showToast(e.message, 'error'); }
  finally { btn.disabled = false; btn.textContent = 'Ya, Batalkan'; }
});

async function loadLive() {
  try {
    const live = await api('/api/live');
    renderLive(live);
  } catch (err) {
    document.getElementById('sourceList').innerHTML = '<div class="empty-small">' + escapeHTML(err.message) + '</div>';
  }
}
function startLivePolling() { stopLivePolling(); state.livePollTimer = setInterval(loadLive, 5000); }
function stopLivePolling() { if (state.livePollTimer) clearInterval(state.livePollTimer); state.livePollTimer = null; }

function renderLive(live) {
  document.getElementById('liveToggle').checked = live.enabled;
  document.getElementById('liveStatusText').textContent = live.status;
  document.getElementById('liveSize').textContent = humanBytes(live.total_size_bytes);
  document.getElementById('liveChecked').textContent = live.last_checked_at ? new Date(live.last_checked_at).toLocaleString('id-ID') : 'Belum pernah';
  document.getElementById('liveDownload').href = '/api/datasets/' + live.dataset_id + '/download';
  const src = document.getElementById('sourceList');
  src.innerHTML = live.sources.map(s =>
    '<div class="source-row">' +
      '<span class="dot ' + (s.enabled ? 'ok' : 'muted') + '"></span>' +
      '<span class="source-name">' + s.source_name + '</span>' +
      '<span class="source-meta">cek: ' + (s.last_check ? new Date(s.last_check).toLocaleString('id-ID') : '-') + '</span>' +
      '<span class="source-meta">ambil: ' + (s.last_ingest ? new Date(s.last_ingest).toLocaleString('id-ID') : '-') + '</span>' +
    '</div>'
  ).join('');
  loadLiveScenes();
}

async function loadLiveScenes() {
  try {
    const scenes = await api('/api/live/scenes?limit=20');
    const box = document.getElementById('liveScenes');
    if (scenes.length === 0) { box.innerHTML = '<div class="empty-small">Belum ada data</div>'; return; }
    box.innerHTML = '<table class="scene-table"><thead><tr><th>Tanggal</th><th>Tier</th><th>Ukuran</th></tr></thead><tbody>' +
      scenes.map(s => '<tr><td>' + new Date(s.scene_date).toLocaleString('id-ID') + '</td><td><span class="chip" style="--chip-color:' + (TIER_COLORS[s.tier] || '#35D0C0') + '">' + s.tier + '</span></td><td>' + s.size_mb.toFixed(1) + ' MB</td></tr>').join('') +
      '</tbody></table>';
  } catch (e) {}
}

document.getElementById('liveToggle').addEventListener('change', async (e) => {
  try { await api('/api/live/toggle', { method: 'POST', body: JSON.stringify({ enabled: e.target.checked }) }); showToast(e.target.checked ? 'Live diaktifkan' : 'Live dinonaktifkan', 'success'); }
  catch (err) { showToast(err.message, 'error'); e.target.checked = !e.target.checked; }
});

document.getElementById('liveClearBtn').addEventListener('click', () => document.getElementById('clearModal').classList.remove('hidden'));
document.getElementById('clearCancel').addEventListener('click', () => document.getElementById('clearModal').classList.add('hidden'));
document.getElementById('clearConfirm').addEventListener('click', async () => {
  try { const r = await api('/api/live/clear', { method: 'POST' }); showToast('Dikosongkan: ' + r.deleted_count + ' file', 'success'); loadLive(); }
  catch (err) { showToast(err.message, 'error'); }
  document.getElementById('clearModal').classList.add('hidden');
});

document.getElementById('backfillForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const ds = document.getElementById('bfStart').value;
  const de = document.getElementById('bfEnd').value;
  if (!ds || !de) { showToast('Isi kedua tanggal', 'error'); return; }
  try {
    const r = await api('/api/live/backfill', { method: 'POST', body: JSON.stringify({ date_start: ds, date_end: de }) });
    showToast('Backfill dimulai (job ' + r.job_id + ')', 'success');
    e.target.reset();
  } catch (err) { showToast(err.message, 'error'); }
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.getElementById('deleteModal').classList.add('hidden');
    document.getElementById('cancelModal').classList.add('hidden');
    document.getElementById('clearModal').classList.add('hidden');
  }
});

switchTab('create');
