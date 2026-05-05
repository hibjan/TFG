import { $$ } from './utils.js';
import { api } from './api.js';
import PaginationHandler from './components/PaginationHandler.js';
import { addFilterTag, clearTags } from './components/FilterTags.js';
import { renderModalContent } from './components/Modal.js';
import { initPanelDragOrder } from './components/PanelDragOrder.js';
import { createCombobox } from './components/Combobox.js';
import { Skeleton } from './components/Skeleton.js';
import { showToast } from './components/Toast.js';
import './styles/main.css';

// ──────────────────────────────────────────────
// State
// ──────────────────────────────────────────────
const state = {
    datasets: [],        // all datasets (for breadcrumb resolution on resume)
    datasetId: null,
    datasetName: '',
    collectionId: null,
    collectionName: '',
    collections: [],     // all collections for current dataset
    notMode: false,      // false = include, true = exclude
    page: 0,
    pageSize: 40,        // Filtered Collection is the primary view -> show more per page
    unionPage: 0,
    unionPageSize: 20,
    // Linear path of collections traversed via Links. The first entry is
    // the starting collection (reason: null); each subsequent entry was
    // reached by clicking the link with that `reason`.
    linkHistory: [],
    historyOpen: false,
};

// ──────────────────────────────────────────────
// DOM refs
// ──────────────────────────────────────────────
const dom = {
    screenDatasets: $$('screen-datasets'),
    screenCollections: $$('screen-collections'),
    screenNavigation: $$('screen-navigation'),

    datasets: $$('datasets'),
    collections: $$('collections'),

    exitNavBtn: $$('exit-nav-btn'),
    bcDataset: $$('bc-dataset'),
    bcCollection: $$('bc-collection'),

    toggleNotBtn: $$('toggle-not-btn'),
    clearAllFiltersBtn: $$('clear-all-filters-btn'),
    metadataFilters: $$('metadata-filters'),
    referenceFilters: $$('reference-filters'),
    metadataActiveFilters: $$('metadata-active-filters'),
    referenceActiveFilters: $$('reference-active-filters'),

    links: $$('links'),
    gobackBtn: $$('goback-btn'),
    restoreBtn: $$('restore-btn'),
    historyToggleBtn: $$('history-toggle-btn'),
    linkHistoryEl: $$('link-history'),

    entities: $$('entities'),
    entitiesCount: $$('entities-count'),
    entitiesPaginationWrap: $$('entities-pagination'),

    unionEntities: $$('union-entities'),
    unionCount: $$('union-count'),
    unionPaginationWrap: $$('union-pagination'),

    unionBtn: $$('union-btn'),
    changeBtn: $$('change-btn'),

    // Modal refs grouped for easy passing to the component
    modalRefs: {
        modal: $$('entity-modal'),
        modalClose: $$('modal-close'),
        modalName: $$('modal-entity-name'),
        modalContents: $$('modal-entity-contents'),
        modalResources: $$('modal-entity-resources'),
        modalMetadata: $$('modal-entity-metadata'),
        modalReferences: $$('modal-entity-references'),
    }
};

const entitiesPagination = new PaginationHandler(dom.entitiesPaginationWrap, async (newPage) => {
    // The UI is 1-based (Page 1, 2), but your state is 0-based (Page 0, 1)
    state.page = newPage - 1; 
    await loadEntities();
});

const unionPagination = new PaginationHandler(dom.unionPaginationWrap, async (newPage) => {
    state.unionPage = newPage - 1;
    await loadUnion();
});

// ──────────────────────────────────────────────
// Screen management
// ──────────────────────────────────────────────
function showScreen(name) {
    dom.screenDatasets.classList.toggle('hidden', name !== 'datasets');
    dom.screenCollections.classList.toggle('hidden', name !== 'collections');
    dom.screenNavigation.classList.toggle('hidden', name !== 'navigation');
}

// ──────────────────────────────────────────────
// Init
// ──────────────────────────────────────────────
async function init() {
    await loadDatasets();
    setupListeners();
    initPanelDragOrder(dom.screenNavigation);

    // Try to resume an existing session
    try {
        const session = await api('/session');
        if (session.active) {
            state.datasetId = session.datasetId;
            state.collectionId = session.collectionId;

            // Resolve dataset name from the cached datasets list
            const ds = state.datasets.find(d => d.id === session.datasetId);
            state.datasetName = ds ? ds.name : '';

            // Load collections so we can resolve names
            const colData = await api(`/collections?datasetId=${session.datasetId}`);
            state.collections = colData.collections;

            const col = state.collections.find(c => c.id === session.collectionId);
            state.collectionName = col ? col.name : '';

            // Seed link history with the current collection (no prior reason)
            state.linkHistory = [{
                collectionId: state.collectionId,
                collectionName: state.collectionName,
                reason: null,
            }];

            enterNavigation();
            return;
        }
    } catch (err) {
        console.warn('No active session, starting fresh');
    }

    showScreen('datasets');
}

// ──────────────────────────────────────────────
// Datasets
// ──────────────────────────────────────────────
async function loadDatasets() {
    Skeleton.pillRow(dom.datasets, 4, { large: true });
    try {
        const data = await api('/datasets');
        state.datasets = data.datasets || [];
        dom.datasets.innerHTML = '';
        state.datasets.forEach(ds => {
            const btn = document.createElement('button');
            btn.textContent = ds.name;
            btn.className = 'large-selection-btn';
            btn.onclick = () => selectDataset(ds.id, ds.name);
            dom.datasets.appendChild(btn);
        });
    } catch (err) {
        console.error('Failed to load datasets:', err);
        dom.datasets.innerHTML = '<p class="empty-state">Failed to load datasets</p>';
        showToast(`Failed to load datasets: ${err.message || err}`, 'error');
    }
}

async function selectDataset(id, name) {
    state.datasetId = id;
    state.datasetName = name;

    Skeleton.pillRow(dom.collections, 6, { large: true });
    showScreen('collections');

    try {
        const data = await api(`/collections?datasetId=${id}`);
        state.collections = data.collections;

        dom.collections.innerHTML = '';
        data.collections.forEach(col => {
            const btn = document.createElement('button');
            btn.textContent = col.name;
            btn.className = 'large-selection-btn';
            btn.onclick = () => selectCollection(col.id, col.name);
            dom.collections.appendChild(btn);
        });
    } catch (err) {
        console.error('Failed to load collections:', err);
        dom.collections.innerHTML = '<p class="empty-state">Failed to load collections</p>';
        showToast(`Failed to load collections: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Collections
// ──────────────────────────────────────────────
async function selectCollection(id, name) {
    state.collectionId = id;
    state.collectionName = name;
    state.page = 0;
    state.unionPage = 0;
    resetLinkHistory();

    try {
        // Create a new session
        await api('/session', {
            method: 'POST',
            body: { datasetId: state.datasetId, collectionId: id },
        });

        enterNavigation();
    } catch (err) {
        console.error('Failed to init session:', err);
        showToast(`Failed to start session: ${err.message || err}`, 'error');
    }
}

async function enterNavigationForUnion(id, name) {
    state.collectionId = id;
    state.collectionName = name;
    state.page = 0;
    state.unionPage = 0;
    resetLinkHistory();

    try {
        // Perform union action (saves current filters + switches collection)
        await api('/navigation', {
            method: 'POST',
            body: { action: 'union', collectionId: id },
        });

        enterNavigation();
        showToast('Saved current results to Union', 'success', 2500);
    } catch (err) {
        console.error('Failed to perform union:', err);
        showToast(`Failed to add to Union: ${err.message || err}`, 'error');
    }
}

function enterNavigation() {
    renderBreadcrumb();
    renderLinkHistory();
    updateNavButtons();
    showScreen('navigation');
    refreshAll();
}

// ──────────────────────────────────────────────
// Breadcrumb / link-history rendering
// ──────────────────────────────────────────────
function renderBreadcrumb() {
    dom.bcDataset.textContent = state.datasetName || 'Dataset';
    dom.bcCollection.textContent = state.collectionName || 'Collection';
}

function resetLinkHistory() {
    state.linkHistory = [{
        collectionId: state.collectionId,
        collectionName: state.collectionName,
        reason: null,
    }];
}

function renderLinkHistory() {
    const el = dom.linkHistoryEl;
    el.innerHTML = '';

    if (!state.linkHistory || state.linkHistory.length === 0) {
        el.innerHTML = '<span class="empty-hint">No history yet</span>';
        return;
    }

    state.linkHistory.forEach((entry, i) => {
        if (i > 0) {
            const arrow = document.createElement('div');
            arrow.className = 'history-arrow';

            const reason = document.createElement('span');
            reason.className = 'history-arrow-reason';
            reason.textContent = entry.reason || '';
            reason.title = entry.reason || '';

            const line = document.createElement('span');
            line.className = 'history-arrow-line';
            line.textContent = '→'; // →

            arrow.appendChild(reason);
            arrow.appendChild(line);
            el.appendChild(arrow);
        }

        const node = document.createElement('div');
        node.className = 'history-node';
        if (i === 0) node.classList.add('start');
        if (i === state.linkHistory.length - 1) node.classList.add('current');
        node.textContent = entry.collectionName || `Collection #${entry.collectionId}`;
        node.title = node.textContent;
        el.appendChild(node);
    });
}

function toggleHistoryView() {
    state.historyOpen = !state.historyOpen;
    dom.linkHistoryEl.classList.toggle('hidden', !state.historyOpen);
    dom.historyToggleBtn.classList.toggle('active', state.historyOpen);
    dom.historyToggleBtn.setAttribute('aria-expanded', String(state.historyOpen));
    dom.historyToggleBtn.innerHTML = state.historyOpen
        ? 'History &#9662;'  // ▾
        : 'History &#9656;'; // ▸
    if (state.historyOpen) renderLinkHistory();
}

function updateNavButtons() {
    // Goback is only meaningful when there's at least one link to undo
    const canGoBack = state.linkHistory.length > 1;
    dom.gobackBtn.disabled = !canGoBack;
    dom.gobackBtn.classList.remove('hidden');
    dom.restoreBtn.classList.remove('hidden');
}

function exitNavigation() {
    if (!confirm('Are you sure you want to exit navigation mode? Your current session will be lost.')) {
        return;
    }
    // Reset state
    state.collectionId = null;
    state.collectionName = '';
    state.notMode = false;
    state.page = 0;
    state.unionPage = 0;
    state.linkHistory = [];
    state.historyOpen = false;

    // Reset toggle UI
    dom.toggleNotBtn.textContent = 'Include';
    dom.toggleNotBtn.className = 'mode-toggle include';

    // Collapse the history panel back to its default state
    dom.linkHistoryEl.classList.add('hidden');
    dom.historyToggleBtn.classList.remove('active');
    dom.historyToggleBtn.setAttribute('aria-expanded', 'false');
    dom.historyToggleBtn.innerHTML = 'History &#9656;';

    showScreen('datasets');
}

// ──────────────────────────────────────────────
// Union & Change flows
// ──────────────────────────────────────────────
function startUnion() {
    dom.collections.innerHTML = '';
    state.collections.forEach(col => {
        const btn = document.createElement('button');
        btn.textContent = col.name;
        btn.onclick = () => enterNavigationForUnion(col.id, col.name);
        dom.collections.appendChild(btn);
    });

    showScreen('collections');
}

function startChange() {
    if (!confirm('Switch to a different collection?\n\nThis clears your current filters and link history. The Union Set is kept.')) {
        return;
    }

    dom.collections.innerHTML = '';
    state.collections.forEach(col => {
        const btn = document.createElement('button');
        btn.textContent = col.name;
        btn.onclick = () => enterNavigationForChange(col.id, col.name);
        dom.collections.appendChild(btn);
    });

    showScreen('collections');
}

async function enterNavigationForChange(id, name) {
    state.collectionId = id;
    state.collectionName = name;
    state.page = 0;
    state.unionPage = 0;
    resetLinkHistory();

    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'change', collectionId: id },
        });

        enterNavigation();
    } catch (err) {
        console.error('Failed to change collection:', err);
        showToast(`Failed to switch collection: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Data Refreshers
// ──────────────────────────────────────────────
async function refreshAll() {
    await Promise.all([
        loadEntities(),
        loadMetadataFacets(),
        loadReferenceFacets(),
        loadLinkFacets(),
        loadUnion(),
    ]);
}

async function loadEntities() {
    Skeleton.entityGrid(dom.entities, Math.min(state.pageSize, 18));
    try {
        const data = await api(`/entities?page=${state.page}&size=${state.pageSize}`);

        // --- SAFE FALLBACKS ---
        // If the backend sends nothing, default to empty array/0
        const entitiesList = data.entities || [];
        const totalEntities = data.total || 0;

        dom.entities.innerHTML = '';
        if (entitiesList.length === 0) {
            dom.entities.innerHTML = '<p class="empty-state">No entities found</p>';
        } else {
            entitiesList.forEach(ent => {
                const card = document.createElement('div');
                card.className = 'entity-card';
                card.textContent = ent.name;
                card.title = ent.name; // full name as native tooltip when ellipsized
                card.onclick = () => viewEntity(ent.id);
                dom.entities.appendChild(card);
            });
        }

        const col = state.collections.find(c => c.id === data.collectionId);
        if (col) {
            state.collectionId = col.id;
            state.collectionName = col.name;
            renderBreadcrumb();
        }

        dom.entitiesCount.textContent = `${totalEntities} entities`;
        
        // Render Pagination safely
        const totalPages = Math.max(1, Math.ceil(totalEntities / state.pageSize));
        entitiesPagination.render(state.page + 1, totalPages);

    } catch (err) {
        console.error('Failed to load entities:', err);
        dom.entities.innerHTML = '<p class="empty-state">Failed to load entities</p>';
        showToast(`Failed to load entities: ${err.message || err}`, 'error');
    }
}

async function loadUnion() {
    Skeleton.entityGrid(dom.unionEntities, Math.min(state.unionPageSize, 8));
    try {
        const data = await api(`/union?page=${state.unionPage}&size=${state.unionPageSize}`);

        // --- SAFE FALLBACKS ---
        const unionList = data.entities || [];
        const totalUnion = data.total || 0;

        dom.unionEntities.innerHTML = '';

        if (unionList.length === 0) {
            dom.unionEntities.innerHTML = '<p class="empty-state">No entries yet &mdash; click <strong>+ Add current</strong> to save these results and explore another collection.</p>';
        } else {
            unionList.forEach(ent => {
                const card = document.createElement('div');
                card.className = 'entity-card';
                card.textContent = ent.name;
                card.title = ent.name; // full name as native tooltip when ellipsized
                card.onclick = () => viewEntity(ent.id, ent.collection_id);
                dom.unionEntities.appendChild(card);
            });
        }

        dom.unionCount.textContent = totalUnion > 0 ? `${totalUnion} entities` : '';
        
        // Render Pagination safely
        const totalPages = Math.max(1, Math.ceil(totalUnion / state.unionPageSize));
        unionPagination.render(state.unionPage + 1, totalPages);

    } catch (err) {
        console.error('Failed to load union:', err);
        dom.unionEntities.innerHTML = '<p class="empty-state">Failed to load Union Set</p>';
        showToast(`Failed to load Union Set: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Facet Loaders
// ──────────────────────────────────────────────
async function loadMetadataFacets() {
    Skeleton.dropdowns(dom.metadataFilters, 3);
    try {
        const data = await api('/facets/metadata');
        const activeMfilters = data.activeFilters.mfilters || {};
        const activeNotMfilters = data.activeFilters.notMfilters || {};

        // Build a Set of already-applied values per attribute (include + exclude)
        // so we can hide them from the combobox — same value can't be picked twice.
        const usedByAttr = {};
        const collectUsed = (source) => {
            for (const [attr, values] of Object.entries(source)) {
                const list = Array.isArray(values) ? values : Object.values(values);
                if (!usedByAttr[attr]) usedByAttr[attr] = new Set();
                list.forEach(v => usedByAttr[attr].add(String(v)));
            }
        };
        collectUsed(activeMfilters);
        collectUsed(activeNotMfilters);

        // Render dropdowns, omitting consumed values (and the whole wrap if all consumed)
        dom.metadataFilters.innerHTML = '';
        for (const [key, values] of Object.entries(data.metadata)) {
            const used = usedByAttr[key] || new Set();
            const available = values.filter(v => !used.has(String(v.value)));
            if (available.length === 0) continue;

            const wrap = document.createElement('div');
            wrap.className = 'filter-dropdown-wrap';

            const label = document.createElement('span');
            label.className = 'filter-dropdown-name';
            label.textContent = key;
            wrap.appendChild(label);

            const combobox = createCombobox({
                options: available.map(v => ({
                    value: v.value,
                    label: v.value,
                    count: v.count,
                })),
                placeholder: '— select —',
                onSelect: (value) => applyMetadataFilter(key, value),
            });

            wrap.appendChild(combobox);
            dom.metadataFilters.appendChild(wrap);
        }

        // Render the active tags inside the Metadata section itself
        clearTags(dom.metadataActiveFilters);
        for (const [attr, values] of Object.entries(activeMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(
                dom.metadataActiveFilters, attr, val, 'include',
                () => removeFilter(attr, val, 'include', 'metadata'),
            ));
        }
        for (const [attr, values] of Object.entries(activeNotMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(
                dom.metadataActiveFilters, attr, val, 'exclude',
                () => removeFilter(attr, val, 'exclude', 'metadata'),
            ));
        }

        updateClearAllBtn();

    } catch (err) {
        console.error('Failed to load metadata facets:', err);
        dom.metadataFilters.innerHTML = '<span class="empty-hint">Failed to load metadata filters</span>';
        showToast(`Failed to load metadata filters: ${err.message || err}`, 'error');
    }
}

async function loadReferenceFacets() {
    Skeleton.dropdowns(dom.referenceFilters, 3);
    try {
        const data = await api('/facets/references');
        const refFacets = data.references || {};
        const activeRfilters = data.activeFilters.rfilters || {};
        const activeNotRfilters = data.activeFilters.notRfilters || {};

        // Build a Set of already-applied entity IDs per (refColId, reason) — so the
        // same entity can't be picked twice in the same dropdown.
        const usedByGroup = {};
        const collectUsed = (source) => {
            for (const [refColId, reasonsMap] of Object.entries(source)) {
                for (const [reason, ids] of Object.entries(reasonsMap)) {
                    const idList = Array.isArray(ids) ? ids : Object.values(ids);
                    const compositeKey = `${refColId}:${reason}`;
                    if (!usedByGroup[compositeKey]) usedByGroup[compositeKey] = new Set();
                    idList.forEach(id => usedByGroup[compositeKey].add(String(id)));
                }
            }
        };
        collectUsed(activeRfilters);
        collectUsed(activeNotRfilters);

        // Render dropdowns
        dom.referenceFilters.innerHTML = '';
        if (Object.keys(refFacets).length === 0) {
            dom.referenceFilters.innerHTML = '<span class="empty-hint">No reference filters available</span>';
        } else {
            for (const [key, group] of Object.entries(refFacets)) {
                const used = usedByGroup[key] || new Set();
                const available = group.entities.filter(ent => !used.has(String(ent.id)));
                if (available.length === 0) continue;

                const wrap = document.createElement('div');
                wrap.className = 'filter-dropdown-wrap';

                const label = document.createElement('span');
                label.className = 'filter-dropdown-name';
                label.textContent = `${group.collectionName} → ${group.reason}`;
                wrap.appendChild(label);

                const combobox = createCombobox({
                    options: available.map(ent => ({
                        value: ent.id,
                        label: ent.name,
                        count: ent.count,
                    })),
                    placeholder: '— select —',
                    onSelect: (value) => applyReferenceFilter(
                        group.collectionId,
                        group.reason,
                        parseInt(value),
                    ),
                });

                wrap.appendChild(combobox);
                dom.referenceFilters.appendChild(wrap);
            }
        }

        // Lookups for human-readable tag labels
        const refLookup = {};
        const collLookup = {};
        for (const [key, group] of Object.entries(refFacets)) {
            collLookup[group.collectionId] = group.collectionName;
            if (!refLookup[key]) refLookup[key] = {};
            group.entities.forEach(ent => { refLookup[key][ent.id] = ent.name; });
        }

        // Render active tags inside the References section
        clearTags(dom.referenceActiveFilters);

        const renderRefTags = (source, type) => {
            for (const [refColId, reasonsMap] of Object.entries(source)) {
                for (const [reason, ids] of Object.entries(reasonsMap)) {
                    const idList = Array.isArray(ids) ? ids : Object.values(ids);
                    const compositeKey = `${refColId}:${reason}`;
                    idList.forEach(id => {
                        const colName = collLookup[refColId] || `Col #${refColId}`;
                        const displayLabel = `${colName} → ${reason}`;
                        const displayValue = refLookup[compositeKey]?.[id] ?? String(id);
                        addFilterTag(
                            dom.referenceActiveFilters, compositeKey, id, type,
                            () => removeFilter(compositeKey, id, type, 'reference'),
                            displayLabel, displayValue,
                        );
                    });
                }
            }
        };
        renderRefTags(activeRfilters, 'include');
        renderRefTags(activeNotRfilters, 'exclude');

        updateClearAllBtn();

    } catch (err) {
        console.error('Failed to load reference facets:', err);
        dom.referenceFilters.innerHTML = '<span class="empty-hint">Failed to load reference filters</span>';
        showToast(`Failed to load reference filters: ${err.message || err}`, 'error');
    }
}

async function loadLinkFacets() {
    Skeleton.pillRow(dom.links, 4);
    try {
        const data = await api('/facets/links');

        dom.links.innerHTML = '';
        if (data.links.length === 0) {
            dom.links.innerHTML = '<span class="empty-hint">No links available</span>';
        } else {
            data.links.forEach(link => {
                const btn = document.createElement('button');
                btn.textContent = `${link.collectionName} → ${link.reason}`;
                btn.onclick = () => navigateLink(link.collectionId, link.reason);
                dom.links.appendChild(btn);
            });
        }
    } catch (err) {
        console.error('Failed to load link facets:', err);
        dom.links.innerHTML = '<span class="empty-hint">Failed to load links</span>';
        showToast(`Failed to load links: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Filter & Link Actions
// ──────────────────────────────────────────────
async function applyMetadataFilter(attribute, value) {
    const action = state.notMode ? 'add_not_mfilter' : 'add_mfilter';
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action, attribute, value },
        });
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to apply filter:', err);
        showToast(`Failed to apply filter: ${err.message || err}`, 'error');
    }
}

async function applyReferenceFilter(collectionId, reason, entityId) {
    const action = state.notMode ? 'add_not_rfilter' : 'add_rfilter';
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action, collectionId, reason, entityId },
        });
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to apply reference filter:', err);
        showToast(`Failed to apply filter: ${err.message || err}`, 'error');
    }
}

async function removeFilter(name, value, type, category) {
    try {
        await removeFilterRaw(name, value, type, category);
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to remove filter:', err);
        showToast(`Failed to remove filter: ${err.message || err}`, 'error');
    }
}

// Same as removeFilter but without the refresh — used by clearAllFilters so
// we can drop N filters and refresh once at the end instead of N times.
async function removeFilterRaw(name, value, type, category) {
    if (category === 'metadata') {
        const action = type === 'include' ? 'rm_mfilter' : 'rm_not_mfilter';
        await api('/navigation', {
            method: 'POST',
            body: { action, attribute: name, value: String(value) },
        });
    } else {
        const splitIndex = name.indexOf(':');
        if (splitIndex === -1) {
            console.error('Invalid reference filter key:', name);
            return;
        }
        const collectionId = name.substring(0, splitIndex);
        const reason = name.substring(splitIndex + 1);
        const action = type === 'include' ? 'rm_rfilter' : 'rm_not_rfilter';
        await api('/navigation', {
            method: 'POST',
            body: { action, collectionId: parseInt(collectionId), reason, entityId: value },
        });
    }
}

// ──────────────────────────────────────────────
// Clear All filters
// ──────────────────────────────────────────────
async function clearAllFilters() {
    try {
        // Get current active filters from both endpoints
        const [mData, rData] = await Promise.all([
            api('/facets/metadata'),
            api('/facets/references'),
        ]);

        const removals = [];

        const collectMeta = (source, type) => {
            for (const [attr, vals] of Object.entries(source || {})) {
                const list = Array.isArray(vals) ? vals : Object.values(vals);
                list.forEach(v => removals.push({ name: attr, value: v, type, category: 'metadata' }));
            }
        };
        collectMeta(mData?.activeFilters?.mfilters, 'include');
        collectMeta(mData?.activeFilters?.notMfilters, 'exclude');

        const collectRef = (source, type) => {
            for (const [refColId, reasonsMap] of Object.entries(source || {})) {
                for (const [reason, ids] of Object.entries(reasonsMap)) {
                    const idList = Array.isArray(ids) ? ids : Object.values(ids);
                    idList.forEach(id => removals.push({
                        name: `${refColId}:${reason}`,
                        value: id,
                        type,
                        category: 'reference',
                    }));
                }
            }
        };
        collectRef(rData?.activeFilters?.rfilters, 'include');
        collectRef(rData?.activeFilters?.notRfilters, 'exclude');

        if (removals.length === 0) return;

        // Sequential to avoid racing on the backend's session-scoped state
        for (const r of removals) {
            await removeFilterRaw(r.name, r.value, r.type, r.category);
        }

        state.page = 0;
        await refreshAll();
        showToast(`Cleared ${removals.length} filter${removals.length === 1 ? '' : 's'}`, 'success', 2500);
    } catch (err) {
        console.error('Failed to clear all filters:', err);
        showToast(`Failed to clear filters: ${err.message || err}`, 'error');
    }
}

// Toggle the Clear All button's enabled state based on whether any tags are rendered
function updateClearAllBtn() {
    if (!dom.clearAllFiltersBtn) return;
    const total = dom.metadataActiveFilters.children.length
                + dom.referenceActiveFilters.children.length;
    dom.clearAllFiltersBtn.disabled = total === 0;
}

// Re-render the Include/Exclude toggle. When `previewOpposite` is true,
// it shows what the user's next click would set (used during hover).
function renderToggleBtn(previewOpposite = false) {
    const isExclude = previewOpposite ? !state.notMode : state.notMode;
    dom.toggleNotBtn.textContent = isExclude ? 'Exclude' : 'Include';
    dom.toggleNotBtn.className = `mode-toggle ${isExclude ? 'exclude' : 'include'}`;
}

async function navigateLink(collectionId, reason) {
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'link', collectionId, reason },
        });

        const col = state.collections.find(c => c.id === collectionId);
        const colName = col ? col.name : `Collection #${collectionId}`;

        state.collectionId = collectionId;
        state.collectionName = colName;
        state.linkHistory.push({ collectionId, collectionName: colName, reason });

        state.page = 0;
        renderBreadcrumb();
        renderLinkHistory();
        updateNavButtons();
        await refreshAll();
    } catch (err) {
        console.error('Failed to navigate link:', err);
        showToast(`Failed to follow link: ${err.message || err}`, 'error');
    }
}

async function goBackLink() {
    if (state.linkHistory.length <= 1) return;
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'goback' },
        });

        state.linkHistory.pop();
        const top = state.linkHistory[state.linkHistory.length - 1];
        state.collectionId = top.collectionId;
        state.collectionName = top.collectionName;

        state.page = 0;
        renderBreadcrumb();
        renderLinkHistory();
        updateNavButtons();
        await refreshAll();
    } catch (err) {
        console.error('Failed to go back:', err);
        showToast(`Failed to go back: ${err.message || err}`, 'error');
    }
}

async function restoreState() {
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'restore' },
        });
        state.page = 0;
        await refreshAll();
        showToast('State restored', 'success', 2500);
    } catch (err) {
        console.error('Failed to restore:', err);
        showToast(`Failed to restore: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Entity Detail Modal Controller
// ──────────────────────────────────────────────
async function viewEntity(entityId, collectionId) {
    try {
        let url = `/entity?id=${entityId}`;
        if (collectionId !== undefined && collectionId !== null) {
            url += `&collectionId=${collectionId}`;
        }
        const data = await api(url);

        renderModalContent(data, dom.modalRefs);
    } catch (err) {
        console.error('Failed to load entity details:', err);
        showToast(`Failed to load entity details: ${err.message || err}`, 'error');
    }
}

// ──────────────────────────────────────────────
// Listeners Placeholder (Assumed to exist in your original)
// ──────────────────────────────────────────────
function setupListeners() {
    // Reattach listeners to static buttons like exitNavBtn, prevPage, etc.
    if (dom.exitNavBtn)        dom.exitNavBtn.onclick        = exitNavigation;
    if (dom.gobackBtn)         dom.gobackBtn.onclick         = goBackLink;
    if (dom.restoreBtn)        dom.restoreBtn.onclick        = restoreState;
    if (dom.unionBtn)          dom.unionBtn.onclick          = startUnion;
    if (dom.changeBtn)         dom.changeBtn.onclick         = startChange;
    if (dom.historyToggleBtn)  dom.historyToggleBtn.onclick  = toggleHistoryView;
    if (dom.clearAllFiltersBtn) dom.clearAllFiltersBtn.onclick = clearAllFilters;

    // Include/Exclude toggle: hover previews the opposite, click commits the swap.
    if (dom.toggleNotBtn) {
        dom.toggleNotBtn.onclick = () => {
            state.notMode = !state.notMode;
            // Mouse is still hovering after click → keep showing what *next* click would do.
            renderToggleBtn(true);
        };
        dom.toggleNotBtn.addEventListener('mouseenter', () => renderToggleBtn(true));
        dom.toggleNotBtn.addEventListener('mouseleave', () => renderToggleBtn(false));
    }
}

// ──────────────────────────────────────────────
// Boot
// ──────────────────────────────────────────────
init();