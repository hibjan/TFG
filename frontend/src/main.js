import { $$ } from './utils.js';
import { api } from './api.js';
import PaginationHandler from './components/PaginationHandler.js';
import { addFilterTag, clearFilterTagsByCategory, refreshEmptyHint } from './components/FilterTags.js';
import { renderModalContent } from './components/Modal.js';
import { initPanelDragOrder } from './components/PanelDragOrder.js';
import './styles/main.css';

// ──────────────────────────────────────────────
// State
// ──────────────────────────────────────────────
const state = {
    datasetId: null,
    datasetName: '',
    collectionId: null,
    collectionName: '',
    collections: [],     // all collections for current dataset
    notMode: false,      // false = include, true = exclude
    page: 0,
    pageSize: 20,
    unionPage: 0,
    unionPageSize: 20,
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
    navTitle: $$('nav-title'),

    toggleNotBtn: $$('toggle-not-btn'),
    metadataFilters: $$('metadata-filters'),
    referenceFilters: $$('reference-filters'),
    activeFilters: $$('active-filters'),

    links: $$('links'),
    gobackBtn: $$('goback-btn'),
    restoreBtn: $$('restore-btn'),

    entities: $$('entities'),
    entitiesCount: $$('entities-count'),
    currentColLabel: $$('current-collection-label'),
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

            // Load collections so we can resolve names
            const colData = await api(`/collections?datasetId=${session.datasetId}`);
            state.collections = colData.collections;

            const col = state.collections.find(c => c.id === session.collectionId);
            state.collectionName = col ? col.name : '';

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
    try {
        const data = await api('/datasets');
        dom.datasets.innerHTML = '';
        data.datasets.forEach(ds => {
            const btn = document.createElement('button');
            btn.textContent = ds.name;
            btn.className = 'large-selection-btn';
            btn.onclick = () => selectDataset(ds.id, ds.name);
            dom.datasets.appendChild(btn);
        });
    } catch (err) {
        console.error('Failed to load datasets:', err);
        dom.datasets.innerHTML = '<p class="empty-state">Failed to load datasets</p>';
    }
}

async function selectDataset(id, name) {
    state.datasetId = id;
    state.datasetName = name;

    try {
        const data = await api(`/collections?datasetId=${id}`);
        state.collections = data.collections;

        dom.collections.innerHTML = '';
        data.collections.forEach(col => {
            const btn = document.createElement('button');
            btn.textContent = col.name;
            btn.onclick = () => selectCollection(col.id, col.name);
            dom.collections.appendChild(btn);
        });

        showScreen('collections');
    } catch (err) {
        console.error('Failed to load collections:', err);
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

    try {
        // Create a new session
        await api('/session', {
            method: 'POST',
            body: { datasetId: state.datasetId, collectionId: id },
        });

        enterNavigation();
    } catch (err) {
        console.error('Failed to init session:', err);
    }
}

async function enterNavigationForUnion(id, name) {
    state.collectionId = id;
    state.collectionName = name;
    state.page = 0;
    state.unionPage = 0;

    try {
        // Perform union action (saves current filters + switches collection)
        await api('/navigation', {
            method: 'POST',
            body: { action: 'union', collectionId: id },
        });

        enterNavigation();
    } catch (err) {
        console.error('Failed to perform union:', err);
    }
}

function enterNavigation() {
    dom.navTitle.textContent = `Navigating — ${state.collectionName}`;
    dom.currentColLabel.textContent = `(${state.collectionName})`;
    showScreen('navigation');
    refreshAll();
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

    // Reset toggle UI
    dom.toggleNotBtn.textContent = 'Include';
    dom.toggleNotBtn.className = 'mode-toggle include';

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

    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'change', collectionId: id },
        });

        enterNavigation();
    } catch (err) {
        console.error('Failed to change collection:', err);
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
                card.onclick = () => viewEntity(ent.id);
                dom.entities.appendChild(card);
            });
        }

        const col = state.collections.find(c => c.id === data.collectionId);
        if (col) {
            state.collectionName = col.name;
            dom.currentColLabel.textContent = `(${col.name})`;
            dom.navTitle.textContent = `Navigating — ${col.name}`;
        }

        dom.entitiesCount.textContent = `${totalEntities} entities`;
        
        // Render Pagination safely
        const totalPages = Math.max(1, Math.ceil(totalEntities / state.pageSize));
        entitiesPagination.render(state.page + 1, totalPages);
        
    } catch (err) {
        console.error('Failed to load entities:', err);
    }
}

async function loadUnion() {
    try {
        const data = await api(`/union?page=${state.unionPage}&size=${state.unionPageSize}`);

        // --- SAFE FALLBACKS ---
        const unionList = data.entities || [];
        const totalUnion = data.total || 0;

        dom.unionEntities.innerHTML = '';

        if (unionList.length === 0) {
            dom.unionEntities.innerHTML = '<p class="empty-state">No union entries</p>';
        } else {
            unionList.forEach(ent => {
                const card = document.createElement('div');
                card.className = 'entity-card';
                card.textContent = ent.name;
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
    }
}

// ──────────────────────────────────────────────
// Facet Loaders
// ──────────────────────────────────────────────
async function loadMetadataFacets() {
    try {
        const data = await api('/facets/metadata');

        dom.metadataFilters.innerHTML = '';
        for (const [key, values] of Object.entries(data.metadata)) {
            const wrap = document.createElement('div');
            wrap.className = 'filter-dropdown-wrap';

            const label = document.createElement('span');
            label.className = 'filter-dropdown-name';
            label.textContent = key;
            wrap.appendChild(label);

            const select = document.createElement('select');
            const defaultOpt = document.createElement('option');
            defaultOpt.value = '';
            defaultOpt.textContent = '— select —';
            select.appendChild(defaultOpt);

            values.forEach(v => {
                const opt = document.createElement('option');
                opt.value = v.value;
                opt.textContent = `${v.value} (${v.count})`;
                select.appendChild(opt);
            });

            select.onchange = () => {
                if (select.value) applyMetadataFilter(key, select.value);
            };

            wrap.appendChild(select);
            dom.metadataFilters.appendChild(wrap);
        }

        clearFilterTagsByCategory(dom.activeFilters, 'metadata');

        const activeMfilters = data.activeFilters.mfilters || {};
        for (const [attr, values] of Object.entries(activeMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(dom.activeFilters, attr, val, 'include', 'metadata', removeFilter));
        }

        const activeNotMfilters = data.activeFilters.notMfilters || {};
        for (const [attr, values] of Object.entries(activeNotMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(dom.activeFilters, attr, val, 'exclude', 'metadata', removeFilter));
        }

        refreshEmptyHint(dom.activeFilters);

        dom.gobackBtn.classList.remove('hidden');
        dom.restoreBtn.classList.remove('hidden');

    } catch (err) {
        console.error('Failed to load metadata facets:', err);
    }
}

async function loadReferenceFacets() {
    try {
        const data = await api('/facets/references');
        const refFacets = data.references || {};
        const activeRfilters = data.activeFilters.rfilters || {};
        const activeNotRfilters = data.activeFilters.notRfilters || {};

        dom.referenceFilters.innerHTML = '';
        if (Object.keys(refFacets).length === 0) {
            dom.referenceFilters.innerHTML = '<span class="empty-hint">No reference filters available</span>';
        } else {
            for (const [key, group] of Object.entries(refFacets)) {
                const wrap = document.createElement('div');
                wrap.className = 'filter-dropdown-wrap';

                const label = document.createElement('span');
                label.className = 'filter-dropdown-name';
                label.textContent = `${group.collectionName} → ${group.reason}`;
                wrap.appendChild(label);

                const select = document.createElement('select');
                const defaultOpt = document.createElement('option');
                defaultOpt.value = '';
                defaultOpt.textContent = '— select —';
                select.appendChild(defaultOpt);

                group.entities.forEach(ent => {
                    const opt = document.createElement('option');
                    opt.value = ent.id;
                    opt.textContent = `${ent.name} (${ent.count})`;
                    select.appendChild(opt);
                });

                select.onchange = () => {
                    if (select.value) {
                        applyReferenceFilter(group.collectionId, group.reason, parseInt(select.value));
                    }
                };

                wrap.appendChild(select);
                dom.referenceFilters.appendChild(wrap);
            }
        }

        const refLookup = {};
        const collLookup = {};
        for (const [key, group] of Object.entries(refFacets)) {
            collLookup[group.collectionId] = group.collectionName;
            if (!refLookup[key]) refLookup[key] = {};
            group.entities.forEach(ent => { refLookup[key][ent.id] = ent.name; });
        }

        clearFilterTagsByCategory(dom.activeFilters, 'reference');

        for (const [refColId, reasonsMap] of Object.entries(activeRfilters)) {
            for (const [reason, ids] of Object.entries(reasonsMap)) {
                const idList = Array.isArray(ids) ? ids : Object.values(ids);
                const compositeKey = `${refColId}:${reason}`;
                idList.forEach(id => {
                    const colName = collLookup[refColId] || `Col #${refColId}`;
                    const displayLabel = `${colName} → ${reason}`;
                    const displayValue = refLookup[compositeKey]?.[id] ?? String(id);
                    addFilterTag(dom.activeFilters, compositeKey, id, 'include', 'reference', removeFilter, displayLabel, displayValue);
                });
            }
        }

        for (const [refColId, reasonsMap] of Object.entries(activeNotRfilters)) {
            for (const [reason, ids] of Object.entries(reasonsMap)) {
                const idList = Array.isArray(ids) ? ids : Object.values(ids);
                const compositeKey = `${refColId}:${reason}`;
                idList.forEach(id => {
                    const colName = collLookup[refColId] || `Col #${refColId}`;
                    const displayLabel = `${colName} → ${reason}`;
                    const displayValue = refLookup[compositeKey]?.[id] ?? String(id);
                    addFilterTag(dom.activeFilters, compositeKey, id, 'exclude', 'reference', removeFilter, displayLabel, displayValue);
                });
            }
        }

        refreshEmptyHint(dom.activeFilters);

    } catch (err) {
        console.error('Failed to load reference facets:', err);
    }
}

async function loadLinkFacets() {
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
    }
}

async function removeFilter(name, value, type, category) {
    try {
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
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to remove filter:', err);
    }
}

async function navigateLink(collectionId, reason) {
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'link', collectionId, reason },
        });

        const col = state.collections.find(c => c.id === collectionId);
        if (col) {
            state.collectionId = collectionId;
            state.collectionName = col.name;
        }
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to navigate link:', err);
    }
}

async function goBackLink() {
    try {
        await api('/navigation', {
            method: 'POST',
            body: { action: 'goback' },
        });
        state.page = 0;
        await refreshAll();
    } catch (err) {
        console.error('Failed to go back:', err);
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
    } catch (err) {
        console.error('Failed to restore:', err);
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
    }
}

// ──────────────────────────────────────────────
// Listeners Placeholder (Assumed to exist in your original)
// ──────────────────────────────────────────────
function setupListeners() {
    // Reattach listeners to static buttons like exitNavBtn, prevPage, etc.
    if(dom.exitNavBtn) dom.exitNavBtn.onclick = exitNavigation;
    if(dom.gobackBtn) dom.gobackBtn.onclick = goBackLink;
    if(dom.restoreBtn) dom.restoreBtn.onclick = restoreState;
    if(dom.unionBtn) dom.unionBtn.onclick = startUnion;
    if(dom.changeBtn) dom.changeBtn.onclick = startChange;
    // etc... 
}

// ──────────────────────────────────────────────
// Boot
// ──────────────────────────────────────────────
init();