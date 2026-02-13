// ──────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────
const API_BASE = 'http://localhost:8080/backend/api';

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
const $$ = (id) => document.getElementById(id);

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
    prevPage: $$('prev-page'),
    nextPage: $$('next-page'),
    pageInfo: $$('page-info'),

    unionEntities: $$('union-entities'),
    unionCount: $$('union-count'),
    unionPrevPage: $$('union-prev-page'),
    unionNextPage: $$('union-next-page'),
    unionPageInfo: $$('union-page-info'),

    unionBtn: $$('union-btn'),

    // Modal
    modal: $$('entity-modal'),
    modalClose: $$('modal-close'),
    modalName: $$('modal-entity-name'),
    modalContents: $$('modal-entity-contents'),
    modalMetadata: $$('modal-entity-metadata'),
    modalReferences: $$('modal-entity-references'),
};

// ──────────────────────────────────────────────
// API helper
// ──────────────────────────────────────────────
async function api(endpoint, options = {}) {
    const url = `${API_BASE}${endpoint}`;
    const config = {
        mode: 'cors',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        ...options,
    };
    if (options.body && typeof options.body === 'object') {
        config.body = JSON.stringify(options.body);
    }
    const res = await fetch(url, config);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
}

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

// Enter navigation mode (also used after union → pick new collection)
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

// ──────────────────────────────────────────────
// Exit navigation mode
// ──────────────────────────────────────────────
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
// Union flow
// ──────────────────────────────────────────────
function startUnion() {
    // Show collection picker (reuse Screen 2) but when a collection is picked
    // we do "union" action instead of creating a new session
    dom.collections.innerHTML = '';
    state.collections.forEach(col => {
        const btn = document.createElement('button');
        btn.textContent = col.name;
        btn.onclick = () => enterNavigationForUnion(col.id, col.name);
        dom.collections.appendChild(btn);
    });

    showScreen('collections');
}

// ──────────────────────────────────────────────
// Refresh everything in navigation mode
// ──────────────────────────────────────────────
async function refreshAll() {
    await Promise.all([
        loadEntities(),
        loadFacets(),
        loadUnion(),
    ]);
}

// ──────────────────────────────────────────────
// Entities (filtered collection)
// ──────────────────────────────────────────────
async function loadEntities() {
    try {
        const data = await api(`/entities?page=${state.page}&size=${state.pageSize}`);

        dom.entities.innerHTML = '';
        if (data.entities.length === 0) {
            dom.entities.innerHTML = '<p class="empty-state">No entities found</p>';
        } else {
            data.entities.forEach(ent => {
                const card = document.createElement('div');
                card.className = 'entity-card';
                card.textContent = ent.name;
                card.onclick = () => viewEntity(ent.id);
                dom.entities.appendChild(card);
            });
        }

        // Update collection name in case it changed (via link)
        const col = state.collections.find(c => c.id === data.collectionId);
        if (col) {
            state.collectionName = col.name;
            dom.currentColLabel.textContent = `(${col.name})`;
            dom.navTitle.textContent = `Navigating — ${col.name}`;
        }

        dom.entitiesCount.textContent = `${data.total} entities`;
        updatePagination(data.total, state.page, state.pageSize, dom.prevPage, dom.nextPage, dom.pageInfo);
    } catch (err) {
        console.error('Failed to load entities:', err);
    }
}

// ──────────────────────────────────────────────
// Union entities
// ──────────────────────────────────────────────
async function loadUnion() {
    try {
        const data = await api(`/union?page=${state.unionPage}&size=${state.unionPageSize}`);

        dom.unionEntities.innerHTML = '';

        if (data.entities.length === 0) {
            dom.unionEntities.innerHTML = '<p class="empty-state">No union entries</p>';
        } else {
            // Data is already paginated and flattened by backend
            data.entities.forEach(ent => {
                const card = document.createElement('div');
                card.className = 'entity-card';
                card.textContent = ent.name;
                card.onclick = () => viewEntity(ent.id, ent.collection_id);
                dom.unionEntities.appendChild(card);
            });
        }

        const totalUnion = data.total;
        dom.unionCount.textContent = totalUnion > 0 ? `${totalUnion} entities` : '';
        updatePagination(totalUnion, state.unionPage, state.unionPageSize, dom.unionPrevPage, dom.unionNextPage, dom.unionPageInfo);
    } catch (err) {
        console.error('Failed to load union:', err);
    }
}

// ──────────────────────────────────────────────
// Shared pagination helper
// ──────────────────────────────────────────────
function updatePagination(total, page, pageSize, prevBtn, nextBtn, infoEl) {
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    infoEl.textContent = `Page ${page + 1} of ${totalPages}`;
    prevBtn.disabled = page === 0;
    nextBtn.disabled = page >= totalPages - 1;
}

// ──────────────────────────────────────────────
// Facets (filters, active filters, links)
// ──────────────────────────────────────────────
async function loadFacets() {
    try {
        const data = await api('/facets');

        // ── Metadata dropdowns ──
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

        // ── Reference filters (dropdowns from reference facets) ──
        dom.referenceFilters.innerHTML = '';
        const activeRfilters = data.activeFilters.rfilters || {};
        const activeNotRfilters = data.activeFilters.notRfilters || {};
        const refFacets = data.references || {};
        console.log('Reference facets data:', JSON.stringify(refFacets));

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

        // ── Helper: Reference Lookup Map ──
        // (collId:reason -> entityId -> entityName)
        const refLookup = {};
        // Also map (collId -> collName) just in case
        const collLookup = {};

        if (refFacets) {
            for (const [key, group] of Object.entries(refFacets)) {
                // key is "collId:reason"
                // group has { collectionId, collectionName, reason, entities: [...] }
                collLookup[group.collectionId] = group.collectionName;

                if (!refLookup[key]) refLookup[key] = {};
                group.entities.forEach(ent => {
                    refLookup[key][ent.id] = ent.name;
                });
            }
        }

        // ── Active filter tags ──
        dom.activeFilters.innerHTML = '';

        // Metadata include filters
        const activeMfilters = data.activeFilters.mfilters || {};
        for (const [attr, values] of Object.entries(activeMfilters)) {
            if (Array.isArray(values)) {
                values.forEach(val => addFilterTag(attr, val, 'include', 'metadata'));
            } else if (values instanceof Object) {
                // It can come as Set serialized to array or object
                Object.values(values).forEach(val => addFilterTag(attr, val, 'include', 'metadata'));
            }
        }

        // Metadata exclude filters
        const activeNotMfilters = data.activeFilters.notMfilters || {};
        for (const [attr, values] of Object.entries(activeNotMfilters)) {
            if (Array.isArray(values)) {
                values.forEach(val => addFilterTag(attr, val, 'exclude', 'metadata'));
            } else if (values instanceof Object) {
                Object.values(values).forEach(val => addFilterTag(attr, val, 'exclude', 'metadata'));
            }
        }

        // Reference include filters
        for (const [refColId, reasonsMap] of Object.entries(activeRfilters)) {
            for (const [reason, ids] of Object.entries(reasonsMap)) {
                const idList = Array.isArray(ids) ? ids : Object.values(ids);
                const compositeKey = `${refColId}:${reason}`;

                // Try to resolve names
                // collection name is tricky if we only have ID, but we might have it in collLookup
                // reason is known
                // entity name from refLookup

                idList.forEach(id => {
                    let displayLabel = compositeKey;
                    let displayValue = String(id);

                    // Resolve Collection Name
                    const colName = collLookup[refColId] || `Col #${refColId}`;
                    displayLabel = `${colName} → ${reason}`;

                    // Resolve Entity Name
                    if (refLookup[compositeKey] && refLookup[compositeKey][id]) {
                        displayValue = refLookup[compositeKey][id];
                    }

                    addFilterTag(compositeKey, id, 'include', 'reference', displayLabel, displayValue);
                });
            }
        }

        // Reference exclude filters
        for (const [refColId, reasonsMap] of Object.entries(activeNotRfilters)) {
            for (const [reason, ids] of Object.entries(reasonsMap)) {
                const idList = Array.isArray(ids) ? ids : Object.values(ids);
                const compositeKey = `${refColId}:${reason}`;

                idList.forEach(id => {
                    let displayLabel = compositeKey;
                    let displayValue = String(id);

                    // Resolve Collection Name
                    const colName = collLookup[refColId] || `Col #${refColId}`;
                    displayLabel = `${colName} → ${reason}`;

                    // Resolve Entity Name
                    if (refLookup[compositeKey] && refLookup[compositeKey][id]) {
                        displayValue = refLookup[compositeKey][id];
                    }

                    addFilterTag(compositeKey, id, 'exclude', 'reference', displayLabel, displayValue);
                });
            }
        }

        if (dom.activeFilters.children.length === 0) {
            dom.activeFilters.innerHTML = '<span class="empty-hint">No active filters</span>';
        }

        // ── Links ──
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

        // Show/hide goback button (only when there's an active link)
        dom.gobackBtn.classList.toggle('hidden', !data.activeFilters.activeLink);
        // Restore is always visible in nav mode (backend handles empty stack)
        dom.restoreBtn.classList.remove('hidden');

    } catch (err) {
        console.error('Failed to load facets:', err);
    }
}

// ──────────────────────────────────────────────
// Active filter tag builder
// ──────────────────────────────────────────────
function addFilterTag(name, value, type, category, displayLabel, displayValue) {
    const tag = document.createElement('span');
    tag.className = `filter-tag ${type === 'include' ? 'include-tag' : 'exclude-tag'}`;

    // Use provided display labels or fallback to name/value
    const labelText = displayLabel !== undefined ? displayLabel : name;
    const valueText = displayValue !== undefined ? displayValue : value;

    tag.innerHTML = `
        <span class="tag-type">${type === 'include' ? 'INC' : 'EXC'}</span>
        <span class="tag-label">${escHtml(String(labelText))}:</span>
        <span class="tag-value">${escHtml(String(valueText))}</span>
    `;

    const removeBtn = document.createElement('button');
    removeBtn.className = 'tag-remove';
    removeBtn.textContent = '×';
    removeBtn.onclick = () => removeFilter(name, value, type, category);
    tag.appendChild(removeBtn);

    dom.activeFilters.appendChild(tag);
}

function escHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ──────────────────────────────────────────────
// Filter actions
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
            // Reference filter: name is "collectionId:reason"
            // Handle reason possibly containing colons by finding first colon index
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

// ──────────────────────────────────────────────
// Link navigation
// ──────────────────────────────────────────────
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
// Entity detail modal
// ──────────────────────────────────────────────
async function viewEntity(entityId, collectionId) {
    try {
        let url = `/entity?id=${entityId}`;
        if (collectionId !== undefined && collectionId !== null) {
            url += `&collectionId=${collectionId}`;
        }
        const data = await api(url);

        dom.modalName.textContent = data.name || `Entity #${entityId}`;

        // Contents (displayed as key-value table)
        dom.modalContents.innerHTML = '';
        if (data.contents) {
            let parsed;
            try {
                parsed = typeof data.contents === 'string' ? JSON.parse(data.contents) : data.contents;
            } catch (_) {
                parsed = null;
            }

            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                const table = document.createElement('table');
                table.className = 'modal-meta-table';
                for (const [key, value] of Object.entries(parsed)) {
                    const row = table.insertRow();
                    const keyCell = row.insertCell();
                    keyCell.textContent = key;
                    const valCell = row.insertCell();
                    valCell.textContent = typeof value === 'object' ? JSON.stringify(value) : String(value);
                }
                dom.modalContents.appendChild(table);
            } else {
                dom.modalContents.textContent = typeof data.contents === 'string' ? data.contents : JSON.stringify(data.contents);
            }
            dom.modalContents.classList.remove('hidden');
        } else {
            dom.modalContents.classList.add('hidden');
        }

        // Metadata
        dom.modalMetadata.innerHTML = '';
        if (data.metadata && Object.keys(data.metadata).length > 0) {
            const title = document.createElement('h3');
            title.className = 'modal-section-title';
            title.textContent = 'Metadata';
            dom.modalMetadata.appendChild(title);

            const table = document.createElement('table');
            table.className = 'modal-meta-table';
            for (const [key, values] of Object.entries(data.metadata)) {
                const row = table.insertRow();
                const keyCell = row.insertCell();
                keyCell.textContent = key;
                const valCell = row.insertCell();
                valCell.textContent = Array.isArray(values) ? values.join(', ') : String(values);
            }
            dom.modalMetadata.appendChild(table);
        }

        // References
        dom.modalReferences.innerHTML = '';
        if (data.references && Object.keys(data.references).length > 0) {
            const title = document.createElement('h3');
            title.className = 'modal-section-title';
            title.textContent = 'References';
            dom.modalReferences.appendChild(title);

            for (const [reason, refs] of Object.entries(data.references)) {
                const group = document.createElement('div');
                group.className = 'modal-ref-group';

                const reasonEl = document.createElement('div');
                reasonEl.className = 'modal-ref-reason';
                reasonEl.textContent = reason;
                group.appendChild(reasonEl);

                const list = document.createElement('div');
                list.className = 'modal-ref-list';
                refs.forEach(ref => {
                    const chip = document.createElement('span');
                    chip.className = 'modal-ref-chip';
                    chip.textContent = ref.name || `#${ref.id}`;
                    list.appendChild(chip);
                });
                group.appendChild(list);

                dom.modalReferences.appendChild(group);
            }
        }

        dom.modal.classList.remove('hidden');
    } catch (err) {
        console.error('Failed to load entity details:', err);
    }
}

function closeModal() {
    dom.modal.classList.add('hidden');
}

// ──────────────────────────────────────────────
// Event listeners
// ──────────────────────────────────────────────
function setupListeners() {
    // Exit navigation
    dom.exitNavBtn.onclick = exitNavigation;

    // Toggle include/exclude
    dom.toggleNotBtn.onclick = () => {
        state.notMode = !state.notMode;
        dom.toggleNotBtn.textContent = state.notMode ? 'Exclude' : 'Include';
        dom.toggleNotBtn.className = `mode-toggle ${state.notMode ? 'exclude' : 'include'}`;
    };

    // Goback & Restore
    dom.gobackBtn.onclick = goBackLink;
    dom.restoreBtn.onclick = restoreState;

    // Union
    dom.unionBtn.onclick = startUnion;

    // Modal
    dom.modalClose.onclick = closeModal;
    dom.modal.onclick = (e) => {
        if (e.target === dom.modal) closeModal();
    };

    // Pagination – filtered entities
    dom.prevPage.onclick = () => {
        if (state.page > 0) {
            state.page--;
            loadEntities();
        }
    };
    dom.nextPage.onclick = () => {
        state.page++;
        loadEntities();
    };

    // Pagination – union entities
    dom.unionPrevPage.onclick = () => {
        if (state.unionPage > 0) {
            state.unionPage--;
            loadUnion();
        }
    };
    dom.unionNextPage.onclick = () => {
        state.unionPage++;
        loadUnion();
    };
}

// ──────────────────────────────────────────────
// Start
// ──────────────────────────────────────────────
init();
