// ──────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────
const API_BASE = `${import.meta.env.VITE_API_BASE_URL}/backend/api`;

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
    changeBtn: $$('change-btn'),

    // Modal
    modal: $$('entity-modal'),
    modalClose: $$('modal-close'),
    modalName: $$('modal-entity-name'),
    modalContents: $$('modal-entity-contents'),
    modalResources: $$('modal-entity-resources'),
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
// Change flow (switch collection, reset state)
// ──────────────────────────────────────────────
function startChange() {
    // Show collection picker — when a collection is picked
    // we do "change" action (resets filters/links/history)
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
// Refresh everything in navigation mode
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
// Facets — three independent loaders
// ──────────────────────────────────────────────

// Clears all existing filter tags of a given category ('metadata' or 'reference')
// so each loader can repopulate only its own slice of the active-filters bar.
function clearFilterTagsByCategory(category) {
    Array.from(dom.activeFilters.querySelectorAll(`.filter-tag[data-category="${category}"]`))
        .forEach(el => el.remove());
    // Remove the "no active filters" hint if tags are present
    const hint = dom.activeFilters.querySelector('.empty-hint');
    if (hint) hint.remove();
}

function refreshEmptyHint() {
    if (dom.activeFilters.children.length === 0) {
        dom.activeFilters.innerHTML = '<span class="empty-hint">No active filters</span>';
    }
}

// ── Metadata facets ──────────────────────────────
async function loadMetadataFacets() {
    try {
        const data = await api('/facets/metadata');

        // Metadata dropdowns
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

        // Active metadata filter tags
        clearFilterTagsByCategory('metadata');

        const activeMfilters = data.activeFilters.mfilters || {};
        for (const [attr, values] of Object.entries(activeMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(attr, val, 'include', 'metadata'));
        }

        const activeNotMfilters = data.activeFilters.notMfilters || {};
        for (const [attr, values] of Object.entries(activeNotMfilters)) {
            const list = Array.isArray(values) ? values : Object.values(values);
            list.forEach(val => addFilterTag(attr, val, 'exclude', 'metadata'));
        }

        refreshEmptyHint();

        // Goback and Restore are always visible in nav mode
        dom.gobackBtn.classList.remove('hidden');
        dom.restoreBtn.classList.remove('hidden');

    } catch (err) {
        console.error('Failed to load metadata facets:', err);
    }
}

// ── Reference facets ─────────────────────────────
async function loadReferenceFacets() {
    try {
        const data = await api('/facets/references');
        const refFacets = data.references || {};
        const activeRfilters = data.activeFilters.rfilters || {};
        const activeNotRfilters = data.activeFilters.notRfilters || {};

        console.log('Reference facets data:', JSON.stringify(refFacets));

        // Reference filter dropdowns
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

        // Build lookup maps for display name resolution
        const refLookup = {};
        const collLookup = {};
        for (const [key, group] of Object.entries(refFacets)) {
            collLookup[group.collectionId] = group.collectionName;
            if (!refLookup[key]) refLookup[key] = {};
            group.entities.forEach(ent => { refLookup[key][ent.id] = ent.name; });
        }

        // Active reference filter tags
        clearFilterTagsByCategory('reference');

        for (const [refColId, reasonsMap] of Object.entries(activeRfilters)) {
            for (const [reason, ids] of Object.entries(reasonsMap)) {
                const idList = Array.isArray(ids) ? ids : Object.values(ids);
                const compositeKey = `${refColId}:${reason}`;
                idList.forEach(id => {
                    const colName = collLookup[refColId] || `Col #${refColId}`;
                    const displayLabel = `${colName} → ${reason}`;
                    const displayValue = refLookup[compositeKey]?.[id] ?? String(id);
                    addFilterTag(compositeKey, id, 'include', 'reference', displayLabel, displayValue);
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
                    addFilterTag(compositeKey, id, 'exclude', 'reference', displayLabel, displayValue);
                });
            }
        }

        refreshEmptyHint();

    } catch (err) {
        console.error('Failed to load reference facets:', err);
    }
}

// ── Link facets ───────────────────────────────────
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
// Active filter tag builder
// ──────────────────────────────────────────────
function addFilterTag(name, value, type, category, displayLabel, displayValue) {
    const tag = document.createElement('span');
    tag.className = `filter-tag ${type === 'include' ? 'include-tag' : 'exclude-tag'}`;
    tag.dataset.category = category;

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
                    if (key === '_resources') continue;
                    const row = table.insertRow();
                    const keyCell = row.insertCell();
                    keyCell.textContent = key;
                    const valCell = row.insertCell();
                    valCell.textContent = typeof value === 'object' ? JSON.stringify(value) : String(value);
                }
                dom.modalContents.appendChild(table);

                // Render resources
                dom.modalResources.innerHTML = '';
                const resources = parsed._resources;
                if (Array.isArray(resources) && resources.length > 0) {
                    // Resolve relative URLs against the backend host
                    const backendOrigin = API_BASE.replace(/\/api$/, '');
                    const resolveUrl = (url) => url.startsWith('/') ? backendOrigin + url : url;

                    const images = resources.filter(r => r.type === 'image');
                    const videos = resources.filter(r => r.type === 'video');
                    const pdfs = resources.filter(r => r.type === 'pdf');
                    const links = resources.filter(r => r.type === 'link');

                    if (images.length > 0) {
                        const imagesContainer = document.createElement('div');
                        imagesContainer.className = 'modal-resource-images';
                        images.forEach(res => {
                            const figure = document.createElement('figure');
                            figure.className = 'modal-resource-figure';
                            const a = document.createElement('a');
                            a.href = resolveUrl(res.url);
                            a.target = '_blank';
                            a.rel = 'noopener noreferrer';
                            const img = document.createElement('img');
                            img.className = 'modal-resource-image';
                            img.src = resolveUrl(res.url);
                            img.alt = res.label;
                            img.loading = 'lazy';
                            a.appendChild(img);
                            figure.appendChild(a);
                            const caption = document.createElement('figcaption');
                            caption.textContent = res.label;
                            figure.appendChild(caption);
                            imagesContainer.appendChild(figure);
                        });
                        dom.modalResources.appendChild(imagesContainer);
                    }

                    if (videos.length > 0) {
                        const videosContainer = document.createElement('div');
                        videosContainer.className = 'modal-resource-videos';
                        videos.forEach(res => {
                            const figure = document.createElement('figure');
                            figure.className = 'modal-resource-figure';
                            const video = document.createElement('video');
                            video.className = 'modal-resource-video';
                            video.src = resolveUrl(res.url);
                            video.controls = true;
                            video.style.maxWidth = '100%';
                            figure.appendChild(video);
                            if (res.label) {
                                const caption = document.createElement('figcaption');
                                caption.textContent = res.label;
                                figure.appendChild(caption);
                            }
                            videosContainer.appendChild(figure);
                        });
                        dom.modalResources.appendChild(videosContainer);
                    }

                    if (pdfs.length > 0) {
                        const pdfsContainer = document.createElement('div');
                        pdfsContainer.className = 'modal-resource-pdfs';
                        pdfs.forEach(res => {
                            const a = document.createElement('a');
                            a.className = 'modal-resource-link modal-resource-pdf';
                            a.href = resolveUrl(res.url);
                            a.target = '_blank';
                            a.rel = 'noopener noreferrer';
                            a.textContent = `📄 ${res.label || 'PDF Document'}`;
                            pdfsContainer.appendChild(a);
                        });
                        dom.modalResources.appendChild(pdfsContainer);
                    }

                    if (links.length > 0) {
                        const linksContainer = document.createElement('div');
                        linksContainer.className = 'modal-resource-links';
                        links.forEach(res => {
                            const a = document.createElement('a');
                            a.className = 'modal-resource-link';
                            a.href = resolveUrl(res.url);
                            a.target = '_blank';
                            a.rel = 'noopener noreferrer';
                            a.textContent = `🔗 ${res.label}`;
                            linksContainer.appendChild(a);
                        });
                        dom.modalResources.appendChild(linksContainer);
                    }
                }
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

    // Change collection
    dom.changeBtn.onclick = startChange;

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
