// src/components/Modal.js
import { getApiBase } from '../api.js';

// ── State ────────────────────────────────────────────────────
// Stack of entity data objects. Each entry = one "layer" in the modal.
let entityStack = [];
// Callback supplied by main.js so we can fetch entity data from here.
let fetchEntityCallback = null;
let modalRefsCache = null;
let listenersInitialized = false;

/**
 * Set the function used to fetch entity data.
 * Signature: async (entityId, collectionId) => entityDataObject
 */
export function setEntityFetcher(fn) {
    fetchEntityCallback = fn;
}

/**
 * Open (or push onto) the entity modal.
 * - First call opens the overlay.
 * - Subsequent calls while the modal is open push a new layer on top.
 */
export function renderModalContent(data, modalRefs) {
    modalRefsCache = modalRefs;
    initListenersOnce(modalRefs);

    entityStack.push(data);
    renderCurrentLayer(modalRefs);

    modalRefs.modal.classList.remove('hidden');
}

// ── Internal: render the top of the stack ────────────────────
function renderCurrentLayer(modalRefs) {
    const {
        modalName,
        modalContents,
        modalResources,
        modalMetadata,
        modalReferences,
        modalBackBtn,
        modalBreadcrumb,
    } = modalRefs;

    const data = entityStack[entityStack.length - 1];

    modalName.textContent = data.name || `Entity #${data.id}`;
    modalContents.innerHTML = '';
    modalResources.innerHTML = '';
    modalMetadata.innerHTML = '';
    modalReferences.innerHTML = '';

    renderResources(data, modalResources);
    renderContents(data, modalContents);
    renderMetadata(data, modalMetadata);
    renderReferences(data, modalReferences);
    renderBreadcrumb(modalBreadcrumb);

    // Show/hide the back button
    if (modalBackBtn) {
        modalBackBtn.classList.toggle('hidden', entityStack.length <= 1);
    }
}

// ── Breadcrumb trail ─────────────────────────────────────────
function renderBreadcrumb(container) {
    if (!container) return;
    container.innerHTML = '';

    if (entityStack.length <= 1) {
        container.classList.add('hidden');
        return;
    }
    container.classList.remove('hidden');

    entityStack.forEach((entry, i) => {
        if (i > 0) {
            const sep = document.createElement('span');
            sep.className = 'modal-breadcrumb-sep';
            sep.textContent = '›';
            container.appendChild(sep);
        }

        const crumb = document.createElement('span');
        crumb.className = 'modal-breadcrumb-item';
        const entityLabel = entry.name || `Entity #${entry.id}`;
        const colName = entry.collection?.name;
        crumb.textContent = colName ? `${entityLabel} (${colName})` : entityLabel;
        crumb.title = crumb.textContent;

        if (i === entityStack.length - 1) {
            crumb.classList.add('current');
        } else {
            // Clicking a breadcrumb pops back to that level
            crumb.addEventListener('click', () => {
                entityStack = entityStack.slice(0, i + 1);
                renderCurrentLayer(modalRefsCache);
            });
        }

        container.appendChild(crumb);
    });
}

// ── Close / back helpers ─────────────────────────────────────
function closeModal(modal) {
    modal.classList.add('hidden');
    entityStack = [];
}

function goBack() {
    if (entityStack.length <= 1) return;
    entityStack.pop();
    renderCurrentLayer(modalRefsCache);
}

// ── Listener wiring ──────────────────────────────────────────
function initListenersOnce(modalRefs) {
    if (listenersInitialized) return;
    const { modal, modalClose, modalBackBtn } = modalRefs;

    // Close via × button
    modalClose.addEventListener('click', () => closeModal(modal));

    // Click outside the card to close
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeModal(modal);
    });

    // Escape key closes the modal (or pops the stack)
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !modal.classList.contains('hidden')) {
            if (entityStack.length > 1) {
                goBack();
            } else {
                closeModal(modal);
            }
        }
    });

    // Back button
    if (modalBackBtn) {
        modalBackBtn.addEventListener('click', goBack);
    }

    listenersInitialized = true;
}

// ── Contents (key/value table from JSON `contents`) ──────────
function renderContents(data, contentsEl) {
    if (!data.contents) {
        contentsEl.classList.add('hidden');
        return;
    }

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
        contentsEl.appendChild(table);
    } else {
        contentsEl.textContent = typeof data.contents === 'string'
            ? data.contents
            : JSON.stringify(data.contents);
    }

    contentsEl.classList.remove('hidden');
}

// ── Metadata table ──────────────────────────────────────────
function renderMetadata(data, container) {
    if (!data.metadata || Object.keys(data.metadata).length === 0) return;

    const title = document.createElement('h3');
    title.className = 'modal-section-title';
    title.textContent = 'Metadata';
    container.appendChild(title);

    const table = document.createElement('table');
    table.className = 'modal-meta-table';
    for (const [key, values] of Object.entries(data.metadata)) {
        const row = table.insertRow();
        const keyCell = row.insertCell();
        keyCell.textContent = key;
        const valCell = row.insertCell();
        valCell.textContent = Array.isArray(values) ? values.join(', ') : String(values);
    }
    container.appendChild(table);
}

// ── References (grouped chips by reason) ────────────────────
function renderReferences(data, container) {
    if (!data.references || Object.keys(data.references).length === 0) return;

    const title = document.createElement('h3');
    title.className = 'modal-section-title';
    title.textContent = 'References';
    container.appendChild(title);

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
            chip.className = 'modal-ref-chip clickable';
            chip.textContent = ref.name || `#${ref.id}`;
            chip.title = `Click to view ${ref.name || `#${ref.id}`}`;

            chip.addEventListener('click', () => onRefChipClick(ref));

            list.appendChild(chip);
        });
        group.appendChild(list);

        container.appendChild(group);
    }
}

// ── Clicking a reference chip ────────────────────────────────
async function onRefChipClick(ref) {
    if (!fetchEntityCallback) {
        console.warn('No entity fetcher configured — cannot open referenced entity');
        return;
    }
    try {
        const data = await fetchEntityCallback(ref.id, ref.collectionId);
        if (data && modalRefsCache) {
            entityStack.push(data);
            renderCurrentLayer(modalRefsCache);
        }
    } catch (err) {
        console.error('Failed to load referenced entity:', err);
    }
}

// ── Resources (toggle-button categories) ────────────────────
function renderResources(data, container) {
    // Extract resources from parsed contents._resources
    let resources = [];
    if (data.contents) {
        let parsed;
        try {
            parsed = typeof data.contents === 'string' ? JSON.parse(data.contents) : data.contents;
        } catch (_) {
            parsed = null;
        }
        if (parsed && typeof parsed === 'object' && Array.isArray(parsed._resources)) {
            resources = parsed._resources;
        }
    }

    if (resources.length === 0) return;

    const backendOrigin = getApiBase().replace(/\/api$/, '');
    const resolveUrl = (url) => url.startsWith('/') ? backendOrigin + url : url;

    const images = resources.filter(r => r.type === 'image');
    const videos = resources.filter(r => r.type === 'video');
    const pdfs   = resources.filter(r => r.type === 'pdf');
    const links  = resources.filter(r => r.type === 'link');

    // Category definitions
    const categories = [
        { key: 'images',  icon: '📷',  label: 'Images',    items: images },
        { key: 'videos',  icon: '🎬',  label: 'Videos',    items: videos },
        { key: 'pdfs',    icon: '📄',  label: 'Documents', items: pdfs },
        { key: 'links',   icon: '🔗',  label: 'Links',     items: links },
    ].filter(c => c.items.length > 0);

    if (categories.length === 0) return;

    // Button bar
    const bar = document.createElement('div');
    bar.className = 'modal-resource-bar';

    // Track which panel is currently open (null = none)
    let openKey = null;
    const panels = {};
    const buttons = {};

    categories.forEach(cat => {
        // Toggle button
        const btn = document.createElement('button');
        btn.className = 'modal-resource-toggle';
        btn.innerHTML = `<span class="resource-icon">${cat.icon}</span> ${cat.label} <span class="resource-count">${cat.items.length}</span>`;
        buttons[cat.key] = btn;

        // Collapsible panel
        const panel = document.createElement('div');
        panel.className = 'modal-resource-panel';
        const inner = document.createElement('div');
        inner.className = 'modal-resource-panel-inner';
        panels[cat.key] = panel;

        // Render the content inside the panel
        buildResourceContent(cat.key, cat.items, inner, resolveUrl);
        panel.appendChild(inner);

        btn.addEventListener('click', () => {
            if (openKey === cat.key) {
                // Close
                panel.classList.remove('open');
                btn.classList.remove('active');
                openKey = null;
            } else {
                // Close previous
                if (openKey) {
                    panels[openKey].classList.remove('open');
                    buttons[openKey].classList.remove('active');
                }
                // Open this
                panel.classList.add('open');
                btn.classList.add('active');
                openKey = cat.key;
            }
        });

        bar.appendChild(btn);
    });

    container.appendChild(bar);

    // Append all panels (only the open one will be visible)
    categories.forEach(cat => {
        container.appendChild(panels[cat.key]);
    });
}

function buildResourceContent(type, items, container, resolveUrl) {
    switch (type) {
        case 'images': {
            const wrap = document.createElement('div');
            wrap.className = 'modal-resource-images';
            items.forEach(res => {
                const figure = document.createElement('figure');
                figure.className = 'modal-resource-figure';

                const a = document.createElement('a');
                a.href = resolveUrl(res.url);
                a.target = '_blank';
                a.rel = 'noopener noreferrer';

                const img = document.createElement('img');
                img.className = 'modal-resource-image';
                img.src = resolveUrl(res.url);
                img.alt = res.label || '';
                img.loading = 'lazy';

                a.appendChild(img);
                figure.appendChild(a);

                if (res.label) {
                    const caption = document.createElement('figcaption');
                    caption.textContent = res.label;
                    figure.appendChild(caption);
                }

                wrap.appendChild(figure);
            });
            container.appendChild(wrap);
            break;
        }
        case 'videos': {
            const wrap = document.createElement('div');
            wrap.className = 'modal-resource-videos';
            items.forEach(res => {
                const figure = document.createElement('figure');
                figure.className = 'modal-resource-figure';

                const video = document.createElement('video');
                video.className = 'modal-resource-video';
                video.src = resolveUrl(res.url);
                video.controls = true;

                figure.appendChild(video);

                if (res.label) {
                    const caption = document.createElement('figcaption');
                    caption.textContent = res.label;
                    figure.appendChild(caption);
                }

                wrap.appendChild(figure);
            });
            container.appendChild(wrap);
            break;
        }
        case 'pdfs': {
            const wrap = document.createElement('div');
            wrap.className = 'modal-resource-pdfs';
            items.forEach(res => {
                const a = document.createElement('a');
                a.className = 'modal-resource-link modal-resource-pdf';
                a.href = resolveUrl(res.url);
                a.target = '_blank';
                a.rel = 'noopener noreferrer';
                a.textContent = `📄 ${res.label || 'PDF Document'}`;
                wrap.appendChild(a);
            });
            container.appendChild(wrap);
            break;
        }
        case 'links': {
            const wrap = document.createElement('div');
            wrap.className = 'modal-resource-links';
            items.forEach(res => {
                const a = document.createElement('a');
                a.className = 'modal-resource-link';
                a.href = resolveUrl(res.url);
                a.target = '_blank';
                a.rel = 'noopener noreferrer';
                a.textContent = `🔗 ${res.label || res.url}`;
                wrap.appendChild(a);
            });
            container.appendChild(wrap);
            break;
        }
    }
}
