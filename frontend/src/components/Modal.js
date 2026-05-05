// src/components/Modal.js
import { getApiBase } from '../api.js';

// Listeners are wired up exactly once on the first render call,
// so we never accumulate duplicates and never replace DOM nodes.
let listenersInitialized = false;

export function renderModalContent(data, modalRefs) {
    const {
        modal,
        modalName,
        modalContents,
        modalResources,
        modalMetadata,
        modalReferences,
    } = modalRefs;

    initListenersOnce(modalRefs);

    modalName.textContent = data.name || `Entity #${data.id}`;
    modalContents.innerHTML = '';
    modalResources.innerHTML = '';
    modalMetadata.innerHTML = '';
    modalReferences.innerHTML = '';

    renderContents(data, modalContents, modalResources);
    renderMetadata(data, modalMetadata);
    renderReferences(data, modalReferences);

    modal.classList.remove('hidden');
}

// ── Listener wiring ──────────────────────────────────────────
function initListenersOnce(modalRefs) {
    if (listenersInitialized) return;
    const { modal, modalClose } = modalRefs;

    // Close via × button
    modalClose.addEventListener('click', () => modal.classList.add('hidden'));

    // Click outside the card to close
    modal.addEventListener('click', (e) => {
        if (e.target === modal) modal.classList.add('hidden');
    });

    listenersInitialized = true;
}

// ── Contents (key/value table from JSON `contents`) ──────────
function renderContents(data, contentsEl, resourcesEl) {
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

        const resources = parsed._resources;
        if (Array.isArray(resources) && resources.length > 0) {
            renderResources(resources, resourcesEl);
        }
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
            chip.className = 'modal-ref-chip';
            chip.textContent = ref.name || `#${ref.id}`;
            list.appendChild(chip);
        });
        group.appendChild(list);

        container.appendChild(group);
    }
}

// ── Resources (images / videos / pdfs / links) ──────────────
function renderResources(resources, container) {
    const backendOrigin = getApiBase().replace(/\/api$/, '');
    const resolveUrl = (url) => url.startsWith('/') ? backendOrigin + url : url;

    const images = resources.filter(r => r.type === 'image');
    const videos = resources.filter(r => r.type === 'video');
    const pdfs = resources.filter(r => r.type === 'pdf');
    const links = resources.filter(r => r.type === 'link');

    if (images.length > 0) {
        const wrap = document.createElement('div');
        wrap.className = 'modal-resource-images';
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
            img.alt = res.label || '';
            img.loading = 'lazy';

            a.appendChild(img);
            figure.appendChild(a);

            const caption = document.createElement('figcaption');
            caption.textContent = res.label || '';
            figure.appendChild(caption);

            wrap.appendChild(figure);
        });
        container.appendChild(wrap);
    }

    if (videos.length > 0) {
        const wrap = document.createElement('div');
        wrap.className = 'modal-resource-videos';
        videos.forEach(res => {
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
    }

    if (pdfs.length > 0) {
        const wrap = document.createElement('div');
        wrap.className = 'modal-resource-pdfs';
        pdfs.forEach(res => {
            const a = document.createElement('a');
            a.className = 'modal-resource-link modal-resource-pdf';
            a.href = resolveUrl(res.url);
            a.target = '_blank';
            a.rel = 'noopener noreferrer';
            a.textContent = `📄 ${res.label || 'PDF Document'}`;
            wrap.appendChild(a);
        });
        container.appendChild(wrap);
    }

    if (links.length > 0) {
        const wrap = document.createElement('div');
        wrap.className = 'modal-resource-links';
        links.forEach(res => {
            const a = document.createElement('a');
            a.className = 'modal-resource-link';
            a.href = resolveUrl(res.url);
            a.target = '_blank';
            a.rel = 'noopener noreferrer';
            a.textContent = `🔗 ${res.label || res.url}`;
            wrap.appendChild(a);
        });
        container.appendChild(wrap);
    }
}
