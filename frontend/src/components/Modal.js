// src/components/Modal.js
import { getApiBase } from '../api.js';

export function renderModalContent(data, modalRefs) {
    const { modal, modalName, modalContents, modalResources, modalMetadata, modalClose } = modalRefs;

    modalName.textContent = data.name || `Entity #${data.id}`;
    modalContents.innerHTML = '';
    modalResources.innerHTML = '';
    modalMetadata.innerHTML = '';

    if (!data.contents) {
        modalContents.classList.add('hidden');
        showModal(modal, modalClose);
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
        modalContents.appendChild(table);

        // Render Resources
        const resources = parsed._resources;
        if (Array.isArray(resources) && resources.length > 0) {
            renderResources(resources, modalResources);
        }
        modalContents.classList.remove('hidden');
    } else {
        modalContents.textContent = typeof data.contents === 'string' ? data.contents : JSON.stringify(data.contents);
        modalContents.classList.remove('hidden');
    }

    showModal(modal, modalClose);
}

function showModal(modal, modalClose) {
    modal.classList.remove('hidden');

    // Wire close button (replace to avoid duplicate listeners)
    const freshClose = modalClose.cloneNode(true);
    modalClose.parentNode.replaceChild(freshClose, modalClose);
    freshClose.addEventListener('click', () => modal.classList.add('hidden'));

    // Click outside the card to close
    modal.addEventListener('click', e => {
        if (e.target === modal) modal.classList.add('hidden');
    }, { once: true });
}

function renderResources(resources, container) {
    const backendOrigin = getApiBase().replace(/\/api$/, '');
    const resolveUrl = (url) => url.startsWith('/') ? backendOrigin + url : url;

    // Group resources
    const groups = {
        images: resources.filter(r => r.type === 'image'),
        videos: resources.filter(r => r.type === 'video'),
        pdfs: resources.filter(r => r.type === 'pdf'),
        links: resources.filter(r => r.type === 'link')
    };

    // Render Images
    if (groups.images.length > 0) {
        const div = document.createElement('div');
        div.className = 'modal-resource-images';
        groups.images.forEach(res => {
            div.innerHTML += `
                <figure class="modal-resource-figure">
                    <a href="${resolveUrl(res.url)}" target="_blank" rel="noopener noreferrer">
                        <img class="modal-resource-image" src="${resolveUrl(res.url)}" alt="${res.label}" loading="lazy">
                    </a>
                    <figcaption>${res.label}</figcaption>
                </figure>
            `;
        });
        container.appendChild(div);
    }
}