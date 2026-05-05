// src/components/Skeleton.js
// Tiny helpers that paint shape-matching placeholder content into a
// container while the real data is in flight. Each helper clears the
// container first, so it's safe to call right before an async fetch.

export const Skeleton = {
    /** A row of pill buttons (for /datasets, /collections, /facets/links). */
    pillRow(container, count = 5, { large = false } = {}) {
        if (!container) return;
        container.innerHTML = '';
        for (let i = 0; i < count; i++) {
            const skel = document.createElement('div');
            skel.className = 'skeleton skeleton-pill' + (large ? ' skeleton-pill-large' : '');
            // Slight width variance so it doesn't look like a barcode
            skel.style.width = `${90 + ((i * 37) % 80)}px`;
            container.appendChild(skel);
        }
    },

    /** A grid of entity cards (for /entities and /union). */
    entityGrid(container, count = 12) {
        if (!container) return;
        container.innerHTML = '';
        for (let i = 0; i < count; i++) {
            const skel = document.createElement('div');
            skel.className = 'skeleton skeleton-entity-card';
            container.appendChild(skel);
        }
    },

    /** Filter-attribute dropdowns with their tiny label above. */
    dropdowns(container, count = 4) {
        if (!container) return;
        container.innerHTML = '';
        for (let i = 0; i < count; i++) {
            const wrap = document.createElement('div');
            wrap.className = 'skeleton-dropdown-wrap';

            const label = document.createElement('div');
            label.className = 'skeleton skeleton-dropdown-label';
            wrap.appendChild(label);

            const drop = document.createElement('div');
            drop.className = 'skeleton skeleton-dropdown';
            wrap.appendChild(drop);

            container.appendChild(wrap);
        }
    },
};
