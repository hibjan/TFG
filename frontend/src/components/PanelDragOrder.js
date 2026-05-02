// src/components/PanelDragOrder.js
// Enables drag-to-reorder for panels inside a container.
// Drag is only initiated from the grip icon (☰) injected into each panel's
// top-right corner. Clicking anywhere else in the panel works normally.
// Order is persisted to localStorage.

const STORAGE_KEY = 'panel-order';

export function initPanelDragOrder(container) {
    injectGrips(container);
    restoreOrder(container);
    attachDragListeners(container);
}

// ── Grip injection ────────────────────────────────────────

function injectGrips(container) {
    container.querySelectorAll('[data-panel-id]').forEach(panel => {
        const grip = document.createElement('div');
        grip.className = 'panel-grip';
        grip.title = 'Drag to reorder';
        grip.innerHTML = `
            <span></span>
            <span></span>
            <span></span>
        `;
        // Panels are position:relative (set in CSS), grip is absolute top-right
        panel.appendChild(grip);
    });
}

// ── Persistence ──────────────────────────────────────────

function saveOrder(container) {
    const order = [...container.querySelectorAll('[data-panel-id]')]
        .map(el => el.dataset.panelId);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(order));
}

function restoreOrder(container) {
    try {
        const saved = JSON.parse(localStorage.getItem(STORAGE_KEY));
        if (!Array.isArray(saved) || saved.length === 0) return;

        const unionBar = container.querySelector('.union-bar');
        saved.forEach(id => {
            const el = container.querySelector(`[data-panel-id="${id}"]`);
            if (el && unionBar) container.insertBefore(el, unionBar);
        });
    } catch (_) { /* corrupt storage — ignore */ }
}

// ── Drag listeners ────────────────────────────────────────

function attachDragListeners(container) {
    let dragging = null;
    let indicator = null;
    let dragFromGrip = false;   // gate: only allow drag when grip was clicked

    container.addEventListener('mousedown', e => {
        dragFromGrip = !!e.target.closest('.panel-grip');
    }, true);

    container.addEventListener('dragstart', e => {
        if (!dragFromGrip) { e.preventDefault(); return; }

        const panel = e.target.closest('[data-panel-id]');
        if (!panel) return;

        dragging = panel;
        setTimeout(() => panel.classList.add('panel-dragging'), 0);
        e.dataTransfer.effectAllowed = 'move';
    });

    container.addEventListener('dragend', () => {
        if (!dragging) return;
        dragging.classList.remove('panel-dragging');
        dragging = null;
        dragFromGrip = false;
        removeIndicator();
        saveOrder(container);
    });

    container.addEventListener('dragover', e => {
        e.preventDefault();
        if (!dragging) return;
        e.dataTransfer.dropEffect = 'move';

        const target = dropTarget(e.target, container);
        if (!target || target === dragging) { removeIndicator(); return; }

        const rect = target.getBoundingClientRect();
        placeIndicator(container, target, e.clientY < rect.top + rect.height / 2);
    });

    container.addEventListener('dragleave', e => {
        if (!container.contains(e.relatedTarget)) removeIndicator();
    });

    container.addEventListener('drop', e => {
        e.preventDefault();
        if (!dragging) return;

        const target = dropTarget(e.target, container);
        if (!target || target === dragging) return;

        const rect = target.getBoundingClientRect();
        const insertBefore = e.clientY < rect.top + rect.height / 2;
        container.insertBefore(dragging, insertBefore ? target : target.nextSibling);

        removeIndicator();
        saveOrder(container);
    });

    // ── Helpers ──────────────────────────────────────────

    function dropTarget(el, container) {
        let node = el;
        while (node && node !== container) {
            if (node.parentElement === container && node.dataset.panelId) return node;
            node = node.parentElement;
        }
        return null;
    }

    function placeIndicator(container, target, insertBefore) {
        removeIndicator();
        indicator = document.createElement('div');
        indicator.className = 'drop-indicator';
        container.insertBefore(indicator, insertBefore ? target : target.nextSibling);
    }

    function removeIndicator() {
        if (indicator) { indicator.remove(); indicator = null; }
    }
}