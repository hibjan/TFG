// src/components/PanelDragOrder.js
// Enables drag-to-reorder for panels inside a container.
// Drag starts when the user mousedowns on a panel's header bar
// — `.panel-header` for the Filters/Links panels, `.space-header`
// for the Filtered/Union space panels — and excludes any interactive
// child (button, input, select, anchor, etc.) so action buttons in
// the header still work normally.
// Order is persisted to localStorage.

const STORAGE_KEY = 'panel-order';
const HEADER_SELECTOR = '.panel-header, .space-header';
const INTERACTIVE_SELECTOR = 'button, input, select, textarea, a, label, [role="button"]';

export function initPanelDragOrder(container) {
    restoreOrder(container);
    attachDragListeners(container);
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

        // Legacy: previously anchored against the bottom .union-bar. That bar
        // is gone now, so we just re-append in the saved order at the end.
        const anchor = container.querySelector('.union-bar') || null;
        saved.forEach(id => {
            const el = container.querySelector(`[data-panel-id="${id}"]`);
            if (!el) return;
            if (anchor) container.insertBefore(el, anchor);
            else container.appendChild(el);
        });
    } catch (_) { /* corrupt storage — ignore */ }
}

// ── Drag listeners ──────────────────────────────────────
function attachDragListeners(container) {
    let dragging = null;
    let indicator = null;
    let dragFromHeader = false;

    // Capture phase: decide as early as possible whether this mousedown
    // qualifies as a header grab. If not, the upcoming dragstart is cancelled.
    container.addEventListener('mousedown', (e) => {
        dragFromHeader = isHeaderDragTarget(e.target);
    }, true);

    container.addEventListener('dragstart', (e) => {
        if (!dragFromHeader) { e.preventDefault(); return; }

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
        dragFromHeader = false;
        removeIndicator();
        saveOrder(container);
    });

    container.addEventListener('dragover', (e) => {
        e.preventDefault();
        if (!dragging) return;
        e.dataTransfer.dropEffect = 'move';

        const target = dropTarget(e.target, container);
        if (!target || target === dragging) { removeIndicator(); return; }

        const rect = target.getBoundingClientRect();
        placeIndicator(container, target, e.clientY < rect.top + rect.height / 2);
    });

    container.addEventListener('dragleave', (e) => {
        if (!container.contains(e.relatedTarget)) removeIndicator();
    });

    container.addEventListener('drop', (e) => {
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

    // ── Helpers ────────────────────────────────────────
    function isHeaderDragTarget(target) {
        // Must be inside a header that lives directly inside a draggable panel.
        const header = target.closest(HEADER_SELECTOR);
        if (!header) return false;

        const panel = header.closest('[data-panel-id]');
        if (!panel || header.parentElement !== panel) return false;

        // Action buttons / inputs inside the header don't initiate a drag.
        if (target.closest(INTERACTIVE_SELECTOR)) return false;

        return true;
    }

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
