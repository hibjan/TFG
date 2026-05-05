// src/components/Combobox.js
// Searchable combobox — drop-in replacement for the native <select>
// used in the Filters panel. Supports typeahead filtering, keyboard
// navigation (↑/↓/Enter/Esc), and resets after each pick so the user
// can apply multiple values per attribute without re-opening anything.

/**
 * @param {Object}   cfg
 * @param {Array}    cfg.options       [{ value, label, count? }, ...]
 * @param {string}  [cfg.placeholder]  Placeholder text (default: '— select —')
 * @param {Function} cfg.onSelect      Called with the picked option's `value`
 * @returns {HTMLElement}              The combobox root element
 */
export function createCombobox({ options = [], placeholder = '— select —', onSelect } = {}) {
    const root = document.createElement('div');
    root.className = 'combobox';

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'combobox-input';
    input.placeholder = placeholder;
    input.autocomplete = 'off';
    input.spellcheck = false;
    root.appendChild(input);

    const caret = document.createElement('span');
    caret.className = 'combobox-caret';
    caret.textContent = '▾';
    caret.setAttribute('aria-hidden', 'true');
    root.appendChild(caret);

    const panel = document.createElement('div');
    panel.className = 'combobox-panel hidden';
    root.appendChild(panel);

    let activeIndex = -1;
    let visibleOptions = [];

    function render(query = '') {
        panel.innerHTML = '';
        const q = query.trim().toLowerCase();
        visibleOptions = q
            ? options.filter(o => String(o.label).toLowerCase().includes(q))
            : options.slice();

        if (visibleOptions.length === 0) {
            const empty = document.createElement('div');
            empty.className = 'combobox-empty';
            empty.textContent = 'No matches';
            panel.appendChild(empty);
            activeIndex = -1;
            return;
        }

        visibleOptions.forEach((opt, idx) => {
            const item = document.createElement('div');
            item.className = 'combobox-option' + (idx === activeIndex ? ' active' : '');
            item.dataset.index = String(idx);

            const labelEl = document.createElement('span');
            labelEl.className = 'combobox-option-label';
            labelEl.textContent = String(opt.label);
            item.appendChild(labelEl);

            if (opt.count !== undefined && opt.count !== null) {
                const countEl = document.createElement('span');
                countEl.className = 'combobox-option-count';
                countEl.textContent = `(${opt.count})`;
                item.appendChild(countEl);
            }

            // mousedown (not click) so it fires before input's blur,
            // otherwise the panel would close before the click registers.
            item.addEventListener('mousedown', e => {
                e.preventDefault();
                pick(opt);
            });
            item.addEventListener('mouseenter', () => {
                activeIndex = idx;
                updateActive();
            });

            panel.appendChild(item);
        });
    }

    function updateActive() {
        const items = panel.querySelectorAll('.combobox-option');
        items.forEach((el, idx) => el.classList.toggle('active', idx === activeIndex));
        const activeEl = items[activeIndex];
        if (activeEl) activeEl.scrollIntoView({ block: 'nearest' });
    }

    function open() {
        panel.classList.remove('hidden');
        root.classList.add('open');
    }

    function close() {
        panel.classList.add('hidden');
        root.classList.remove('open');
        activeIndex = -1;
    }

    function pick(opt) {
        // Reset so the user can pick another value of the same attribute
        input.value = '';
        close();
        if (typeof onSelect === 'function') onSelect(opt.value);
    }

    // ── Listeners ──────────────────────────────────────────
    input.addEventListener('focus', () => {
        render(input.value);
        open();
    });

    input.addEventListener('input', () => {
        activeIndex = -1;
        render(input.value);
        open();
    });

    input.addEventListener('blur', () => {
        // Brief delay so a click on a panel option can land before we close
        setTimeout(close, 120);
    });

    input.addEventListener('keydown', (e) => {
        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                if (visibleOptions.length === 0) return;
                activeIndex = (activeIndex + 1) % visibleOptions.length;
                updateActive();
                break;
            case 'ArrowUp':
                e.preventDefault();
                if (visibleOptions.length === 0) return;
                activeIndex = (activeIndex - 1 + visibleOptions.length) % visibleOptions.length;
                updateActive();
                break;
            case 'Enter':
                e.preventDefault();
                if (activeIndex >= 0 && visibleOptions[activeIndex]) {
                    pick(visibleOptions[activeIndex]);
                } else if (visibleOptions.length === 1) {
                    // Convenience: only one filtered result → pick it
                    pick(visibleOptions[0]);
                }
                break;
            case 'Escape':
                input.blur();
                break;
        }
    });

    // Pre-render so visibleOptions is populated even before first focus
    render('');

    return root;
}
