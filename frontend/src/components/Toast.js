// src/components/Toast.js
// Lightweight toast notifications. The host page must include
// <div id="toasts" class="toasts"></div> somewhere in the DOM.
//
// Usage:
//   import { showToast } from './components/Toast.js';
//   showToast('Filter applied');                      // info, 5s
//   showToast('Failed to load', 'error');             // error, 5s
//   showToast('Welcome back', 'success', 3000);       // success, 3s
//   showToast('Saved', 'success', { duration: 0 });   // sticky (no auto-dismiss)

let containerEl = null;

function getContainer() {
    if (!containerEl) containerEl = document.getElementById('toasts');
    return containerEl;
}

/**
 * @param {string}  message
 * @param {'info'|'success'|'error'} [type='info']
 * @param {number|object} [opts]  duration in ms, or { duration }
 * @returns {Function} dismiss function (call to close manually)
 */
export function showToast(message, type = 'info', opts = {}) {
    const container = getContainer();
    if (!container) return () => {};

    const duration = typeof opts === 'number' ? opts : (opts.duration ?? 5000);

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.setAttribute('role', type === 'error' ? 'alert' : 'status');

    const messageEl = document.createElement('span');
    messageEl.className = 'toast-message';
    messageEl.textContent = message;
    toast.appendChild(messageEl);

    const closeBtn = document.createElement('button');
    closeBtn.className = 'toast-close';
    closeBtn.setAttribute('aria-label', 'Dismiss');
    closeBtn.innerHTML = '&times;';
    toast.appendChild(closeBtn);

    let timer = null;
    const dismiss = () => {
        if (timer) { clearTimeout(timer); timer = null; }
        if (!toast.isConnected) return;
        toast.classList.add('toast-dismissing');
        setTimeout(() => toast.remove(), 200);
    };

    closeBtn.onclick = dismiss;
    container.appendChild(toast);

    if (duration > 0) timer = setTimeout(dismiss, duration);

    return dismiss;
}
