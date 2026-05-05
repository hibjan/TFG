// src/api.js
const API_BASE = `${import.meta.env.VITE_API_BASE_URL}/backend/api`;

export const getApiBase = () => API_BASE;

export async function api(endpoint, options = {}) {
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