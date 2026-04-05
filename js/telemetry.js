(function (global) {
    const MAX_QUEUE_SIZE = 20;
    const RECOVERY_FLAG_KEY = '__paramext_context_recovery_ts';
    const AUTH_BLOCK_MS = 120000;
    const ERROR_BLOCK_MS = 15000;
    const installedHandlerScopes = new Set();
    const queue = [];
    let flushInFlight = false;
    let blockedUntil = 0;

    function safeGetExtensionVersion() {
        try {
            if (!chrome || !chrome.runtime || typeof chrome.runtime.getManifest !== 'function') {
                return 'unknown';
            }
            return chrome.runtime.getManifest().version || 'unknown';
        } catch (_) {
            return 'invalidated';
        }
    }

    function getEventMessage(value) {
        if (!value) {
            return '';
        }
        if (typeof value === 'string') {
            return value;
        }
        if (value instanceof Error && value.message) {
            return String(value.message);
        }
        if (typeof value.message === 'string') {
            return value.message;
        }
        return '';
    }

    function isExtensionContextInvalidated(value) {
        const message = getEventMessage(value).toLowerCase();
        return message.includes('extension context invalidated');
    }

    function recoverInvalidContext(scope, source) {
        try {
            // Inside an iframe, skip reload — the top-level frame handles its own recovery.
            // Reloading only the iframe while the outer React page stays alive causes
            // state inconsistency and a potential double-reload cascade.
            if (window !== window.top) {
                return;
            }

            const now = Date.now();
            const last = Number(sessionStorage.getItem(RECOVERY_FLAG_KEY) || '0');
            if (Number.isFinite(last) && now - last < 10000) {
                return;
            }

            sessionStorage.setItem(RECOVERY_FLAG_KEY, String(now));
            setTimeout(() => {
                location.reload();
            }, 150);
        } catch (_) {
            // Keep recovery best-effort only.
        }

        try {
            console.warn('[paramEXT] Extension context invalidated, reloading tab...', {
                scope,
                source
            });
        } catch (_) {
            // Ignore console failures.
        }
    }

    async function getSettingsSafe() {
        try {
            if (!global.ParamExtSettings) {
                return null;
            }
            return await global.ParamExtSettings.getSettings();
        } catch (_) {
            return null;
        }
    }

    function normalizeBaseUrl(raw) {
        if (!raw || typeof raw !== 'string') {
            return '';
        }
        return raw.trim().replace(/\/$/, '');
    }

    function buildHeaders(token) {
        const headers = {
            'Content-Type': 'application/json'
        };

        if (token && token.length > 0) {
            headers.Authorization = 'Bearer ' + token;
            headers['X-API-Token'] = token;
        }

        return headers;
    }

    function buildSystemInfo(scope) {
        return {
            scope,
            extensionVersion: safeGetExtensionVersion(),
            userAgent: navigator.userAgent,
            language: navigator.language,
            platform: navigator.platform,
            url: location.href,
            timestamp: new Date().toISOString()
        };
    }

    async function requestViaBackground(request) {
        return await new Promise((resolve) => {
            try {
                if (!chrome || !chrome.runtime || typeof chrome.runtime.sendMessage !== 'function') {
                    resolve(null);
                    return;
                }
            } catch (_) {
                resolve(null);
                return;
            }

            chrome.runtime.sendMessage({
                type: 'PARAMEXT_HTTP',
                request
            }, (response) => {
                const lastError = chrome.runtime.lastError;
                if (lastError) {
                    resolve(null);
                    return;
                }
                resolve(response || null);
            });
        });
    }

    function getPlatformFromScope(scope, activePlatform) {
        if (typeof scope === 'string') {
            if (scope.includes('moodle')) {
                return 'moodle';
            }
            if (scope.includes('openedu')) {
                return 'openedu';
            }
        }
        return activePlatform === 'moodle' ? 'moodle' : 'openedu';
    }

    function pickBackendConfig(settings, scope) {
        const platform = getPlatformFromScope(scope, settings?.activePlatform);

        if (global.ParamExtSettings && typeof global.ParamExtSettings.getBackendByPlatform === 'function') {
            return global.ParamExtSettings.getBackendByPlatform(settings, platform);
        }

        const backend = settings?.backend || {};
        if (backend.moodle || backend.openedu) {
            return platform === 'moodle' ? (backend.moodle || {}) : (backend.openedu || {});
        }

        return backend;
    }

    async function flushQueue(scope) {
        if (queue.length === 0) {
            return;
        }

        if (flushInFlight) {
            return;
        }

        if (blockedUntil > Date.now()) {
            return;
        }

        flushInFlight = true;

        try {
            const settings = await getSettingsSafe();
            const backendConfig = pickBackendConfig(settings, scope);
            const baseUrl = normalizeBaseUrl(backendConfig?.apiBaseUrl);
            if (!baseUrl) {
                return;
            }

            const token = backendConfig?.apiToken || '';
            const timeoutMs = Number(backendConfig?.requestTimeoutMs || 4000);

            while (queue.length > 0) {
                const packet = queue[0];
                if (!packet) {
                    queue.shift();
                    continue;
                }

                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), timeoutMs);

                let delivered = false;

                try {
                    const bgResponse = await requestViaBackground({
                        url: baseUrl + '/v1/logs/client',
                        method: 'POST',
                        headers: buildHeaders(token),
                        body: JSON.stringify(packet),
                        timeoutMs
                    });

                    if (bgResponse) {
                        if (bgResponse.ok) {
                            delivered = true;
                        } else {
                            const status = Number(bgResponse.status || 0);
                            if (status === 401 || status === 403) {
                                blockedUntil = Date.now() + AUTH_BLOCK_MS;
                                queue.shift();
                                break;
                            }

                            blockedUntil = Date.now() + ERROR_BLOCK_MS;
                            break;
                        }
                    } else {
                        const response = await fetch(baseUrl + '/v1/logs/client', {
                            method: 'POST',
                            headers: buildHeaders(token),
                            body: JSON.stringify(packet),
                            signal: controller.signal
                        });

                        if (response.ok) {
                            delivered = true;
                        } else {
                            if (response.status === 401 || response.status === 403) {
                                blockedUntil = Date.now() + AUTH_BLOCK_MS;
                                queue.shift();
                                break;
                            }

                            blockedUntil = Date.now() + ERROR_BLOCK_MS;
                            break;
                        }
                    }
                } catch (_) {
                    blockedUntil = Date.now() + ERROR_BLOCK_MS;
                    break;
                } finally {
                    clearTimeout(timer);
                }

                if (delivered) {
                    queue.shift();
                }
            }
        } finally {
            flushInFlight = false;
        }
    }

    function push(kind, payload, scope) {
        const packet = {
            kind,
            payload,
            system: buildSystemInfo(scope)
        };

        queue.push(packet);
        if (queue.length > MAX_QUEUE_SIZE) {
            queue.splice(0, queue.length - MAX_QUEUE_SIZE);
        }

        flushQueue(scope);
    }

    function installGlobalHandlers(scope) {
        const finalScope = scope || 'global';
        if (installedHandlerScopes.has(finalScope)) {
            return;
        }
        installedHandlerScopes.add(finalScope);

        window.addEventListener('error', (event) => {
            const errMessage = getEventMessage(event && event.error) || getEventMessage(event && event.message);
            if (isExtensionContextInvalidated(errMessage)) {
                if (typeof event.preventDefault === 'function') {
                    event.preventDefault();
                }
                recoverInvalidContext(finalScope, 'error');
                return;
            }

            push('error', {
                message: event.message,
                source: event.filename,
                line: event.lineno,
                column: event.colno,
                stack: event.error && event.error.stack ? String(event.error.stack) : ''
            }, finalScope);
        });

        window.addEventListener('unhandledrejection', (event) => {
            const reason = event.reason;
            if (isExtensionContextInvalidated(reason)) {
                if (typeof event.preventDefault === 'function') {
                    event.preventDefault();
                }
                recoverInvalidContext(finalScope, 'unhandledrejection');
                return;
            }

            push('unhandledrejection', {
                message: typeof reason === 'string' ? reason : (reason && reason.message ? String(reason.message) : 'unknown rejection'),
                stack: reason && reason.stack ? String(reason.stack) : ''
            }, finalScope);
        });
    }

    if (typeof window !== 'undefined') {
        installGlobalHandlers('bootstrap');
    }

    global.ParamExtTelemetry = {
        push,
        installGlobalHandlers
    };
})(globalThis);
