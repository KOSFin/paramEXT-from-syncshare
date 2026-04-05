importScripts('background.js');

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (!message || message.type !== 'PARAMEXT_HTTP' || !message.request || typeof message.request.url !== 'string') {
        return;
    }

    const request = message.request;
    const timeoutMsRaw = Number(request.timeoutMs);
    const timeoutMs = Number.isFinite(timeoutMsRaw) ? Math.max(500, timeoutMsRaw) : 4000;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    fetch(request.url, {
        method: typeof request.method === 'string' ? request.method : 'GET',
        headers: request.headers && typeof request.headers === 'object' ? request.headers : undefined,
        body: typeof request.body === 'string' ? request.body : undefined,
        mode: 'cors',
        credentials: 'omit',
        cache: 'no-store',
        redirect: 'follow',
        referrerPolicy: 'no-referrer',
        signal: controller.signal
    }).then(async (response) => {
        let text = '';
        try {
            text = await response.text();
        } catch (_) {
            text = '';
        }

        let json = null;
        if (text) {
            try {
                json = JSON.parse(text);
            } catch (_) {
                json = null;
            }
        }

        sendResponse({
            ok: response.ok,
            status: response.status,
            responseType: response.type,
            redirected: response.redirected,
            finalUrl: response.url || request.url,
            error: (!response.ok && Number(response.status || 0) === 0)
                ? ('status_0_' + String(response.type || 'unknown'))
                : '',
            json,
            text
        });
    }).catch((error) => {
        const isTimeout = controller.signal.aborted;
        sendResponse({
            ok: false,
            status: 0,
            error: isTimeout
                ? 'request_timeout'
                : (error && error.message ? error.message : 'request_failed'),
            errorName: error && error.name ? String(error.name) : '',
            isTimeout
        });
    }).finally(() => {
        clearTimeout(timer);
    });

    return true;
});
