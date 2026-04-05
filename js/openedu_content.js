(function () {
    const HOST_RE = /(^|\.)openedu\.ru$/i;
    const STICK_ID = 'paramext-openedu-stick';
    const WAND_TOGGLE_ID = 'paramext-openedu-wand-toggle';
    const QUESTION_KEY_ATTR = 'data-paramext-openedu-question-key';
    const INLINE_WAND_ATTR = 'data-paramext-openedu-inline-wand';
    const INLINE_MENU_CLASS = 'paramext-openedu-inline-menu';
    const MAX_ANSWERS_PER_QUESTION = 5;
    const RETRY_DELAYS_MS = [0, 350, 900];
    const CYCLE_INTERVAL_MS = 7000;
    const BACKEND_LOG_THROTTLE_MS = 30000;

    const NEGATIVE_MARK_RE = /(choicegroup_incorrect|(^|[^a-zа-яё])(incorrect|wrong|false|неверн|неправильн|ошиб)([^a-zа-яё]|$))/i;
    const POSITIVE_MARK_RE = /(choicegroup_correct|(^|[^a-zа-яё])(correct|right|true|верн|правильн)([^a-zа-яё]|$))/i;

    if (!HOST_RE.test(location.hostname)) {
        return;
    }

    if (!window.ParamExtSettings) {
        return;
    }

    if (window.ParamExtTelemetry) {
        window.ParamExtTelemetry.installGlobalHandlers('openedu-content');
    }

    const isTopFrame = window === window.top;

    let settings = null;
    let stickRoot = null;
    let stickBody = null;
    let wandToggle = null;
    let statusDot = null;
    let statusText = null;
    let lastAutoAdvanceAt = 0;
    let cycleInFlight = false;
    let panelVisible = false;
    let lastBackendIssueAt = 0;
    let lastBackendIssueSignature = '';

    function textOf(node) {
        return (node && node.textContent ? node.textContent : '').replace(/\s+/g, ' ').trim();
    }

    function normalizeText(value) {
        return String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
    }

    function hash(input) {
        let value = 0;
        const source = String(input || '');
        for (let i = 0; i < source.length; i += 1) {
            value = ((value << 5) - value) + source.charCodeAt(i);
            value |= 0;
        }
        return String(Math.abs(value));
    }

    function delay(ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    }

    function escapeSelector(value) {
        const raw = String(value || '');
        if (globalThis.CSS && typeof globalThis.CSS.escape === 'function') {
            return globalThis.CSS.escape(raw);
        }
        return raw.replace(/([!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~\s])/g, '\\$1');
    }

    function normalizeApiBaseUrl() {
        const raw = settings?.backend?.openedu?.apiBaseUrl || settings?.backend?.apiBaseUrl;
        if (typeof raw !== 'string') {
            return '';
        }
        return raw.trim().replace(/\/$/, '');
    }

    function getAuthHeaders(withJsonContentType) {
        const token = settings?.backend?.openedu?.apiToken || settings?.backend?.apiToken || '';
        const headers = {};
        if (withJsonContentType) {
            headers['Content-Type'] = 'application/json';
        }
        if (token.length > 0) {
            headers.Authorization = 'Bearer ' + token;
        }
        return headers;
    }

    function maybeLogBackendIssue(kind, payload) {
        if (!window.ParamExtTelemetry || typeof window.ParamExtTelemetry.push !== 'function') {
            return;
        }

        const signature = kind + '|' + String(payload?.path || '') + '|' + String(payload?.status || 0) + '|' + String(payload?.error || '');
        const now = Date.now();
        if (signature === lastBackendIssueSignature && now - lastBackendIssueAt < BACKEND_LOG_THROTTLE_MS) {
            return;
        }

        lastBackendIssueSignature = signature;
        lastBackendIssueAt = now;
        window.ParamExtTelemetry.push(kind, payload, 'openedu-content');
    }

    function errorMessageFromPayload(raw) {
        if (!raw) {
            return '';
        }

        if (typeof raw === 'string') {
            return raw;
        }

        if (typeof raw.detail === 'string') {
            return raw.detail;
        }

        if (typeof raw.message === 'string') {
            return raw.message;
        }

        return '';
    }

    async function requestViaBackground(request) {
        return await new Promise((resolve) => {
            try {
                if (!chrome.runtime || typeof chrome.runtime.sendMessage !== 'function') {
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

    async function requestJson(method, path, body, logErrors) {
        const baseUrl = normalizeApiBaseUrl();
        if (!baseUrl) {
            return {
                ok: false,
                status: 0,
                error: 'api_base_url_missing',
                data: null
            };
        }

        const timeoutMs = Number(settings?.backend?.openedu?.requestTimeoutMs || settings?.backend?.requestTimeoutMs || 4000);
        const request = {
            url: baseUrl + path,
            method,
            headers: getAuthHeaders(body !== null),
            timeoutMs
        };

        if (body !== null) {
            request.body = JSON.stringify(body);
        }

        const bgResponse = await requestViaBackground(request);
        if (bgResponse) {
            if (!bgResponse.ok) {
                const bgError = String(bgResponse.error || errorMessageFromPayload(bgResponse.json) || bgResponse.text || ('http_' + String(bgResponse.status || 0))).trim();
                const result = {
                    ok: false,
                    status: Number(bgResponse.status || 0),
                    error: bgError || 'request_failed',
                    data: null
                };

                if (logErrors) {
                    maybeLogBackendIssue('openedu_backend_error', {
                        method,
                        path,
                        status: result.status,
                        error: result.error,
                        via: 'background'
                    });
                }
                return result;
            }

            return {
                ok: true,
                status: Number(bgResponse.status || 200),
                error: '',
                data: bgResponse.json && typeof bgResponse.json === 'object' ? bgResponse.json : null
            };
        }

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(baseUrl + path, {
                method,
                headers: getAuthHeaders(body !== null),
                body: body !== null ? JSON.stringify(body) : undefined,
                signal: controller.signal
            });

            let text = '';
            try {
                text = await response.text();
            } catch (_) {
                text = '';
            }

            let data = null;
            if (text) {
                try {
                    data = JSON.parse(text);
                } catch (_) {
                    data = null;
                }
            }

            if (!response.ok) {
                const result = {
                    ok: false,
                    status: Number(response.status || 0),
                    error: errorMessageFromPayload(data) || text || ('http_' + String(response.status || 0)),
                    data: null
                };

                if (logErrors) {
                    maybeLogBackendIssue('openedu_backend_error', {
                        method,
                        path,
                        status: result.status,
                        error: result.error,
                        via: 'content'
                    });
                }
                return result;
            }

            return {
                ok: true,
                status: Number(response.status || 200),
                error: '',
                data
            };
        } catch (error) {
            const message = error && error.message ? String(error.message) : 'network_error';
            const result = {
                ok: false,
                status: 0,
                error: message,
                data: null
            };

            if (logErrors) {
                maybeLogBackendIssue('openedu_backend_error', {
                    method,
                    path,
                    status: 0,
                    error: message,
                    via: 'content'
                });
            }

            return result;
        } finally {
            clearTimeout(timer);
        }
    }

    async function postWithRetry(path, body, retries) {
        let last = {
            ok: false,
            status: 0,
            error: 'request_failed',
            data: null
        };

        for (let attempt = 0; attempt <= retries; attempt += 1) {
            if (attempt > 0) {
                const delayMs = RETRY_DELAYS_MS[Math.min(attempt, RETRY_DELAYS_MS.length - 1)] || 300;
                await delay(delayMs);
            }

            last = await requestJson('POST', path, body, true);
            if (last.ok) {
                return last;
            }

            if (last.status >= 400 && last.status < 500 && last.status !== 429) {
                break;
            }
        }

        return last;
    }

    function describeRequestError(result) {
        if (!result || result.ok) {
            return '';
        }

        if (result.error === 'api_base_url_missing') {
            return 'не указан API URL';
        }

        if (result.status === 401) {
            return '401 (токен)';
        }

        if (result.status === 403) {
            return '403 (доступ)';
        }

        if (result.status === 404) {
            return '404 (роут)';
        }

        if (result.status > 0) {
            return String(result.status);
        }

        return String(result.error || 'network');
    }

    async function probeBackendOnline() {
        const baseUrl = normalizeApiBaseUrl();
        if (!baseUrl) {
            return false;
        }

        const probePaths = ['/healthz', '/health', '/v2/status'];
        let hasHttpResponse = false;

        for (const path of probePaths) {
            const result = await requestJson('GET', path, null, false);
            if (result.ok) {
                return true;
            }

            if (result.status > 0) {
                hasHttpResponse = true;
                if (result.status !== 404) {
                    return true;
                }
            }
        }

        return hasHttpResponse;
    }

    function getCourseContext() {
        let path = location.pathname;
        let fullUrl = location.href;

        if (document.referrer) {
            try {
                const ref = new URL(document.referrer);
                if (HOST_RE.test(ref.hostname)) {
                    path = ref.pathname;
                    fullUrl = ref.href;
                }
            } catch (_) {
                // Keep current frame URL.
            }
        }

        const titleNode = document.querySelector('h1, h2, h3');
        const title = textOf(titleNode) || document.title;

        return {
            host: location.host,
            path,
            fullUrl,
            title,
            testKey: hash(location.host + '|' + path)
        };
    }

    function collectSameOriginDocuments(rootDoc, out, seen) {
        if (!rootDoc || seen.has(rootDoc)) {
            return;
        }
        seen.add(rootDoc);
        out.push(rootDoc);

        const frames = rootDoc.querySelectorAll('iframe, frame');
        frames.forEach((frame) => {
            let childDoc = null;
            try {
                childDoc = frame.contentDocument;
            } catch (_) {
                childDoc = null;
            }

            if (childDoc) {
                collectSameOriginDocuments(childDoc, out, seen);
            }
        });
    }

    function getSearchDocuments() {
        if (!isTopFrame) {
            return [document];
        }

        const docs = [];
        collectSameOriginDocuments(document, docs, new Set());
        return docs;
    }

    function getQuestionBlocks() {
        const selectors = [
            '.problems-wrapper[data-problem-id]',
            '.xblock-student_view-problem .problems-wrapper',
            '.xblock-student_view-problem',
            '.problem[data-problem-id]',
            '.problem',
            '.problems-wrapper',
            '[data-problem-id]',
            '[id^="problem_"]',
            '.question'
        ];

        const seen = new WeakSet();
        const result = [];
        const docs = getSearchDocuments();

        docs.forEach((doc) => {
            selectors.forEach((selector) => {
                const nodes = doc.querySelectorAll(selector);
                nodes.forEach((node) => {
                    if (!(node instanceof HTMLElement) || seen.has(node)) {
                        return;
                    }

                    const hasAnswers = Boolean(node.querySelector('label[for], input[type="radio"], input[type="checkbox"]'));
                    if (!hasAnswers) {
                        return;
                    }

                    seen.add(node);
                    result.push(node);
                });
            });
        });

        return result;
    }

    function getQuestionPrompt(root) {
        const labelNode = root.querySelector(
            '.problem-header, .wrapper-problem-response p, .wrapper-problem-response h3, .problem-title, .question-title, legend'
        );
        const prompt = textOf(labelNode);
        if (prompt) {
            return prompt;
        }

        return textOf(root.querySelector('h2, h3, p, legend'));
    }

    function getMarkerText(label, input) {
        const pieces = [
            String(label?.className || ''),
            String(input?.className || ''),
            String(label?.getAttribute?.('aria-label') || ''),
            String(input?.getAttribute?.('aria-label') || ''),
            String(label?.getAttribute?.('data-correct') || ''),
            String(input?.getAttribute?.('data-correct') || ''),
            String(label?.getAttribute?.('data-state') || ''),
            String(input?.getAttribute?.('data-state') || '')
        ];

        const host = label?.closest?.('li, .choicegroup, .answer, .option, .response') || input?.closest?.('li, .choicegroup, .answer, .option, .response');
        if (host) {
            pieces.push(String(host.className || ''));
            pieces.push(String(host.getAttribute('aria-label') || ''));
            pieces.push(String(host.getAttribute('data-state') || ''));
            pieces.push(String(host.getAttribute('data-correct') || ''));
        }

        return pieces.join(' ').toLowerCase();
    }

    function isOptionMarkedCorrect(label, input) {
        const markerText = getMarkerText(label, input);
        if (NEGATIVE_MARK_RE.test(markerText)) {
            return false;
        }

        const explicit = normalizeText(
            String(label?.getAttribute?.('data-correct') || '') + ' ' +
            String(input?.getAttribute?.('data-correct') || '')
        );
        if (explicit.includes('false') || explicit.includes('0') || explicit.includes('no')) {
            return false;
        }
        if (explicit.includes('true') || explicit.includes('1') || explicit.includes('yes')) {
            return true;
        }

        if (POSITIVE_MARK_RE.test(markerText)) {
            return true;
        }

        return false;
    }

    function buildAnswerKey(answerText, input, fallbackIndex) {
        const controlName = input instanceof HTMLInputElement ? input.name || '' : '';
        const controlValue = input instanceof HTMLInputElement ? input.value || '' : '';
        const controlId = input instanceof HTMLInputElement ? input.id || '' : '';
        return hash(controlName + '|' + controlValue + '|' + controlId + '|' + answerText + '|' + String(fallbackIndex));
    }

    function getAnswerOptions(root) {
        const options = [];
        const labels = root.querySelectorAll('label.response-label, label.field-label, .choicegroup label[for], label[for], label');
        const usedKeys = new Set();

        labels.forEach((label, idx) => {
            const inputId = label.getAttribute('for') || '';
            const input = inputId
                ? root.querySelector('#' + escapeSelector(inputId))
                : label.querySelector('input[type="radio"], input[type="checkbox"]');

            const answerText = textOf(label);
            if (!answerText) {
                return;
            }

            const dedupeKey = inputId || answerText;
            if (usedKeys.has(dedupeKey)) {
                return;
            }
            usedKeys.add(dedupeKey);

            options.push({
                answerKey: buildAnswerKey(answerText, input, idx),
                answerText,
                selected: Boolean(input && input.checked),
                correct: isOptionMarkedCorrect(label, input),
                inputId
            });
        });

        if (options.length === 0) {
            const inputs = root.querySelectorAll('input[type="radio"], input[type="checkbox"]');
            inputs.forEach((input, idx) => {
                if (!(input instanceof HTMLInputElement)) {
                    return;
                }

                const inputId = input.id || '';
                const label = inputId
                    ? root.querySelector('label[for="' + escapeSelector(inputId) + '"]')
                    : input.closest('label');
                const answerText = textOf(label);
                if (!answerText) {
                    return;
                }

                options.push({
                    answerKey: buildAnswerKey(answerText, input, idx),
                    answerText,
                    selected: Boolean(input.checked),
                    correct: isOptionMarkedCorrect(label, input),
                    inputId
                });
            });
        }

        return options;
    }

    function isQuestionCorrect(root) {
        const exact = root.querySelector(
            '.status.correct, .feedback-hint-correct, .message .feedback-hint-correct, .problem-status-correct, [data-correct="true"]'
        );
        if (exact) {
            return true;
        }

        const statusNode = root.querySelector('.status, .message, .problem-progress, .notification, .feedback, .problem-results');
        const statusTextRaw = normalizeText(textOf(statusNode));
        if (!statusTextRaw) {
            return false;
        }

        if (NEGATIVE_MARK_RE.test(statusTextRaw)) {
            return false;
        }

        return POSITIVE_MARK_RE.test(statusTextRaw);
    }

    function createEmptyStatsEntry() {
        return {
            completedCount: 0,
            verifiedAnswers: [],
            fallbackAnswers: []
        };
    }

    function normalizeAnswerStatsList(items) {
        if (!Array.isArray(items)) {
            return [];
        }

        const normalized = [];
        items.forEach((item) => {
            const answerText = String(item?.answerText || '').trim();
            if (!answerText) {
                return;
            }

            normalized.push({
                answerKey: typeof item?.answerKey === 'string' ? item.answerKey : '',
                answerText,
                count: Math.max(0, Number(item?.count || 0))
            });
        });

        normalized.sort((a, b) => {
            if (b.count !== a.count) {
                return b.count - a.count;
            }
            return a.answerText.localeCompare(b.answerText);
        });

        return normalized.slice(0, MAX_ANSWERS_PER_QUESTION);
    }

    function buildLocalFallbackStats(questions) {
        const local = {};

        questions.forEach((question) => {
            const selected = question.options
                .filter((option) => option.selected)
                .slice(0, MAX_ANSWERS_PER_QUESTION)
                .map((option) => ({
                    answerKey: option.answerKey,
                    answerText: option.answerText,
                    count: 1
                }));

            if (selected.length === 0) {
                return;
            }

            local[question.questionKey] = {
                completedCount: 0,
                verifiedAnswers: [],
                fallbackAnswers: selected,
                localOnly: true
            };
        });

        return local;
    }

    function mergeStatsByQuestion(remoteStatsByQuestion, localStatsByQuestion, questions) {
        const merged = {};

        questions.forEach((question) => {
            const key = question.questionKey;
            const remote = remoteStatsByQuestion && remoteStatsByQuestion[key]
                ? remoteStatsByQuestion[key]
                : createEmptyStatsEntry();
            const local = localStatsByQuestion && localStatsByQuestion[key]
                ? localStatsByQuestion[key]
                : null;

            const remoteVerified = normalizeAnswerStatsList(remote.verifiedAnswers);
            const remoteFallback = normalizeAnswerStatsList(remote.fallbackAnswers);
            const hasRemoteAnswers = remoteVerified.length > 0 || remoteFallback.length > 0;

            if (hasRemoteAnswers || !local) {
                merged[key] = {
                    completedCount: Number(remote.completedCount || 0),
                    verifiedAnswers: remoteVerified,
                    fallbackAnswers: remoteFallback,
                    localOnly: false
                };
                return;
            }

            merged[key] = {
                completedCount: 0,
                verifiedAnswers: [],
                fallbackAnswers: normalizeAnswerStatsList(local.fallbackAnswers),
                localOnly: true
            };
        });

        return merged;
    }

    function getNodeDepth(node) {
        let depth = 0;
        let current = node;
        while (current && current.parentElement) {
            depth += 1;
            current = current.parentElement;
        }
        return depth;
    }

    function buildQuestionSignature(sourcePath, prompt, options, locationBucket) {
        const normalizedPrompt = normalizeText(prompt);
        const optionSignature = options
            .map((option) => normalizeText(option.answerText))
            .filter(Boolean)
            .sort()
            .join('|');

        return sourcePath + '|' + String(locationBucket) + '|' + normalizedPrompt + '|' + optionSignature;
    }

    function parseQuestions() {
        const blocks = getQuestionBlocks();

        const rawQuestions = blocks.map((root, idx) => {
            const prompt = getQuestionPrompt(root);
            const options = getAnswerOptions(root);
            const questionDomId = root.getAttribute('data-problem-id') || root.getAttribute('id') || ('question-' + idx);
            const sourcePath = root.ownerDocument?.location?.pathname || location.pathname;
            const questionKey = hash(sourcePath + '|' + questionDomId + '|' + prompt);
            const locationBucket = Math.round((root.getBoundingClientRect().top || 0) / 12);
            const signature = buildQuestionSignature(sourcePath, prompt, options, locationBucket);
            const nodeSize = root.querySelectorAll('*').length;
            const nodeDepth = getNodeDepth(root);

            root.setAttribute(QUESTION_KEY_ATTR, questionKey);

            const byStatus = isQuestionCorrect(root);
            const byOptions = options.some((item) => item.correct);

            return {
                questionKey,
                domId: questionDomId,
                domSelector: '[' + QUESTION_KEY_ATTR + '="' + questionKey + '"]',
                ownerDocument: root.ownerDocument || document,
                root,
                prompt,
                correct: byStatus,
                options,
                hasVerifiedAnswer: byStatus || byOptions,
                signature,
                nodeSize,
                nodeDepth,
                sourcePath,
                orderIndex: idx
            };
        }).filter((item) => item.options.length > 0);

        const dedupedBySignature = new Map();
        rawQuestions.forEach((question) => {
            const previous = dedupedBySignature.get(question.signature);
            if (!previous) {
                dedupedBySignature.set(question.signature, question);
                return;
            }

            // Prefer the most specific (deeper and smaller) node to avoid nested duplicate wrappers.
            const currentScore = (question.nodeDepth * 100000) - question.nodeSize;
            const previousScore = (previous.nodeDepth * 100000) - previous.nodeSize;
            if (currentScore > previousScore) {
                dedupedBySignature.set(question.signature, question);
            }
        });

        const deduped = Array.from(dedupedBySignature.values());
        deduped.sort((a, b) => a.orderIndex - b.orderIndex);

        return deduped.map((item) => ({
            questionKey: item.questionKey,
            domId: item.domId,
            domSelector: item.domSelector,
            ownerDocument: item.ownerDocument,
            root: item.root,
            prompt: item.prompt,
            correct: item.correct,
            options: item.options,
            hasVerifiedAnswer: item.hasVerifiedAnswer
        }));
    }

    function isWholePageCompleted(questions) {
        if (questions.length === 0) {
            return false;
        }
        return questions.every((question) => question.correct || question.hasVerifiedAnswer);
    }

    async function pushAttemptSnapshot(questions) {
        const context = getCourseContext();
        const payload = {
            source: 'extension',
            context,
            completed: isWholePageCompleted(questions),
            questions: questions.map((question) => ({
                questionKey: question.questionKey,
                prompt: question.prompt,
                verified: question.hasVerifiedAnswer,
                isCorrect: question.correct,
                answers: question.options.map((option) => ({
                    answerKey: option.answerKey,
                    answerText: option.answerText,
                    selected: option.selected,
                    correct: option.correct
                }))
            }))
        };

        return await postWithRetry('/v1/openedu/attempts', payload, 2);
    }

    async function pullStatistics(questions) {
        const context = getCourseContext();
        return await postWithRetry('/v1/openedu/solutions/query', {
            context,
            questionKeys: questions.map((question) => question.questionKey)
        }, 1);
    }

    function locateQuestionBlock(question) {
        if (question.root instanceof HTMLElement && question.root.isConnected) {
            return question.root;
        }

        const doc = question.ownerDocument || document;

        const byKey = question.domSelector ? doc.querySelector(question.domSelector) : null;
        if (byKey instanceof HTMLElement) {
            return byKey;
        }

        if (question.domId) {
            const byDataId = doc.querySelector('[data-problem-id="' + question.domId.replace(/"/g, '\\"') + '"]');
            if (byDataId instanceof HTMLElement) {
                return byDataId;
            }

            const byId = doc.getElementById(question.domId);
            if (byId instanceof HTMLElement) {
                return byId;
            }
        }

        return null;
    }

    function findInputForOption(block, option) {
        if (option.inputId) {
            const direct = block.querySelector('#' + escapeSelector(option.inputId));
            if (direct instanceof HTMLInputElement) {
                return direct;
            }
        }

        const expectedText = normalizeText(option.answerText);
        if (!expectedText) {
            return null;
        }

        const labels = block.querySelectorAll('label.response-label, label.field-label, .choicegroup label[for], label[for], label');
        for (const label of labels) {
            const normalized = normalizeText(textOf(label));
            if (normalized !== expectedText) {
                continue;
            }

            const inputId = label.getAttribute('for') || '';
            if (inputId) {
                const byId = block.querySelector('#' + escapeSelector(inputId));
                if (byId instanceof HTMLInputElement) {
                    return byId;
                }
            }

            const nested = label.querySelector('input[type="radio"], input[type="checkbox"]');
            if (nested instanceof HTMLInputElement) {
                return nested;
            }
        }

        return null;
    }

    function questionAllowsMultipleAnswers(block) {
        const checkboxes = block.querySelectorAll('input[type="checkbox"]');
        const radios = block.querySelectorAll('input[type="radio"]');
        return checkboxes.length > 0 && radios.length === 0;
    }

    function dispatchInputState(input, checked) {
        if (!(input instanceof HTMLInputElement)) {
            return;
        }

        if (input.checked === checked) {
            return;
        }

        input.checked = checked;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
    }

    function highlightQuestionBlock(block) {
        block.classList.add('paramext-openedu-highlight');
        setTimeout(() => {
            block.classList.remove('paramext-openedu-highlight');
        }, 1600);
    }

    function resolveTargetOptions(options, targetAnswers) {
        const targets = Array.isArray(targetAnswers) ? targetAnswers : [];
        const resolved = [];
        const seen = new Set();

        targets.forEach((target) => {
            const expectedKey = String(target?.answerKey || '').trim();
            const expectedText = normalizeText(target?.answerText || target || '');

            let matched = null;
            if (expectedKey) {
                matched = options.find((option) => option.answerKey === expectedKey) || null;
            }
            if (!matched && expectedText) {
                matched = options.find((option) => normalizeText(option.answerText) === expectedText) || null;
            }
            if (!matched) {
                return;
            }

            const key = matched.answerKey + '|' + normalizeText(matched.answerText);
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            resolved.push(matched);
        });

        return resolved;
    }

    function applyAnswersToQuestion(question, answers, mode) {
        const block = locateQuestionBlock(question);
        if (!block) {
            return false;
        }

        const options = getAnswerOptions(block);
        const targets = resolveTargetOptions(options, answers);
        if (targets.length === 0) {
            return false;
        }

        const multi = questionAllowsMultipleAnswers(block);
        if (!multi) {
            const input = findInputForOption(block, targets[0]);
            if (!(input instanceof HTMLInputElement)) {
                return false;
            }

            input.click();
            input.dispatchEvent(new Event('change', { bubbles: true }));
            highlightQuestionBlock(block);
            return true;
        }

        const selectedInputs = new Set();
        targets.forEach((target) => {
            const input = findInputForOption(block, target);
            if (input instanceof HTMLInputElement && input.type === 'checkbox') {
                selectedInputs.add(input);
            }
        });

        if (selectedInputs.size === 0) {
            return false;
        }

        const modeName = typeof mode === 'string' ? mode : 'add';
        if (modeName === 'set-all') {
            const allCheckboxes = block.querySelectorAll('input[type="checkbox"]');
            allCheckboxes.forEach((input) => {
                if (input instanceof HTMLInputElement) {
                    dispatchInputState(input, selectedInputs.has(input));
                }
            });
        } else {
            selectedInputs.forEach((input) => {
                dispatchInputState(input, true);
            });
        }

        highlightQuestionBlock(block);
        return true;
    }

    function applyAnswerToQuestion(question, answer) {
        return applyAnswersToQuestion(question, [answer], 'add');
    }

    function pickAnswersForUi(stats) {
        const verifiedAnswers = normalizeAnswerStatsList(stats?.verifiedAnswers);
        const fallbackAnswers = normalizeAnswerStatsList(stats?.fallbackAnswers);
        const canUseFallback = Boolean(settings?.openedu?.showFallbackStats);

        if (verifiedAnswers.length > 0) {
            return {
                answers: verifiedAnswers,
                isFallback: false
            };
        }

        if (canUseFallback) {
            return {
                answers: fallbackAnswers,
                isFallback: true
            };
        }

        return {
            answers: [],
            isFallback: false
        };
    }

    function renderInlineWands(statsByQuestion, questions) {
        const activeKeys = new Set();

        const docsForCleanup = getSearchDocuments();
        docsForCleanup.forEach((doc) => {
            const legacyButtons = doc.querySelectorAll('button[' + INLINE_WAND_ATTR + ']');
            legacyButtons.forEach((button) => {
                if (!button.closest('.' + INLINE_MENU_CLASS)) {
                    button.remove();
                }
            });
        });

        questions.forEach((question) => {
            const block = locateQuestionBlock(question);
            if (!block) {
                return;
            }

            const stats = statsByQuestion?.[question.questionKey] || createEmptyStatsEntry();
            const verifiedAnswers = normalizeAnswerStatsList(stats.verifiedAnswers);
            const fallbackAnswers = normalizeAnswerStatsList(stats.fallbackAnswers);
            const isMulti = questionAllowsMultipleAnswers(block);

            let menu = block.querySelector('.' + INLINE_MENU_CLASS + '[' + INLINE_WAND_ATTR + '="' + question.questionKey + '"]');
            if (!(menu instanceof HTMLElement)) {
                menu = document.createElement('span');
                menu.className = INLINE_MENU_CLASS;
                menu.setAttribute(INLINE_WAND_ATTR, question.questionKey);

                const anchor = block.querySelector('.problem-header, .problem-title, .question-title, legend, h3') || block;
                if (anchor.firstChild) {
                    anchor.insertBefore(menu, anchor.firstChild);
                } else {
                    anchor.appendChild(menu);
                }
            }

            menu.innerHTML = '';

            const trigger = document.createElement('button');
            trigger.type = 'button';
            trigger.className = 'paramext-openedu-inline-wand';
            trigger.textContent = verifiedAnswers.length > 0 ? '|*' : '|*?';
            trigger.title = verifiedAnswers.length > 0
                ? 'Открыть список проверенных ответов и статистики'
                : 'Открыть статистику ответов';

            const popover = document.createElement('div');
            popover.className = 'paramext-openedu-inline-popover';

            const popTitle = document.createElement('div');
            popTitle.className = 'paramext-openedu-inline-title';
            popTitle.textContent = 'paramEXT';
            popover.appendChild(popTitle);

            const applyVerified = document.createElement('button');
            applyVerified.type = 'button';
            applyVerified.className = 'paramext-openedu-inline-action';
            applyVerified.textContent = isMulti ? 'Вставить правильные ответы' : 'Вставить правильный ответ';
            applyVerified.disabled = verifiedAnswers.length === 0;
            applyVerified.addEventListener('click', () => {
                const payload = isMulti ? verifiedAnswers : [verifiedAnswers[0]];
                const mode = isMulti ? 'set-all' : 'add';
                const applied = applyAnswersToQuestion(question, payload, mode);
                if (!applied) {
                    maybeLogBackendIssue('openedu_apply_failed', {
                        questionKey: question.questionKey,
                        mode,
                        source: 'verified'
                    });
                }
            });
            popover.appendChild(applyVerified);

            if (settings.openedu.showFallbackStats) {
                const applyFallback = document.createElement('button');
                applyFallback.type = 'button';
                applyFallback.className = 'paramext-openedu-inline-action fallback';
                applyFallback.textContent = isMulti ? 'Вставить вероятные ответы' : 'Вставить вероятный ответ';
                applyFallback.disabled = fallbackAnswers.length === 0;
                applyFallback.addEventListener('click', () => {
                    const payload = isMulti ? fallbackAnswers : [fallbackAnswers[0]];
                    const mode = isMulti ? 'set-all' : 'add';
                    const applied = applyAnswersToQuestion(question, payload, mode);
                    if (!applied) {
                        maybeLogBackendIssue('openedu_apply_failed', {
                            questionKey: question.questionKey,
                            mode,
                            source: 'fallback'
                        });
                    }
                });
                popover.appendChild(applyFallback);
            }

            const list = document.createElement('ul');
            list.className = 'paramext-openedu-inline-stats';

            function appendAnswersSection(sectionTitle, answers, isFallbackSection) {
                if (!answers || answers.length === 0) {
                    return;
                }

                const sectionHeader = document.createElement('li');
                sectionHeader.className = 'paramext-openedu-inline-section';
                sectionHeader.textContent = sectionTitle;
                list.appendChild(sectionHeader);

                answers.slice(0, MAX_ANSWERS_PER_QUESTION).forEach((answer) => {
                    const row = document.createElement('li');
                    row.className = 'paramext-openedu-inline-row';

                    const answerBtn = document.createElement('button');
                    answerBtn.type = 'button';
                    answerBtn.className = 'paramext-openedu-inline-answer';
                    answerBtn.textContent = answer.answerText;
                    answerBtn.title = 'Вставить этот вариант';
                    answerBtn.addEventListener('click', () => {
                        const applied = applyAnswersToQuestion(question, [answer], isMulti ? 'add' : 'add');
                        if (!applied) {
                            maybeLogBackendIssue('openedu_apply_failed', {
                                questionKey: question.questionKey,
                                answerText: answer.answerText,
                                answerKey: answer.answerKey || '',
                                source: isFallbackSection ? 'fallback-item' : 'verified-item'
                            });
                        }
                    });

                    const count = document.createElement('span');
                    count.className = 'paramext-openedu-inline-count' + (isFallbackSection ? ' fallback' : '');
                    count.textContent = String(answer.count || 0);

                    row.appendChild(answerBtn);
                    row.appendChild(count);
                    list.appendChild(row);
                });
            }

            appendAnswersSection('Проверенные', verifiedAnswers, false);
            if (settings.openedu.showFallbackStats) {
                appendAnswersSection('Вероятные', fallbackAnswers, true);
            }

            if (list.children.length === 0) {
                const empty = document.createElement('li');
                empty.className = 'paramext-openedu-inline-empty';
                empty.textContent = 'Нет статистики по этому вопросу.';
                list.appendChild(empty);
            }

            popover.appendChild(list);

            menu.appendChild(trigger);
            menu.appendChild(popover);

            activeKeys.add(question.questionKey);
        });

        const docs = getSearchDocuments();
        docs.forEach((doc) => {
            const stale = doc.querySelectorAll('.' + INLINE_MENU_CLASS + '[' + INLINE_WAND_ATTR + ']');
            stale.forEach((node) => {
                const key = node.getAttribute(INLINE_WAND_ATTR) || '';
                if (!activeKeys.has(key)) {
                    node.remove();
                }
            });
        });
    }

    function setStickOnline(isOnline, detail) {
        if (!statusDot || !statusText) {
            return;
        }

        statusDot.classList.toggle('online', isOnline);
        statusText.textContent = detail || (isOnline ? 'API доступен' : 'API недоступен');
    }

    function renderStick(statsByQuestion, questions) {
        if (!stickBody) {
            return;
        }

        stickBody.innerHTML = '';

        if (!statsByQuestion || Object.keys(statsByQuestion).length === 0) {
            const emptyState = document.createElement('div');
            emptyState.className = 'paramext-stick-empty';
            emptyState.textContent = 'Статистика появится после первого прохождения.';
            stickBody.appendChild(emptyState);
            return;
        }

        questions.forEach((question, index) => {
            const stats = statsByQuestion[question.questionKey];
            if (!stats) {
                return;
            }

            const card = document.createElement('div');
            card.className = 'paramext-question-card';

            const head = document.createElement('div');
            head.className = 'paramext-question-head';

            const title = document.createElement('p');
            title.className = 'paramext-question-name';
            title.textContent = 'Вопрос ' + (index + 1);

            const meta = document.createElement('p');
            meta.className = 'paramext-question-meta';
            const completedCount = Number(stats.completedCount || 0);
            if (completedCount > 0) {
                meta.textContent = 'завершений: ' + completedCount;
            } else if (stats.localOnly) {
                meta.textContent = 'локальные ответы';
            } else {
                meta.textContent = 'ожидание данных';
            }

            head.appendChild(title);
            head.appendChild(meta);
            card.appendChild(head);

            const list = document.createElement('ul');
            list.className = 'paramext-answer-list';

            const picked = pickAnswersForUi(stats);
            if (picked.answers.length === 0) {
                const emptyItem = document.createElement('li');
                emptyItem.className = 'paramext-answer-item';
                emptyItem.textContent = 'Пока нет подтвержденных ответов.';
                list.appendChild(emptyItem);
            }

            picked.answers.forEach((answer) => {
                const item = document.createElement('li');
                item.className = 'paramext-answer-item';

                const text = document.createElement('span');
                text.className = 'paramext-answer-text';
                text.textContent = answer.answerText;

                const count = document.createElement('span');
                count.className = 'paramext-answer-count' + (picked.isFallback ? ' fallback' : '');
                count.textContent = String(answer.count || 0);

                item.appendChild(text);
                item.appendChild(count);
                list.appendChild(item);
            });

            card.appendChild(list);

            const topAnswer = picked.answers.length > 0 ? picked.answers[0] : null;
            const controls = document.createElement('div');
            controls.className = 'paramext-question-controls';
            const applyBtn = document.createElement('button');
            applyBtn.className = 'paramext-apply-btn';
            applyBtn.textContent = picked.isFallback ? 'Применить (резерв)' : 'Применить лучший';
            applyBtn.disabled = !topAnswer;
            applyBtn.addEventListener('click', () => {
                if (!topAnswer) {
                    return;
                }

                const applied = applyAnswerToQuestion(question, topAnswer);
                if (!applied) {
                    maybeLogBackendIssue('openedu_apply_failed', {
                        questionKey: question.questionKey,
                        answerText: topAnswer.answerText,
                        answerKey: topAnswer.answerKey || ''
                    });
                }
            });
            controls.appendChild(applyBtn);
            card.appendChild(controls);

            stickBody.appendChild(card);
        });
    }

    function toggleStick(forceState) {
        if (!stickRoot || !wandToggle) {
            return;
        }

        if (typeof forceState === 'boolean') {
            panelVisible = forceState;
        } else {
            panelVisible = !panelVisible;
        }

        stickRoot.classList.toggle('hidden', !panelVisible);
        wandToggle.classList.toggle('active', panelVisible);
    }

    function ensureStickUi() {
        if (!isTopFrame) {
            return;
        }

        if (stickRoot && wandToggle) {
            return;
        }

        const staleStick = document.getElementById(STICK_ID);
        if (staleStick) {
            staleStick.remove();
        }

        const staleToggle = document.getElementById(WAND_TOGGLE_ID);
        if (staleToggle) {
            staleToggle.remove();
        }

        wandToggle = document.createElement('button');
        wandToggle.id = WAND_TOGGLE_ID;
        wandToggle.type = 'button';
        wandToggle.className = 'paramext-openedu-wand-toggle';
        wandToggle.textContent = '|*';
        wandToggle.title = 'paramEXT OpenEdu: показать статистику';
        wandToggle.addEventListener('click', () => {
            toggleStick();
        });

        stickRoot = document.createElement('aside');
        stickRoot.id = STICK_ID;
        stickRoot.className = 'paramext-openedu-stick hidden';

        const header = document.createElement('div');
        header.className = 'paramext-stick-header';

        const left = document.createElement('div');
        const title = document.createElement('div');
        title.className = 'paramext-stick-title';
        title.textContent = 'paramEXT OpenEdu';
        const subtitle = document.createElement('div');
        subtitle.className = 'paramext-stick-subtitle';
        subtitle.textContent = 'Палочка и проверенные ответы';
        left.appendChild(title);
        left.appendChild(subtitle);

        const actions = document.createElement('div');
        actions.className = 'paramext-stick-actions';

        statusDot = document.createElement('span');
        statusDot.className = 'paramext-stick-status';

        statusText = document.createElement('span');
        statusText.className = 'paramext-stick-subtitle';
        statusText.textContent = 'API недоступен';

        const hideButton = document.createElement('button');
        hideButton.className = 'paramext-stick-button';
        hideButton.type = 'button';
        hideButton.textContent = 'Скрыть';
        hideButton.addEventListener('click', () => {
            toggleStick(false);
        });

        actions.appendChild(statusDot);
        actions.appendChild(statusText);
        actions.appendChild(hideButton);

        header.appendChild(left);
        header.appendChild(actions);

        stickBody = document.createElement('div');
        stickBody.className = 'paramext-stick-content';

        stickRoot.appendChild(header);
        stickRoot.appendChild(stickBody);

        document.documentElement.appendChild(wandToggle);
        document.documentElement.appendChild(stickRoot);
    }

    async function runStickCycle() {
        if (cycleInFlight) {
            return;
        }

        cycleInFlight = true;
        try {
            const questions = parseQuestions();
            if (questions.length === 0) {
                renderInlineWands({}, []);

                if (isTopFrame) {
                    const online = await probeBackendOnline();
                    setStickOnline(online, online ? 'API доступен' : 'API недоступен');
                    renderStick(null, []);
                }
                return;
            }

            const pushResult = await pushAttemptSnapshot(questions);
            const statsResult = await pullStatistics(questions);

            const statsByQuestion = statsResult.ok && statsResult.data && typeof statsResult.data === 'object'
                ? statsResult.data.statsByQuestion || null
                : null;

            const localFallbackStats = buildLocalFallbackStats(questions);
            const mergedStatsByQuestion = mergeStatsByQuestion(statsByQuestion, localFallbackStats, questions);

            renderInlineWands(mergedStatsByQuestion, questions);

            if (isTopFrame) {
                if (pushResult.ok || statsResult.ok) {
                    setStickOnline(true, 'API доступен');
                } else {
                    const pushErr = describeRequestError(pushResult);
                    const statsErr = describeRequestError(statsResult);
                    const errText = [pushErr, statsErr].filter(Boolean).join(' / ');
                    const probeOnline = await probeBackendOnline();
                    if (probeOnline) {
                        setStickOnline(true, 'API ок, sync: ' + (errText || 'ошибка'));
                    } else {
                        setStickOnline(false, 'API недоступен: ' + (errText || 'network'));
                    }
                }

                renderStick(mergedStatsByQuestion, questions);
            }
        } finally {
            cycleInFlight = false;
        }
    }

    function isAutoAdvanceEnabled() {
        return settings.openedu.autoAdvanceEnabled || settings.openedu.mode === 'autoSolve';
    }

    function maybeClickNextOnSequencePage() {
        if (!isTopFrame) {
            return;
        }

        const tabsHost = document.querySelector('.sequence-navigation-tabs');
        if (!tabsHost) {
            return;
        }

        const activeTab = tabsHost.querySelector('button.active');
        if (!activeTab) {
            return;
        }

        const isComplete = activeTab.classList.contains('complete');
        if (!isComplete && settings.openedu.activeTabRefreshEnabled) {
            activeTab.click();
            return;
        }

        if (!isComplete && settings.openedu.requiredCompletionOnly) {
            return;
        }

        const now = Date.now();
        const delayMs = Number(settings.openedu.autoAdvanceDelayMs || 1800);
        if (now - lastAutoAdvanceAt < delayMs) {
            return;
        }

        const nextButton = document.querySelector('.next-btn:not([disabled]), .next-button:not([disabled])');
        if (!nextButton) {
            return;
        }

        lastAutoAdvanceAt = now;
        nextButton.click();
    }

    function installKeyboardToggle() {
        if (!isTopFrame) {
            return;
        }

        document.addEventListener('keydown', (event) => {
            if (event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement) {
                return;
            }

            if (window.ParamExtSettings.hotkeyMatches(event, settings.openedu.stickHotkey)) {
                event.preventDefault();
                toggleStick();
            }
        });
    }

    function installStorageSync() {
        chrome.storage.onChanged.addListener(async (changes, areaName) => {
            if (areaName !== 'local') {
                return;
            }

            if (!Object.prototype.hasOwnProperty.call(changes, window.ParamExtSettings.STORAGE_KEY)) {
                return;
            }

            settings = await window.ParamExtSettings.getSettings();
            runStickCycle();
        });
    }

    async function boot() {
        settings = await window.ParamExtSettings.getSettings();

        if (window.ParamExtTelemetry) {
            window.ParamExtTelemetry.push('system_state', {
                activePlatform: settings.activePlatform,
                mode: settings.openedu.mode,
                autoAdvanceEnabled: settings.openedu.autoAdvanceEnabled,
                locationHost: location.host,
                frame: isTopFrame ? 'top' : 'iframe'
            }, 'openedu-content');
        }

        ensureStickUi();
        installKeyboardToggle();
        installStorageSync();

        if (isTopFrame) {
            if (!normalizeApiBaseUrl()) {
                setStickOnline(false, 'Не указан API URL');
            } else {
                setStickOnline(await probeBackendOnline());
            }
        }

        runStickCycle();
        setInterval(() => {
            runStickCycle();
        }, CYCLE_INTERVAL_MS);

        if (isTopFrame) {
            setInterval(() => {
                if (isAutoAdvanceEnabled()) {
                    maybeClickNextOnSequencePage();
                }
            }, 3000);
        }
    }

    boot();
})();
