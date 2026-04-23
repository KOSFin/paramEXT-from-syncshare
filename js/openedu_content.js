(function () {
    const HOST_RE = /(^|\.)openedu\.ru$/i;
    const STICK_ID = 'paramext-openedu-stick';
    const WAND_TOGGLE_ID = 'paramext-openedu-wand-toggle';
    const QUESTION_KEY_ATTR = 'data-paramext-openedu-question-key';
    const INLINE_WAND_ATTR = 'data-paramext-openedu-inline-wand';
    const INLINE_MENU_CLASS = 'paramext-openedu-inline-menu';
    const WAND_VISIBILITY_KEY = 'paramExtOpeneduWandsHidden';
    const QUESTION_INPUT_SELECTOR = 'input[type="radio"], input[type="checkbox"], input[type="text"]';
    const QUESTION_ROOT_SELECTOR = '[data-problem-id], .problem, .xblock-student_view-problem, .problems-wrapper, .wrapper-problem-response, fieldset, [role="group"], .choicegroup, [id^="problem_"]';
    const QUESTION_GROUP_SELECTOR = 'fieldset, .question, .subquestion, .problem-question, .wrapper-problem-response, .choicegroup, .answers, .options, .response, .answer';
    const OPTION_LABEL_SELECTOR = 'label.response-label, label.field-label, .choicegroup label[for], label[for], label';
    const MAX_ANSWERS_PER_QUESTION = 50;
    const RETRY_DELAYS_MS = [0, 350, 900];
    const MIN_CYCLE_GAP_MS = 10000;
    const MAX_CONSECUTIVE_FAILURES = 7;
    const AUTH_FAILURE_COOLDOWN_MS = 120000;
    const QUERY_COOLDOWN_MS = 25000;
    const PUSH_COOLDOWN_MS = 15000;
    const API_SYNC_MIN_GAP_MS = 8000;
    const ACTIVE_TAB_REFRESH_MIN_GAP_MS = 45000;
    const ACTIVE_TAB_REFRESH_AFTER_SUBMIT_WINDOW_MS = 15000;
    const BACKEND_LOG_THROTTLE_MS = 30000;
    const CONTENT_FALLBACK_BLOCK_MS = 90000;
    const TRANSIENT_EMPTY_QUESTIONS_GRACE_MS = 9000;
    const BOOTSTRAP_SYNC_DELAYS_MS = [1800, 5200];
    const POST_SUBMIT_SYNC_DELAYS_MS = [2500, 6500];
    const MESSAGE_TRIGGER_THROTTLE_MS = 3000;
    const MUTATION_TRIGGER_THROTTLE_MS = 3000;
    const DEBUG_SYNC_STORAGE_KEY = 'paramExtOpeneduDebug';
    const PARTICIPANT_KEY_STORAGE = 'paramExtOpeneduParticipantKey';

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
    const openeduShared = window.ParamExtOpeneduShared || {};
    const DEBUG_SYNC_ENABLED = (() => {
        try {
            const raw = localStorage.getItem(DEBUG_SYNC_STORAGE_KEY);
            if (raw === null || raw === '') {
                return false;
            }
            return /^(1|true|on|yes)$/i.test(String(raw).trim());
        } catch (_) {
            return false;
        }
    })();

    let settings = null;
    let stickRoot = null;
    let stickBody = null;
    let wandToggle = null;
    let statusDot = null;
    let statusText = null;
    let lastAutoAdvanceAt = 0;
    let lastActiveTabRefreshAt = 0;
    let lastSubmitActionAt = 0;
    let cycleInFlight = false;
    let lastCycleAt = 0;
    let consecutiveCycleFailures = 0;
    let cyclesStopped = false;
    let panelVisible = false;
    let wandsHidden = false;
    let syncBlockedUntil = 0;
    let syncBlockedReason = '';
    let lastBackendIssueAt = 0;
    let lastBackendIssueSignature = '';
    let lastAttemptPayloadHash = '';
    let lastAttemptPushAt = 0;
    let lastNetworkSyncAt = 0;
    let lastStatsQuerySignature = '';
    let lastStatsQueryAt = 0;
    let lastStatsResponse = null;
    let scheduledCycleTimer = 0;
    let scheduledCycleForce = false;
    let scheduledCycleAllowNetwork = false;
    let contentFallbackBlockedUntil = 0;
    let contentFallbackBlockedReason = '';
    let participantKeyCache = '';
    let lastMergedStatsByQuestion = null;
    let lastMessageTriggerAt = 0;
    let lastMutationTriggerAt = 0;
    let lastMeaningfulQuestionsAt = 0;

    let iframeQuestionsCache = [];
    let topFrameIframeQuestions = null;
    let topFrameIframeStats = null;
    let topFrameOnlineState = { online: false, text: 'Wait...' };
    window.__PARAMEXT_TOPFRAME_ONLINE_STATE = topFrameOnlineState;
    let _topContextPromise = null;

    function debugSync(event, payload) {
        if (!DEBUG_SYNC_ENABLED) {
            return;
        }

        try {
            console.log('[paramEXT OpenEdu][' + (isTopFrame ? 'top' : 'iframe') + '] ' + event, payload || {});
        } catch (_) {
            // Ignore console errors.
        }
    }

    function summarizeQuestionsForDebug(questions) {
        return (Array.isArray(questions) ? questions : []).map((question) => ({
            questionKey: question.questionKey,
            prompt: String(question.prompt || '').slice(0, 160),
            isCorrect: Boolean(question.correct),
            hasVerifiedAnswer: Boolean(question.hasVerifiedAnswer),
            selectedAnswers: (Array.isArray(question.options) ? question.options : [])
                .filter((option) => option.selected)
                .map((option) => ({
                    answerKey: option.answerKey,
                    answerText: option.answerText,
                    selected: Boolean(option.selected),
                    markedCorrect: Boolean(option.correct)
                })),
            markedCorrectAnswers: (Array.isArray(question.options) ? question.options : [])
                .filter((option) => option.correct)
                .map((option) => ({
                    answerKey: option.answerKey,
                    answerText: option.answerText
                }))
        }));
    }

    function requestTopContext() {
        if (isTopFrame) return Promise.resolve(null);
        if (window.__PARAMEXT_TOP_CONTEXT) return Promise.resolve(window.__PARAMEXT_TOP_CONTEXT);
        if (_topContextPromise) return _topContextPromise;
        _topContextPromise = new Promise(resolve => {
            let handled = false;
            const listener = (event) => {
                if (event.data && event.data.type === 'PARAMEXT_OPENEDU_CONTEXT_REPLY') {
                    window.removeEventListener('message', listener);
                    window.__PARAMEXT_TOP_CONTEXT = event.data.context;
                    handled = true;
                    resolve(event.data.context);
                }
            };
            window.addEventListener('message', listener);
            try { window.top.postMessage({ type: 'PARAMEXT_OPENEDU_CONTEXT_REQUEST' }, '*'); } catch (e) {}
            setTimeout(() => {
                if (!handled) {
                    window.removeEventListener('message', listener);
                    resolve(null);
                }
            }, 1500);
        });
        return _topContextPromise;
    }

    window.addEventListener('message', (event) => {
        if (!event.data || typeof event.data.type !== 'string') return;

        if (isTopFrame) {
            if (event.data.type === 'PARAMEXT_OPENEDU_CONTEXT_REQUEST') {
                try {
                    event.source.postMessage({
                        type: 'PARAMEXT_OPENEDU_CONTEXT_REPLY',
                        context: getCourseContext(true)
                    }, '*');
                } catch (e) {}
            } else if (event.data.type === 'PARAMEXT_OPENEDU_QUESTIONS_SYNC') {
                topFrameIframeStats = event.data.stats;
                topFrameIframeQuestions = event.data.questions;
                debugSync('top_received_iframe_sync', {
                    questionCount: Array.isArray(topFrameIframeQuestions) ? topFrameIframeQuestions.length : 0,
                    statKeys: topFrameIframeStats && typeof topFrameIframeStats === 'object' ? Object.keys(topFrameIframeStats).length : 0
                });
                renderStick(topFrameIframeStats, topFrameIframeQuestions);
            } else if (event.data.type === 'PARAMEXT_OPENEDU_STICK_ONLINE') {
                topFrameOnlineState = { online: Boolean(event.data.online), text: String(event.data.text || '') };
                window.__PARAMEXT_TOPFRAME_ONLINE_STATE = topFrameOnlineState;
                debugSync('top_received_iframe_online', topFrameOnlineState);
                setStickOnline(topFrameOnlineState.online, topFrameOnlineState.text);
            }
        } else if (event.data.type === 'PARAMEXT_APPLY_ANSWERS' || event.data.type === 'PARAMEXT_APPLY_ANSWER') {
            const reference = event.data.question || {
                questionKey: event.data.questionKey,
                domId: event.data.domId || '',
                prompt: event.data.prompt || ''
            };
            const answers = event.data.type === 'PARAMEXT_APPLY_ANSWER'
                ? [event.data.answer]
                : (Array.isArray(event.data.answers) ? event.data.answers : []);
            const mode = typeof event.data.mode === 'string' ? event.data.mode : 'add';

            debugSync('iframe_apply_answers_command', {
                questionKey: reference?.questionKey || '',
                answerCount: answers.length,
                mode
            });

            let question = findQuestionByReference(iframeQuestionsCache, reference);
            if (!question) {
                iframeQuestionsCache = parseQuestions();
                question = findQuestionByReference(iframeQuestionsCache, reference);
            }

            if (question) {
                applyAnswersToQuestion(question, answers, mode);
                return;
            }

            broadcastApplyMessageToChildFrames(event.data);
        }
    });

    function textOf(node) {
        return (node && node.textContent ? node.textContent : '').replace(/\s+/g, ' ').trim();
    }

    function collapseWhitespace(value) {
        if (typeof openeduShared.collapseWhitespace === 'function') {
            return openeduShared.collapseWhitespace(value);
        }
        return String(value || '').replace(/\s+/g, ' ').trim();
    }

    function normalizeText(value) {
        if (typeof openeduShared.normalizeText === 'function') {
            return openeduShared.normalizeText(value);
        }
        return collapseWhitespace(value).toLowerCase();
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

    function buildStableQuestionKeyBase(payload) {
        if (typeof openeduShared.buildStableQuestionKeyBase === 'function') {
            return openeduShared.buildStableQuestionKeyBase(payload);
        }

        const sourcePath = String(payload?.sourcePath || '').trim();
        const prompt = String(payload?.prompt || '').trim();
        return 'q2_' + hash(sourcePath + '|' + prompt);
    }

    function delay(ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    }

    function getParticipantKey() {
        if (participantKeyCache) {
            return participantKeyCache;
        }

        try {
            const existing = String(localStorage.getItem(PARTICIPANT_KEY_STORAGE) || '').trim();
            if (existing) {
                participantKeyCache = existing;
                return participantKeyCache;
            }

            const generated = 'p_' + hash(
                location.host + '|' +
                (navigator.userAgent || '') + '|' +
                String(Date.now()) + '|' +
                String(Math.random())
            );
            localStorage.setItem(PARTICIPANT_KEY_STORAGE, generated);
            participantKeyCache = generated;
            return participantKeyCache;
        } catch (_) {
            participantKeyCache = 'p_' + hash(location.host + '|' + String(Date.now()));
            return participantKeyCache;
        }
    }

    function canUseContentFallback() {
        return Date.now() >= contentFallbackBlockedUntil;
    }

    function blockContentFallback(reason) {
        contentFallbackBlockedUntil = Date.now() + CONTENT_FALLBACK_BLOCK_MS;
        contentFallbackBlockedReason = String(reason || 'content_fallback_blocked');
        debugSync('content_fallback_blocked', {
            reason: contentFallbackBlockedReason,
            blockedUntil: contentFallbackBlockedUntil
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
            headers['X-API-Token'] = token;
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
            debugSync('http_skip_no_api_base_url', { method, path });
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

        debugSync('http_request', {
            method,
            path,
            url: request.url,
            timeoutMs,
            hasBody: body !== null,
            bodyBytes: request.body ? request.body.length : 0
        });

        let bgStatus0Hint = '';
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

                if (result.status === 0) {
                    bgStatus0Hint = result.error || String(bgResponse.responseType || 'status_0');
                    debugSync('http_background_status_0_fallback', {
                        method,
                        path,
                        status: result.status,
                        error: result.error,
                        responseType: bgResponse.responseType || '',
                        errorName: bgResponse.errorName || '',
                        isTimeout: Boolean(bgResponse.isTimeout)
                    });

                    if (!canUseContentFallback()) {
                        return {
                            ok: false,
                            status: 0,
                            error: 'background_status_0: ' + bgStatus0Hint + ' | content_blocked=' + contentFallbackBlockedReason,
                            data: null
                        };
                    }
                } else {
                    if (logErrors) {
                        maybeLogBackendIssue('openedu_backend_error', {
                            method,
                            path,
                            status: result.status,
                            error: result.error,
                            via: 'background'
                        });
                    }
                    debugSync('http_response', {
                        method,
                        path,
                        via: 'background',
                        ok: false,
                        status: result.status,
                        error: result.error
                    });
                    return result;
                }
            }

            if (bgResponse.ok) {
                debugSync('http_response', {
                    method,
                    path,
                    via: 'background',
                    ok: true,
                    status: Number(bgResponse.status || 200)
                });
                return {
                    ok: true,
                    status: Number(bgResponse.status || 200),
                    error: '',
                    data: bgResponse.json && typeof bgResponse.json === 'object' ? bgResponse.json : null
                };
            }
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
                let contentError = errorMessageFromPayload(data) || text || ('http_' + String(response.status || 0));
                if (Number(response.status || 0) === 0 && bgStatus0Hint) {
                    contentError = 'status_0_content | bg=' + bgStatus0Hint;
                }

                const result = {
                    ok: false,
                    status: Number(response.status || 0),
                    error: contentError,
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
                debugSync('http_response', {
                    method,
                    path,
                    via: 'content',
                    ok: false,
                    status: result.status,
                    error: result.error,
                    backgroundHint: bgStatus0Hint
                });

                if (result.status === 0 && bgStatus0Hint) {
                    blockContentFallback(result.error || bgStatus0Hint);
                }
                return result;
            }

            debugSync('http_response', {
                method,
                path,
                via: 'content',
                ok: true,
                status: Number(response.status || 200)
            });
            contentFallbackBlockedUntil = 0;
            contentFallbackBlockedReason = '';
            return {
                ok: true,
                status: Number(response.status || 200),
                error: '',
                data
            };
        } catch (error) {
            const rawMessage = error && error.message ? String(error.message) : '';
            const fallbackMessage = controller.signal.aborted ? 'request_timeout' : 'network_error';
            const message = rawMessage || fallbackMessage;
            const combinedMessage = bgStatus0Hint ? (message + ' | bg=' + bgStatus0Hint) : message;
            const result = {
                ok: false,
                status: 0,
                error: combinedMessage,
                data: null
            };

            if (logErrors) {
                maybeLogBackendIssue('openedu_backend_error', {
                    method,
                    path,
                    status: 0,
                    error: combinedMessage,
                    via: 'content'
                });
            }

            debugSync('http_response', {
                method,
                path,
                via: 'content',
                ok: false,
                status: 0,
                error: combinedMessage,
                backgroundHint: bgStatus0Hint
            });

            if (bgStatus0Hint) {
                blockContentFallback(combinedMessage || bgStatus0Hint);
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

        if (isSyncBlocked()) {
            return {
                ok: false,
                status: 0,
                error: syncBlockedReason || 'sync_blocked',
                data: null
            };
        }

        for (let attempt = 0; attempt <= retries; attempt += 1) {
            if (attempt > 0) {
                const delayMs = RETRY_DELAYS_MS[Math.min(attempt, RETRY_DELAYS_MS.length - 1)] || 300;
                await delay(delayMs);
            }

            last = await requestJson('POST', path, body, true);
            if (last.ok) {
                return last;
            }

            if (last.status === 401 || last.status === 403) {
                blockSync('auth_' + String(last.status), AUTH_FAILURE_COOLDOWN_MS);
                break;
            }

            if (last.status === 0 && String(last.error || '').includes('background_status_0')) {
                // Persistent background transport failures are not fixed by immediate retries.
                break;
            }

            if (last.status >= 400 && last.status < 500 && last.status !== 429) {
                break;
            }
        }

        return last;
    }

    function blockSync(reason, durationMs) {
        syncBlockedReason = String(reason || 'sync_blocked');
        syncBlockedUntil = Date.now() + Math.max(5000, Number(durationMs || AUTH_FAILURE_COOLDOWN_MS));
    }

    function clearSyncBlock() {
        syncBlockedUntil = 0;
        syncBlockedReason = '';
    }

    function isSyncBlocked() {
        return syncBlockedUntil > Date.now();
    }

    function applyWandsVisibilityToDocument(doc, visible) {
        if (!doc || !doc.documentElement) {
            return;
        }
        doc.documentElement.classList.toggle('paramext-openedu-hide-wands', !visible);
    }

    async function persistWandsVisibility(value) {
        try {
            await chrome.storage.local.set({ [WAND_VISIBILITY_KEY]: Boolean(value) });
        } catch (_) {
            // Ignore persistence errors.
        }
    }

    function setWandsHidden(hidden, persist) {
        wandsHidden = Boolean(hidden);
        const visible = !wandsHidden;

        const docs = getSearchDocuments();
        docs.forEach((doc) => {
            applyWandsVisibilityToDocument(doc, visible);
        });
        applyWandsVisibilityToDocument(document, visible);

        if (!visible) {
            panelVisible = false;
            if (stickRoot) {
                stickRoot.classList.add('hidden');
            }
            if (wandToggle) {
                wandToggle.classList.remove('active');
            }
        }

        if (persist) {
            persistWandsVisibility(wandsHidden);
        }
    }

    async function loadWandsHiddenState() {
        try {
            const payload = await chrome.storage.local.get(WAND_VISIBILITY_KEY);
            return Boolean(payload && payload[WAND_VISIBILITY_KEY]);
        } catch (_) {
            return false;
        }
    }

    function scheduleCycle(force, source, options) {
        const allowNetwork = options?.allowNetwork !== false;
        if (cyclesStopped || scheduledCycleTimer) {
            scheduledCycleForce = scheduledCycleForce || Boolean(force);
            scheduledCycleAllowNetwork = scheduledCycleAllowNetwork || allowNetwork;
            return;
        }

        const now = Date.now();
        const reason = String(source || 'generic');
        if (!force) {
            if (reason === 'message') {
                if (now - lastMessageTriggerAt < MESSAGE_TRIGGER_THROTTLE_MS) {
                    return;
                }
                lastMessageTriggerAt = now;
            }

            if (reason === 'mutation') {
                if (now - lastMutationTriggerAt < MUTATION_TRIGGER_THROTTLE_MS) {
                    return;
                }
                lastMutationTriggerAt = now;
            }
        }

        scheduledCycleForce = scheduledCycleForce || Boolean(force);
        scheduledCycleAllowNetwork = scheduledCycleAllowNetwork || allowNetwork;

        scheduledCycleTimer = setTimeout(() => {
            scheduledCycleTimer = 0;
            const runForce = scheduledCycleForce;
            const runAllowNetwork = scheduledCycleAllowNetwork;
            scheduledCycleForce = false;
            scheduledCycleAllowNetwork = false;
            runStickCycle(Boolean(runForce), { source: reason, allowNetwork: runAllowNetwork });
        }, 350);
    }

    function quickRerender() {
        if (!lastMergedStatsByQuestion) {
            return;
        }
        const questions = parseQuestions();
        if (questions.length === 0) {
            return;
        }
        iframeQuestionsCache = questions;
        renderInlineWands(lastMergedStatsByQuestion, questions);
    }

    function shouldHandleDomRefreshTrigger() {
        const now = Date.now();
        return lastMeaningfulQuestionsAt === 0
            || (now - lastMeaningfulQuestionsAt) <= TRANSIENT_EMPTY_QUESTIONS_GRACE_MS
            || (now - lastSubmitActionAt) <= ACTIVE_TAB_REFRESH_AFTER_SUBMIT_WINDOW_MS;
    }

    function scheduleBootstrapSyncs() {
        BOOTSTRAP_SYNC_DELAYS_MS.forEach((delayMs) => {
            setTimeout(() => {
                if (cyclesStopped) {
                    return;
                }

                if (lastMeaningfulQuestionsAt === 0 || !lastStatsResponse) {
                    scheduleCycle(true, 'bootstrap', { allowNetwork: true });
                }
            }, delayMs);
        });
    }

    function schedulePostSubmitSyncs() {
        POST_SUBMIT_SYNC_DELAYS_MS.forEach((delayMs) => {
            setTimeout(() => {
                scheduleCycle(true, 'submit-delay', { allowNetwork: true });
            }, delayMs);
        });
    }

    function describeRequestError(result) {
        if (!result || result.ok) {
            return '';
        }

        if (result.error === 'auth_401' || result.error === 'auth_403') {
            return result.error === 'auth_401' ? '401 (токен)' : '403 (доступ)';
        }

        if (result.error === 'sync_blocked') {
            return 'sync блокирован';
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

    function getCourseContext(forceTop = false) {
        if (!forceTop && !isTopFrame && window.__PARAMEXT_TOP_CONTEXT) {
            return window.__PARAMEXT_TOP_CONTEXT;
        }

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
            testKey: hash(location.host + '|' + path),
            participantKey: getParticipantKey()
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

    function isQuestionRootCandidate(root) {
        if (!(root instanceof HTMLElement)) {
            return false;
        }

        const controlCount = root.querySelectorAll(QUESTION_INPUT_SELECTOR).length;
        if (controlCount === 0) {
            return false;
        }

        if (root.querySelector('legend, .problem-header, .problem-group-label, .problem-title, .question-title, .choicegroup, .wrapper-problem-response')) {
            return true;
        }

        return root.querySelectorAll(OPTION_LABEL_SELECTOR).length > 0;
    }

    function findQuestionRoot(control) {
        if (!(control instanceof HTMLInputElement)) {
            return null;
        }

        let current = control;
        while (current && current !== current.ownerDocument.documentElement) {
            if (current instanceof HTMLElement && current.matches(QUESTION_ROOT_SELECTOR) && isQuestionRootCandidate(current)) {
                return current;
            }
            current = current.parentElement;
        }

        return null;
    }

    function collectOptionMediaDescriptors(node) {
        if (!(node instanceof Element)) {
            return [];
        }

        const descriptors = [];
        const mediaNodes = node.querySelectorAll('img, source, video, audio');
        mediaNodes.forEach((mediaNode) => {
            if (!(mediaNode instanceof Element)) {
                return;
            }

            const tag = mediaNode.tagName.toLowerCase();
            const source = collapseWhitespace(
                mediaNode.getAttribute('src')
                || mediaNode.getAttribute('srcset')
                || mediaNode.getAttribute('data-src')
                || mediaNode.getAttribute('poster')
                || ''
            );

            descriptors.push({
                kind: tag,
                src: source,
                alt: collapseWhitespace(mediaNode.getAttribute('alt') || ''),
                title: collapseWhitespace(mediaNode.getAttribute('title') || ''),
                ariaLabel: collapseWhitespace(mediaNode.getAttribute('aria-label') || '')
            });
        });

        return descriptors;
    }

    function getOptionAnswerText(label, input) {
        const rawText = label instanceof HTMLElement ? textOf(label) : '';
        if (typeof openeduShared.deriveOptionAnswerText === 'function') {
            return openeduShared.deriveOptionAnswerText({
                text: rawText,
                ariaLabel: collapseWhitespace(label?.getAttribute?.('aria-label') || ''),
                title: collapseWhitespace(label?.getAttribute?.('title') || ''),
                inputValue: input instanceof HTMLInputElement ? collapseWhitespace(input.value || '') : '',
                mediaDescriptors: collectOptionMediaDescriptors(label)
            });
        }

        return rawText;
    }

    function getQuestionBlocks() {
        const seen = new WeakSet();
        const result = [];
        const docs = getSearchDocuments();

        docs.forEach((doc) => {
            const controls = doc.querySelectorAll(QUESTION_INPUT_SELECTOR);
            controls.forEach((control) => {
                if (!(control instanceof HTMLInputElement)) {
                    return;
                }

                const root = findQuestionRoot(control);
                if (!(root instanceof HTMLElement) || seen.has(root)) {
                    return;
                }

                if (!root.querySelector(OPTION_LABEL_SELECTOR + ', ' + QUESTION_INPUT_SELECTOR)) {
                    return;
                }

                seen.add(root);
                result.push(root);
            });
        });

        return result;
    }

    function getQuestionPrompt(root) {
        const labelNode = root.querySelector(
            '.problem-header, .problem-group-label, .wrapper-problem-response p, .wrapper-problem-response h3, .problem-title, .question-title, legend'
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

        const host = label?.closest?.('li, .answer, .option, .response') || input?.closest?.('li, .answer, .option, .response');
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

        // Check negative markers first — choicegroup_incorrect on labels, or
        // standalone "incorrect"/"wrong" etc. in class names / attributes.
        if (NEGATIVE_MARK_RE.test(markerText)) {
            return false;
        }

        // Check aria-describedby status element.  In edX/OpenEdu all labels
        // inside a choicegroup share the same aria-describedby pointing to a
        // single status span (e.g. <span class="status correct">).  When the
        // status is shared we still check it — the status reflects the
        // question-level result which is valid for the selected option.
        const statusRef = String(label?.getAttribute?.('aria-describedby') || input?.getAttribute?.('aria-describedby') || '').trim();
        if (statusRef) {
            const ownerDocument = input?.ownerDocument || label?.ownerDocument || document;
            const statusNode = ownerDocument.getElementById(statusRef);
            if (statusNode) {
                const statusClass = String(statusNode.className || '').toLowerCase();
                const statusNodeText = normalizeText(textOf(statusNode));
                if (statusClass.includes('incorrect') || NEGATIVE_MARK_RE.test(statusNodeText)) {
                    return false;
                }
                if (statusClass.includes('correct') || POSITIVE_MARK_RE.test(statusNodeText)) {
                    // For shared status: only the selected option counts as correct.
                    if (input instanceof HTMLInputElement && input.type !== 'text' && !input.checked) {
                        return false;
                    }
                    return true;
                }
            }
        }

        // Check explicit data-correct attributes.
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

        // Final fallback: check positive markers in class names / attributes
        // (e.g. choicegroup_correct on the label itself).
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

    function buildElementPath(root, element) {
        if (!(root instanceof Element) || !(element instanceof Element)) {
            return '';
        }

        const parts = [];
        let current = element;
        while (current && current !== root) {
            const parent = current.parentElement;
            if (!parent) {
                break;
            }
            const index = Array.prototype.indexOf.call(parent.children, current);
            parts.push(String(index));
            current = parent;
        }

        return current === root ? parts.reverse().join('.') : '';
    }

    function getElementByPath(root, path) {
        if (!(root instanceof Element) || !path) {
            return root instanceof HTMLElement ? root : null;
        }

        let current = root;
        const parts = String(path).split('.');
        for (let i = 0; i < parts.length; i += 1) {
            const idx = Number(parts[i]);
            if (!Number.isInteger(idx) || idx < 0 || idx >= current.children.length) {
                return null;
            }
            current = current.children[idx];
        }

        return current instanceof HTMLElement ? current : null;
    }

    function isTopQuestionWrapper(node) {
        if (!(node instanceof Element)) {
            return false;
        }
        return node.matches(QUESTION_ROOT_SELECTOR);
    }

    function getInputGroupContainer(root, input) {
        if (!(root instanceof HTMLElement) || !(input instanceof HTMLInputElement)) {
            return root;
        }

        let current = input.parentElement;
        while (current && current !== root) {
            if (
                !isTopQuestionWrapper(current)
                && current.matches(QUESTION_GROUP_SELECTOR)
            ) {
                return current;
            }
            current = current.parentElement;
        }

        return root;
    }

    function findPromptBeforeNode(root, node) {
        if (!(root instanceof HTMLElement) || !(node instanceof Element)) {
            return '';
        }

        let cursor = node;
        while (cursor && cursor !== root) {
            let previous = cursor.previousElementSibling;
            while (previous) {
                const direct = textOf(previous);
                if (direct && direct.length >= 8) {
                    return direct;
                }

                const nested = textOf(previous.querySelector('h1, h2, h3, h4, legend, .problem-title, .question-title, .problem-header, p'));
                if (nested && nested.length >= 8) {
                    return nested;
                }

                previous = previous.previousElementSibling;
            }
            cursor = cursor.parentElement;
        }

        return '';
    }

    function resolveGroupRootByInput(root, inputPath, inputName, expectedCount) {
        if (!(root instanceof HTMLElement)) {
            return root;
        }

        const input = getElementByPath(root, inputPath);
        if (!(input instanceof HTMLInputElement)) {
            return root;
        }

        let current = input.parentElement;
        while (current && current !== root) {
            const allInputs = current.querySelectorAll(QUESTION_INPUT_SELECTOR).length;
            if (allInputs < expectedCount) {
                current = current.parentElement;
                continue;
            }

            if (inputName) {
                const scopedSameName = current.querySelectorAll('input[name="' + escapeSelector(inputName) + '"]').length;
                if (scopedSameName === expectedCount) {
                    return current;
                }
            }

            if (allInputs === expectedCount) {
                return current;
            }

            current = current.parentElement;
        }

        return root;
    }

    function getAnswerOptions(root) {
        const options = [];
        const labels = root.querySelectorAll(OPTION_LABEL_SELECTOR);
        const usedKeys = new Set();

        labels.forEach((label, idx) => {
            const inputId = label.getAttribute('for') || '';
            const input = inputId
                ? root.querySelector('#' + escapeSelector(inputId))
                : label.querySelector('input[type="radio"], input[type="checkbox"]');

            // Skip labels paired with text inputs — handled separately below.
            if (input instanceof HTMLInputElement && input.type === 'text') {
                return;
            }

            const groupContainer = getInputGroupContainer(root, input);
            const groupPath = groupContainer && groupContainer !== root ? buildElementPath(root, groupContainer) : '';
            const inputName = input instanceof HTMLInputElement ? String(input.name || '').trim() : '';
            const groupKey = groupPath
                ? ('c:' + groupPath)
                : (inputName ? ('n:' + inputName) : ('i:' + String(idx)));

            const answerText = getOptionAnswerText(label, input);
            if (!answerText) {
                return;
            }

            const dedupeKey = groupKey + '|' + (inputId || answerText);
            if (usedKeys.has(dedupeKey)) {
                return;
            }
            usedKeys.add(dedupeKey);

            options.push({
                answerKey: buildAnswerKey(answerText, input, idx),
                answerText,
                selected: Boolean(input && input.checked),
                correct: isOptionMarkedCorrect(label, input),
                inputId,
                inputName,
                groupKey,
                groupPath,
                inputPath: input instanceof HTMLInputElement ? buildElementPath(root, input) : ''
            });
        });

        // Text inputs: each input produces one option with the typed value.
        const textInputs = root.querySelectorAll('input[type="text"]');
        textInputs.forEach((input, tidx) => {
            if (!(input instanceof HTMLInputElement)) {
                return;
            }

            const inputId = input.id || '';
            const inputName = String(input.name || '').trim();

            // Skip if already captured by the label loop.
            const alreadyCaptured = options.some((o) => o.inputId === inputId && inputId);
            if (alreadyCaptured) {
                return;
            }

            const label = inputId
                ? root.querySelector('label[for="' + escapeSelector(inputId) + '"]')
                : null;

            const answerText = input.value.trim();
            const groupContainer = getInputGroupContainer(root, input);
            const groupPath = groupContainer && groupContainer !== root ? buildElementPath(root, groupContainer) : '';
            const groupKey = groupPath
                ? ('c:' + groupPath)
                : (inputName ? ('n:' + inputName) : ('t:' + String(tidx)));

            const dedupeKey = groupKey + '|' + inputId;
            if (usedKeys.has(dedupeKey)) {
                return;
            }
            usedKeys.add(dedupeKey);

            options.push({
                answerKey: buildAnswerKey(answerText, input, tidx),
                answerText,
                selected: answerText.length > 0,
                correct: isOptionMarkedCorrect(label, input),
                inputId,
                inputName,
                groupKey,
                groupPath,
                inputPath: buildElementPath(root, input),
                inputType: 'text'
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
                const groupContainer = getInputGroupContainer(root, input);
                const groupPath = groupContainer && groupContainer !== root ? buildElementPath(root, groupContainer) : '';
                const inputName = String(input.name || '').trim();
                const groupKey = groupPath
                    ? ('c:' + groupPath)
                    : (inputName ? ('n:' + inputName) : ('i:' + String(idx)));
                const answerText = getOptionAnswerText(label, input);
                if (!answerText) {
                    return;
                }

                options.push({
                    answerKey: buildAnswerKey(answerText, input, idx),
                    answerText,
                    selected: Boolean(input.checked),
                    correct: isOptionMarkedCorrect(label, input),
                    inputId,
                    inputName,
                    groupKey,
                    groupPath,
                    inputPath: buildElementPath(root, input)
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
                    localOnly: false,
                    similarMatch: Boolean(remote.similarMatch),
                    matchedBy: typeof remote.matchedBy === 'string'
                        ? remote.matchedBy
                        : (Boolean(remote.similarMatch) ? 'similar' : 'exact'),
                    matchedQuestionKey: typeof remote.matchedQuestionKey === 'string' ? remote.matchedQuestionKey : '',
                    matchedScore: Math.max(0, Number(remote.matchedScore || 0))
                };
                return;
            }

            merged[key] = {
                completedCount: 0,
                verifiedAnswers: [],
                fallbackAnswers: normalizeAnswerStatsList(local.fallbackAnswers),
                localOnly: true,
                similarMatch: false,
                matchedBy: 'local',
                matchedQuestionKey: '',
                matchedScore: 0
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

    function buildQuestionSignature(sourcePath, prompt, options, locationBucket, groupIdentity) {
        const normalizedPrompt = normalizeText(prompt);
        const optionSignature = options
            .map((option) => normalizeText(option.answerText))
            .filter(Boolean)
            .sort()
            .join('|');

        return sourcePath + '|' + String(locationBucket) + '|' + String(groupIdentity || '') + '|' + normalizedPrompt + '|' + optionSignature;
    }

    function parseQuestions() {
        const blocks = getQuestionBlocks();

        const rawQuestions = [];

        blocks.forEach((root, idx) => {
            const options = getAnswerOptions(root);
            if (options.length === 0) {
                return;
            }

            const sourcePath = root.ownerDocument?.location?.pathname || location.pathname;
            const baseDomId = root.getAttribute('data-problem-id') || root.getAttribute('id') || ('question-' + idx);
            const fallbackPrompt = getQuestionPrompt(root);

            const grouped = new Map();
            options.forEach((option, optionIndex) => {
                const key = option.groupKey || ('g:' + String(optionIndex));
                if (!grouped.has(key)) {
                    grouped.set(key, []);
                }
                grouped.get(key).push(option);
            });

            const groups = Array.from(grouped.entries());
            groups.forEach(([groupId, groupOptions], groupIndex) => {
                const first = groupOptions[0] || null;
                let groupRoot = first?.groupPath
                    ? (getElementByPath(root, first.groupPath) || root)
                    : root;
                if (groupRoot === root && groups.length > 1) {
                    groupRoot = resolveGroupRootByInput(
                        root,
                        first?.inputPath || '',
                        first?.inputName || '',
                        groupOptions.length,
                    );
                }
                const nearPrompt = findPromptBeforeNode(root, groupRoot);
                const prompt = getQuestionPrompt(groupRoot) || nearPrompt || fallbackPrompt;

                const scopedDomId = baseDomId + '::' + String(groupId || groupIndex);
                const locationBucket = Math.round(((groupRoot.getBoundingClientRect().top || root.getBoundingClientRect().top || 0)) / 12);
                const signature = buildQuestionSignature(sourcePath, prompt, groupOptions, locationBucket, groupId);
                const nodeSize = groupRoot.querySelectorAll('*').length;
                const nodeDepth = getNodeDepth(groupRoot);
                const allowsMultipleAnswers = questionAllowsMultipleAnswers(groupRoot);
                const stableAnswerTexts = groupOptions
                    .filter((option) => option.inputType !== 'text')
                    .map((option) => String(option.answerText || '').trim())
                    .filter(Boolean);
                const textInputCount = groupOptions.filter((option) => option.inputType === 'text').length;
                const questionKeyBase = buildStableQuestionKeyBase({
                    sourcePath,
                    prompt,
                    answerTexts: stableAnswerTexts,
                    choiceCount: groupOptions.length,
                    textInputCount,
                    allowsMultipleAnswers
                });

                const byStatus = isQuestionCorrect(groupRoot);
                const byOptions = groupOptions.some((item) => item.correct);

                rawQuestions.push({
                    questionKey: '',
                    questionKeyBase,
                    domId: scopedDomId,
                    domSelector: '',
                    ownerDocument: groupRoot.ownerDocument || document,
                    root: groupRoot,
                    prompt,
                    correct: byStatus || byOptions,
                    options: groupOptions,
                    allowsMultipleAnswers,
                    hasVerifiedAnswer: byStatus || byOptions,
                    signature,
                    nodeSize,
                    nodeDepth,
                    sourcePath,
                    orderIndex: (idx * 100) + groupIndex
                });
            });
        });

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
        const duplicateIndexByBase = new Map();

        deduped.forEach((item) => {
            const occurrenceIndex = duplicateIndexByBase.get(item.questionKeyBase) || 0;
            duplicateIndexByBase.set(item.questionKeyBase, occurrenceIndex + 1);

            item.questionKey = occurrenceIndex === 0
                ? item.questionKeyBase
                : (item.questionKeyBase + '_' + String(occurrenceIndex + 1));
            item.domSelector = '[' + QUESTION_KEY_ATTR + '="' + item.questionKey + '"]';

            if (item.root instanceof Element) {
                item.root.setAttribute(QUESTION_KEY_ATTR, item.questionKey);
            }
        });

        return deduped.map((item) => ({
            questionKey: item.questionKey,
            domId: item.domId,
            domSelector: item.domSelector,
            ownerDocument: item.ownerDocument,
            root: item.root,
            prompt: item.prompt,
            correct: item.correct,
            options: item.options,
            allowsMultipleAnswers: item.allowsMultipleAnswers,
            hasVerifiedAnswer: item.hasVerifiedAnswer,
            orderIndex: item.orderIndex
        }));
    }

    function isWholePageCompleted(questions) {
        if (questions.length === 0) {
            return false;
        }
        return questions.every((question) => question.correct);
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

        debugSync('push_attempt_snapshot_payload', {
            context,
            completed: payload.completed,
            questionCount: payload.questions.length,
            questions: summarizeQuestionsForDebug(questions)
        });

        const result = await postWithRetry('/v1/openedu/attempts', payload, 2);
        debugSync('push_attempt_snapshot_result', {
            ok: result.ok,
            status: result.status,
            error: result.error || ''
        });
        return result;
    }

    async function pullStatistics(questions) {
        const context = getCourseContext();
        const queryPayload = {
            context,
            questionKeys: questions.map((question) => question.questionKey),
            questions: questions.map((question) => ({
                questionKey: question.questionKey,
                prompt: question.prompt,
                answers: question.options
                    .map((option) => String(option.answerText || '').trim())
                    .filter(Boolean)
            }))
        };

        debugSync('pull_statistics_payload', {
            context,
            questionCount: queryPayload.questionKeys.length,
            questionKeys: queryPayload.questionKeys
        });

        const result = await postWithRetry('/v1/openedu/solutions/query', queryPayload, 1);
        const statsByQuestion = result?.data?.statsByQuestion;
        const statsKeys = statsByQuestion && typeof statsByQuestion === 'object' ? Object.keys(statsByQuestion) : [];
        const nonEmptyStatsKeys = statsKeys.filter((key) => {
            const entry = statsByQuestion?.[key];
            const verifiedCount = Array.isArray(entry?.verifiedAnswers) ? entry.verifiedAnswers.length : 0;
            const fallbackCount = Array.isArray(entry?.fallbackAnswers) ? entry.fallbackAnswers.length : 0;
            return verifiedCount > 0 || fallbackCount > 0;
        });
        debugSync('pull_statistics_result', {
            ok: result.ok,
            status: result.status,
            error: result.error || '',
            statsKeys: statsKeys.length,
            nonEmptyStatsKeys: nonEmptyStatsKeys.length
        });
        return result;
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

    function matchesQuestionReference(candidate, reference) {
        if (typeof openeduShared.matchesQuestionReference === 'function') {
            return openeduShared.matchesQuestionReference(candidate, reference);
        }

        return String(candidate?.questionKey || '') === String(reference?.questionKey || '');
    }

    function findQuestionByReference(questions, reference) {
        const list = Array.isArray(questions) ? questions : [];
        return list.find((question) => matchesQuestionReference(question, reference)) || null;
    }

    function broadcastApplyMessageToChildFrames(payload) {
        let posted = false;
        const frames = document.querySelectorAll('iframe, frame');
        frames.forEach((frame) => {
            try {
                if (frame.contentWindow) {
                    frame.contentWindow.postMessage(payload, '*');
                    posted = true;
                }
            } catch (_) {
                // Ignore inaccessible child frames.
            }
        });
        return posted;
    }

    function requestApplyAnswers(question, answers, mode) {
        if (!question) {
            return false;
        }

        if (isTopFrame && question.fromIframe) {
            return broadcastApplyMessageToChildFrames({
                type: 'PARAMEXT_APPLY_ANSWERS',
                question: {
                    questionKey: question.questionKey,
                    domId: question.domId,
                    prompt: question.prompt,
                    options: Array.isArray(question.options) ? question.options : []
                },
                answers: Array.isArray(answers) ? answers : [],
                mode: typeof mode === 'string' ? mode : 'add'
            });
        }

        return applyAnswersToQuestion(question, answers, mode);
    }

    function findInputForOption(block, option) {
        if (option.inputId) {
            const direct = block.querySelector('#' + escapeSelector(option.inputId));
            if (direct instanceof HTMLInputElement) {
                return direct;
            }
        }

        // For text inputs, label-text matching doesn't apply
        // (the label is the prompt, not the answer text).
        if (option.inputType === 'text') {
            if (option.inputName) {
                const byName = block.querySelector('input[type="text"][name="' + escapeSelector(option.inputName) + '"]');
                if (byName instanceof HTMLInputElement) {
                    return byName;
                }
            }
            return null;
        }

        const expectedText = normalizeText(option.answerText);
        if (!expectedText) {
            return null;
        }

        const labels = block.querySelectorAll(OPTION_LABEL_SELECTOR);
        for (const label of labels) {
            const normalized = normalizeText(getOptionAnswerText(label, null));
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
        if (wandsHidden) {
            return;
        }
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
            debugSync('apply_answers_failed', {
                reason: 'question_block_not_found',
                questionKey: question?.questionKey || ''
            });
            return false;
        }

        // Text input questions: fill the field directly instead of
        // going through the radio/checkbox resolve logic.
        const textInput = block.querySelector('input[type="text"]');
        if (textInput instanceof HTMLInputElement) {
            const targetText = String(
                (Array.isArray(answers) ? answers[0] : answers)?.answerText
                || (Array.isArray(answers) ? answers[0] : answers)
                || ''
            ).trim();
            if (!targetText) {
                debugSync('apply_answers_failed', {
                    reason: 'text_input_empty_target',
                    questionKey: question?.questionKey || ''
                });
                return false;
            }

            const nativeSetter = Object.getOwnPropertyDescriptor(
                HTMLInputElement.prototype, 'value'
            )?.set;
            if (nativeSetter) {
                nativeSetter.call(textInput, targetText);
            } else {
                textInput.value = targetText;
            }
            textInput.dispatchEvent(new Event('input', { bubbles: true }));
            textInput.dispatchEvent(new Event('change', { bubbles: true }));
            highlightQuestionBlock(block);
            debugSync('apply_answers_success', {
                questionKey: question?.questionKey || '',
                mode: 'text',
                answerText: targetText
            });
            return true;
        }

        const options = getAnswerOptions(block);
        const targets = resolveTargetOptions(options, answers);
        if (targets.length === 0) {
            debugSync('apply_answers_failed', {
                reason: 'target_answers_not_resolved',
                questionKey: question?.questionKey || '',
                requestedAnswers: Array.isArray(answers) ? answers.map((item) => ({
                    answerKey: item?.answerKey || '',
                    answerText: item?.answerText || item || ''
                })) : []
            });
            return false;
        }

        const multi = questionAllowsMultipleAnswers(block);
        if (!multi) {
            const input = findInputForOption(block, targets[0]);
            if (!(input instanceof HTMLInputElement)) {
                debugSync('apply_answers_failed', {
                    reason: 'input_not_found_single',
                    questionKey: question?.questionKey || '',
                    target: {
                        answerKey: targets[0]?.answerKey || '',
                        answerText: targets[0]?.answerText || ''
                    }
                });
                return false;
            }

            input.click();
            input.dispatchEvent(new Event('change', { bubbles: true }));
            highlightQuestionBlock(block);
            debugSync('apply_answers_success', {
                questionKey: question?.questionKey || '',
                mode: 'single',
                selected: [{
                    answerKey: targets[0]?.answerKey || '',
                    answerText: targets[0]?.answerText || ''
                }]
            });
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
            debugSync('apply_answers_failed', {
                reason: 'no_checkbox_inputs_resolved',
                questionKey: question?.questionKey || ''
            });
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
        debugSync('apply_answers_success', {
            questionKey: question?.questionKey || '',
            mode: modeName,
            selected: targets.map((item) => ({
                answerKey: item.answerKey,
                answerText: item.answerText
            }))
        });
        return true;
    }

    function applyAnswerToQuestion(question, answer) {
        return applyAnswersToQuestion(question, [answer], 'add');
    }

    function mergeAndSortAnswers(verifiedAnswers, fallbackAnswers) {
        const map = new Map();

        (verifiedAnswers || []).forEach((ans) => {
            const sig = ans.answerKey + '|' + normalizeText(ans.answerText);
            map.set(sig, {
                answerKey: ans.answerKey,
                answerText: ans.answerText,
                verifiedCount: ans.count || 0,
                fallbackCount: 0,
                isVerified: true
            });
        });

        (fallbackAnswers || []).forEach((ans) => {
            const sig = ans.answerKey + '|' + normalizeText(ans.answerText);
            if (map.has(sig)) {
                map.get(sig).fallbackCount = ans.count || 0;
            } else {
                map.set(sig, {
                    answerKey: ans.answerKey,
                    answerText: ans.answerText,
                    verifiedCount: 0,
                    fallbackCount: ans.count || 0,
                    isVerified: false
                });
            }
        });

        const merged = Array.from(map.values());
        merged.sort((a, b) => {
            if (b.fallbackCount !== a.fallbackCount) return b.fallbackCount - a.fallbackCount;
            if (b.verifiedCount !== a.verifiedCount) return b.verifiedCount - a.verifiedCount;
            if (a.isVerified !== b.isVerified) return a.isVerified ? -1 : 1;
            return a.answerText.localeCompare(b.answerText);
        });
        return merged;
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

            const matchKind = String(stats.matchedBy || (stats.similarMatch ? 'similar' : 'exact'));
            const isSimilar = matchKind === 'similar' || matchKind === 'content';

            const trigger = document.createElement('button');
            trigger.type = 'button';
            trigger.className = 'paramext-openedu-inline-wand' + (isSimilar ? ' paramext-openedu-inline-wand--similar' : '');
            trigger.textContent = verifiedAnswers.length > 0
                ? (isSimilar ? '|*~' : '|*')
                : (isSimilar ? '|*?~' : '|*?');
            trigger.title = verifiedAnswers.length > 0
                ? 'Открыть список проверенных ответов и статистики'
                : 'Открыть статистику ответов';

            if (isSimilar) {
                trigger.title = 'Статистика получена из похожего вопроса';
            }

            const popover = document.createElement('div');
            popover.className = 'paramext-openedu-inline-popover';

            const popTitle = document.createElement('div');
            popTitle.className = 'paramext-openedu-inline-title';
            popTitle.textContent = 'paramEXT';
            popover.appendChild(popTitle);

            let actionsHost = popover;
            if (isSimilar) {
                const similarNotice = document.createElement('div');
                similarNotice.className = 'paramext-openedu-inline-similar-notice';
                similarNotice.textContent = 'Точный ответ для этого вопроса не найден. Показаны данные похожего вопроса.';
                popover.appendChild(similarNotice);

                const tabs = document.createElement('div');
                tabs.className = 'paramext-openedu-inline-tabs';

                const thisQuestionTab = document.createElement('button');
                thisQuestionTab.type = 'button';
                thisQuestionTab.className = 'paramext-openedu-inline-tab';
                thisQuestionTab.textContent = 'Этот вопрос';

                const similarQuestionTab = document.createElement('button');
                similarQuestionTab.type = 'button';
                similarQuestionTab.className = 'paramext-openedu-inline-tab active';
                similarQuestionTab.textContent = 'Похожий вопрос';

                tabs.appendChild(thisQuestionTab);
                tabs.appendChild(similarQuestionTab);
                popover.appendChild(tabs);

                const thisPane = document.createElement('div');
                thisPane.className = 'paramext-openedu-inline-tab-pane';
                thisPane.style.display = 'none';
                thisPane.textContent = 'Для этого вопроса пока нет своей статистики.';

                const similarPane = document.createElement('div');
                similarPane.className = 'paramext-openedu-inline-tab-pane';

                thisQuestionTab.addEventListener('click', () => {
                    thisQuestionTab.classList.add('active');
                    similarQuestionTab.classList.remove('active');
                    thisPane.style.display = '';
                    similarPane.style.display = 'none';
                });

                similarQuestionTab.addEventListener('click', () => {
                    similarQuestionTab.classList.add('active');
                    thisQuestionTab.classList.remove('active');
                    similarPane.style.display = '';
                    thisPane.style.display = 'none';
                });

                popover.appendChild(thisPane);
                popover.appendChild(similarPane);
                actionsHost = similarPane;
            }

            const applyVerified = document.createElement('button');
            applyVerified.type = 'button';
            applyVerified.className = 'paramext-openedu-inline-action';
            applyVerified.textContent = isMulti
                ? (isSimilar ? 'Вставить ответы похожего вопроса' : 'Вставить правильные ответы')
                : (isSimilar ? 'Вставить ответ похожего вопроса' : 'Вставить правильный ответ');
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
            actionsHost.appendChild(applyVerified);

            if (settings.openedu.showFallbackStats) {
                const applyFallback = document.createElement('button');
                applyFallback.type = 'button';
                applyFallback.className = 'paramext-openedu-inline-action fallback';
                applyFallback.textContent = isMulti
                    ? (isSimilar ? 'Вставить популярные ответы похожего вопроса' : 'Вставить популярные ответы')
                    : (isSimilar ? 'Вставить популярный ответ похожего вопроса' : 'Вставить популярный ответ');
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
                actionsHost.appendChild(applyFallback);
            }

            const list = document.createElement('ul');
            list.className = 'paramext-openedu-inline-stats';

            const allAnswers = mergeAndSortAnswers(verifiedAnswers, fallbackAnswers);

            if (allAnswers.length === 0) {
                const empty = document.createElement('li');
                empty.className = 'paramext-openedu-inline-empty';
                empty.textContent = 'Нет статистики по этому вопросу.';
                list.appendChild(empty);
            } else {
                const sectionHeader = document.createElement('li');
                sectionHeader.className = 'paramext-openedu-inline-section';
                sectionHeader.textContent = 'Ответы';
                list.appendChild(sectionHeader);

                allAnswers.forEach((answer) => {
                    const row = document.createElement('li');
                    row.className = 'paramext-openedu-inline-row';

                    const answerBtn = document.createElement('button');
                    answerBtn.type = 'button';
                    answerBtn.className = 'paramext-openedu-inline-answer';
                    answerBtn.textContent = answer.isVerified ? (answer.answerText + ' ✓') : answer.answerText;
                    answerBtn.title = 'Вставить этот вариант';
                    answerBtn.addEventListener('click', () => {
                        const applied = applyAnswersToQuestion(question, [answer], 'add');
                        if (!applied) {
                            maybeLogBackendIssue('openedu_apply_failed', {
                                questionKey: question.questionKey,
                                answerText: answer.answerText,
                                answerKey: answer.answerKey || ''
                            });
                        }
                    });
                    
                    row.appendChild(answerBtn);

                    const countsContainer = document.createElement('div');
                    countsContainer.style.display = 'flex';
                    countsContainer.style.gap = '4px';
                    countsContainer.style.marginLeft = '8px';

                    if (answer.isVerified) {
                        const vCount = document.createElement('span');
                        vCount.className = 'paramext-openedu-inline-count verified';
                        vCount.textContent = answer.verifiedCount > 0 ? answer.verifiedCount : '✓';
                        vCount.title = answer.verifiedCount > 0
                            ? 'Подтверждено платформой: ' + answer.verifiedCount + ' раз'
                            : 'Ответ подтверждён платформой';
                        countsContainer.appendChild(vCount);
                    }

                    const fCount = document.createElement('span');
                    fCount.className = 'paramext-openedu-inline-count fallback';
                    fCount.textContent = answer.fallbackCount;
                    fCount.title = 'Выбирали: ' + answer.fallbackCount + ' раз';
                    countsContainer.appendChild(fCount);

                    row.appendChild(countsContainer);
                    list.appendChild(row);
                });
            }

            actionsHost.appendChild(list);

            menu.appendChild(trigger);
            if (isSimilar) {
                const sourceMark = document.createElement('span');
                sourceMark.className = 'paramext-openedu-inline-source-mark';
                sourceMark.textContent = 'похож.';
                sourceMark.title = 'Данные не из этого вопроса, а из похожего';
                menu.appendChild(sourceMark);
            }
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

    function buildQuestionCard(question, index, stats) {
        const card = document.createElement('div');
        card.className = 'paramext-question-card';

        const head = document.createElement('div');
        head.className = 'paramext-question-head';

        const titleWrap = document.createElement('div');
        titleWrap.className = 'paramext-question-title-wrap';

        const title = document.createElement('p');
        title.className = 'paramext-question-name';
        title.textContent = 'Вопрос ' + (index + 1);
        titleWrap.appendChild(title);

        const matchedBy = String(stats.matchedBy || (stats.similarMatch ? 'similar' : 'exact'));
        if (matchedBy === 'similar' || matchedBy === 'content') {
            const titleBadge = document.createElement('span');
            titleBadge.className = 'paramext-question-source-badge';
            titleBadge.textContent = matchedBy === 'content' ? 'по содержанию' : 'похожий';
            titleBadge.title = 'Статистика взята из похожего вопроса';
            titleWrap.appendChild(titleBadge);
        }

        const meta = document.createElement('p');
        meta.className = 'paramext-question-meta';
        const completedCount = Number(stats.completedCount || 0);
        if (matchedBy === 'similar') {
            const score = Math.round(Math.max(0, Number(stats.matchedScore || 0)) * 100);
            const scoreText = score > 0 ? ' | совпадение: ' + score + '%' : '';
            meta.textContent = 'похожий вопрос' + scoreText + (completedCount > 0 ? ' | завершений: ' + completedCount : '');
            meta.classList.add('paramext-question-meta--similar');
        } else if (matchedBy === 'content') {
            meta.textContent = 'этот вопрос (по содержанию)' + (completedCount > 0 ? ' | завершений: ' + completedCount : '');
        } else if (completedCount > 0) {
            meta.textContent = 'завершений: ' + completedCount;
        } else if (stats.localOnly) {
            meta.textContent = 'локальные ответы';
        } else {
            meta.textContent = 'ожидание данных';
        }

        head.appendChild(titleWrap);
        head.appendChild(meta);
        card.appendChild(head);

        const list = document.createElement('ul');
        list.className = 'paramext-answer-list';

        const verifiedAnswers = normalizeAnswerStatsList(stats.verifiedAnswers);
        const selectedAnswers = normalizeAnswerStatsList(stats.fallbackAnswers);
        const allAnswers = mergeAndSortAnswers(verifiedAnswers, selectedAnswers);

        if (allAnswers.length === 0) {
            const emptyItem = document.createElement('li');
            emptyItem.className = 'paramext-answer-item';
            emptyItem.textContent = 'Пока нет ответов.';
            list.appendChild(emptyItem);
        }

        allAnswers.forEach((answer) => {
            const item = document.createElement('li');
            item.className = 'paramext-answer-item';

            const text = document.createElement('span');
            text.className = 'paramext-answer-text';
            text.textContent = answer.isVerified ? (answer.answerText + ' ✓') : answer.answerText;
            item.appendChild(text);

            const countsContainer = document.createElement('div');
            countsContainer.style.display = 'flex';
            countsContainer.style.gap = '4px';

            if (answer.isVerified) {
                const vCount = document.createElement('span');
                vCount.className = 'paramext-answer-count verified';
                vCount.textContent = answer.verifiedCount > 0
                    ? answer.verifiedCount + ' подтв.'
                    : 'подтв.';
                countsContainer.appendChild(vCount);
            }

            const fCount = document.createElement('span');
            fCount.className = 'paramext-answer-count fallback';
            fCount.textContent = answer.fallbackCount + ' отв.';
            countsContainer.appendChild(fCount);

            item.appendChild(countsContainer);
            list.appendChild(item);
        });

        card.appendChild(list);

        const topAnswer = allAnswers[0] || null;
        const isMulti = Boolean(question?.allowsMultipleAnswers);
        const controls = document.createElement('div');
        controls.className = 'paramext-question-controls';
        const applyBtn = document.createElement('button');
        applyBtn.className = 'paramext-apply-btn';
        applyBtn.textContent = isMulti
            ? ((topAnswer && topAnswer.isVerified) ? 'Применить правильные' : 'Применить популярные')
            : ((topAnswer && topAnswer.isVerified) ? 'Применить правильный' : 'Применить популярный');
        applyBtn.disabled = !topAnswer;
        applyBtn.addEventListener('click', () => {
            if (!topAnswer) {
                return;
            }

            const payload = isMulti
                ? (topAnswer.isVerified ? verifiedAnswers : selectedAnswers)
                : [topAnswer];
            const mode = isMulti ? 'set-all' : 'add';
            const applied = requestApplyAnswers(question, payload, mode);
            if (!applied) {
                maybeLogBackendIssue('openedu_apply_failed', {
                    questionKey: question.questionKey,
                    answerText: topAnswer.answerText,
                    answerKey: topAnswer.answerKey || '',
                    mode
                });
            }
        });
        controls.appendChild(applyBtn);
        card.appendChild(controls);

        return card;
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

        // Split questions into exact and similar groups.
        const exactItems = [];
        const similarItems = [];
        questions.forEach((question, index) => {
            const stats = statsByQuestion[question.questionKey];
            if (!stats) {
                return;
            }
            const entry = { question, index, stats };
            const entryMatchedBy = String(stats.matchedBy || (stats.similarMatch ? 'similar' : 'exact'));
            if (entryMatchedBy === 'similar' || entryMatchedBy === 'content') {
                similarItems.push(entry);
            } else {
                exactItems.push(entry);
            }
        });

        const hasSimilar = similarItems.length > 0;
        const hasExact = exactItems.length > 0;

        if (!hasSimilar) {
            // No similar matches — render flat list, no tabs needed.
            exactItems.forEach((item) => {
                stickBody.appendChild(buildQuestionCard(item.question, item.index, item.stats));
            });
            return;
        }

        // Build tab bar.
        const tabBar = document.createElement('div');
        tabBar.className = 'paramext-stick-tabs';

        const exactTab = document.createElement('button');
        exactTab.type = 'button';
        exactTab.className = 'paramext-stick-tab active';
        exactTab.textContent = 'Этот вопрос' + (hasExact ? ' (' + exactItems.length + ')' : '');

        const similarTab = document.createElement('button');
        similarTab.type = 'button';
        similarTab.className = 'paramext-stick-tab';
        similarTab.textContent = 'Похожие вопросы (' + similarItems.length + ')';

        tabBar.appendChild(exactTab);
        tabBar.appendChild(similarTab);

        const exactPane = document.createElement('div');
        exactPane.className = 'paramext-stick-tab-pane';
        if (hasExact) {
            exactItems.forEach((item) => {
                exactPane.appendChild(buildQuestionCard(item.question, item.index, item.stats));
            });
        } else {
            const empty = document.createElement('div');
            empty.className = 'paramext-stick-empty';
            empty.textContent = 'Для этого вопроса пока нет точной статистики.';
            exactPane.appendChild(empty);
        }

        const similarPane = document.createElement('div');
        similarPane.className = 'paramext-stick-tab-pane';
        similarPane.style.display = 'none';
        similarItems.forEach((item) => {
            similarPane.appendChild(buildQuestionCard(item.question, item.index, item.stats));
        });

        exactTab.addEventListener('click', () => {
            exactTab.classList.add('active');
            similarTab.classList.remove('active');
            exactPane.style.display = '';
            similarPane.style.display = 'none';
        });

        similarTab.addEventListener('click', () => {
            similarTab.classList.add('active');
            exactTab.classList.remove('active');
            similarPane.style.display = '';
            exactPane.style.display = 'none';
        });

        stickBody.appendChild(tabBar);
        stickBody.appendChild(exactPane);
        stickBody.appendChild(similarPane);
    }

    function toggleStick(forceState) {
        if (!stickRoot || !wandToggle) {
            return;
        }

        if (wandsHidden) {
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

    function syncIframeStateToTop(statsByQuestion, questions, onlineState) {
        if (isTopFrame) {
            return;
        }

        try {
            const simplifiedQuestions = (Array.isArray(questions) ? questions : []).map((question) => ({
                questionKey: question.questionKey,
                domId: question.domId,
                correct: question.correct,
                hasVerifiedAnswer: question.hasVerifiedAnswer,
                allowsMultipleAnswers: Boolean(question.allowsMultipleAnswers),
                orderIndex: question.orderIndex,
                prompt: question.prompt,
                fromIframe: true,
                options: (Array.isArray(question.options) ? question.options : []).map((option) => ({
                    answerKey: option.answerKey,
                    answerText: option.answerText,
                    selected: option.selected,
                    correct: option.correct
                }))
            }));

            window.top.postMessage({
                type: 'PARAMEXT_OPENEDU_QUESTIONS_SYNC',
                stats: statsByQuestion,
                questions: simplifiedQuestions
            }, '*');
            window.top.postMessage({
                type: 'PARAMEXT_OPENEDU_STICK_ONLINE',
                online: Boolean(onlineState?.online),
                text: String(onlineState?.text || '')
            }, '*');
            debugSync('iframe_posted_sync_to_top', {
                questionCount: simplifiedQuestions.length,
                mergedKeys: statsByQuestion && typeof statsByQuestion === 'object'
                    ? Object.keys(statsByQuestion).length
                    : 0,
                onlineState
            });
        } catch (_) {
            // Ignore postMessage failures.
        }
    }

    async function runStickCycle(force, options) {
        const allowNetwork = options?.allowNetwork !== false;
        const source = String(options?.source || 'generic');

        if (cyclesStopped) {
            return;
        }

        const now = Date.now();
        if (!Boolean(force) && (now - lastCycleAt) < MIN_CYCLE_GAP_MS) {
            return;
        }

        if (cycleInFlight) {
            return;
        }

        lastCycleAt = now;
        cycleInFlight = true;
        try {
            const questions = parseQuestions();
            iframeQuestionsCache = questions;
            if (questions.length > 0) {
                lastMeaningfulQuestionsAt = now;
            }

            debugSync('cycle_parsed_questions', {
                force: Boolean(force),
                allowNetwork,
                source,
                questionCount: questions.length,
                questions: summarizeQuestionsForDebug(questions)
            });

            if (questions.length === 0) {
                const retainRenderedAnswers = typeof openeduShared.shouldRetainRenderedAnswers === 'function'
                    ? openeduShared.shouldRetainRenderedAnswers({
                        questionCount: 0,
                        hadRenderedAnswers: Boolean(lastMergedStatsByQuestion && Object.keys(lastMergedStatsByQuestion).length > 0),
                        msSinceLastMeaningfulQuestions: now - lastMeaningfulQuestionsAt,
                        msSinceLastSubmit: now - lastSubmitActionAt,
                        transientGraceMs: TRANSIENT_EMPTY_QUESTIONS_GRACE_MS,
                        submitGraceMs: ACTIVE_TAB_REFRESH_AFTER_SUBMIT_WINDOW_MS
                    })
                    : false;

                debugSync('cycle_no_questions', {
                    retainRenderedAnswers,
                    usingTopIframeCache: Boolean(isTopFrame && topFrameIframeQuestions && topFrameIframeQuestions.length > 0)
                });

                if (retainRenderedAnswers) {
                    return;
                }

                renderInlineWands({}, []);
                lastMergedStatsByQuestion = null;

                if (isTopFrame) {
                    if (topFrameIframeQuestions && topFrameIframeQuestions.length > 0) {
                        const iframeOnlineState = (typeof topFrameOnlineState !== 'undefined' && topFrameOnlineState)
                            || window.__PARAMEXT_TOPFRAME_ONLINE_STATE
                            || { online: false, text: 'API недоступен' };
                        setStickOnline(Boolean(iframeOnlineState.online), String(iframeOnlineState.text || 'API недоступен'));
                        renderStick(topFrameIframeStats, topFrameIframeQuestions);
                    } else {
                        setStickOnline(false, 'Ожидание данных из iframe');
                        renderStick({}, []);
                    }
                } else {
                    syncIframeStateToTop({}, [], topFrameOnlineState);
                }
                return;
            }

            if (!isTopFrame) {
                await requestTopContext();
            }

            const localStatsByQuestion = buildLocalFallbackStats(questions);
            let onlineState = {
                online: Boolean(topFrameOnlineState?.online),
                text: String(topFrameOnlineState?.text || 'API недоступен')
            };

            if (isSyncBlocked()) {
                const reason = syncBlockedReason === 'auth_401'
                    ? '401 токен'
                    : (syncBlockedReason === 'auth_403'
                        ? '403 доступ'
                        : (syncBlockedReason === 'network_0' ? 'network 0 (пауза)' : syncBlockedReason || 'blocked'));

                debugSync('cycle_sync_blocked', {
                    reason,
                    syncBlockedReason,
                    syncBlockedUntil
                });

                const cachedStats = lastStatsResponse && typeof lastStatsResponse === 'object'
                    ? lastStatsResponse.statsByQuestion || null
                    : null;
                const mergedStatsByQuestion = mergeStatsByQuestion(cachedStats, localStatsByQuestion, questions);
                renderInlineWands(mergedStatsByQuestion, questions);
                lastMergedStatsByQuestion = mergedStatsByQuestion;

                onlineState = { online: false, text: 'Sync пауза: ' + reason };
                topFrameOnlineState = onlineState;
                window.__PARAMEXT_TOPFRAME_ONLINE_STATE = topFrameOnlineState;

                if (isTopFrame) {
                    setStickOnline(false, onlineState.text);
                    renderStick(mergedStatsByQuestion, questions);
                } else {
                    syncIframeStateToTop(mergedStatsByQuestion, questions, onlineState);
                }
                return;
            }

            let pushResult = {
                ok: true,
                status: 204,
                error: allowNetwork ? 'not_changed' : 'skipped_no_network',
                data: null
            };
            let statsResult = {
                ok: true,
                status: 200,
                error: allowNetwork ? 'cached' : 'skipped_no_network',
                data: lastStatsResponse || { statsByQuestion: null }
            };
            let didPushUpdate = false;

            if (allowNetwork) {
                const context = getCourseContext();
                const normalizedQuestions = questions.map((question) => ({
                    questionKey: String(question.questionKey || ''),
                    correct: Boolean(question.correct),
                    verified: Boolean(question.hasVerifiedAnswer),
                    answers: (Array.isArray(question.options) ? question.options : [])
                        .map((option) => ({
                            answerKey: String(option.answerKey || ''),
                            selected: Boolean(option.selected),
                            correct: Boolean(option.correct),
                            answerText: String(option.answerText || '')
                        }))
                        .sort((a, b) => {
                            const keyCmp = a.answerKey.localeCompare(b.answerKey);
                            if (keyCmp !== 0) {
                                return keyCmp;
                            }
                            return a.answerText.localeCompare(b.answerText);
                        })
                })).sort((a, b) => a.questionKey.localeCompare(b.questionKey));

                const attemptFingerprint = hash(JSON.stringify({
                    context: {
                        testKey: context.testKey,
                        path: context.path
                    },
                    questions: normalizedQuestions
                }));

                const questionSignature = hash(JSON.stringify(normalizedQuestions.map((item) => item.questionKey)));
                const nowMs = Date.now();
                const pushCooldownActive = !Boolean(force) && (nowMs - lastAttemptPushAt) < PUSH_COOLDOWN_MS;

                if (attemptFingerprint !== lastAttemptPayloadHash && !pushCooldownActive) {
                    pushResult = await pushAttemptSnapshot(questions);
                    if (pushResult.ok) {
                        lastAttemptPayloadHash = attemptFingerprint;
                        lastAttemptPushAt = Date.now();
                        lastNetworkSyncAt = lastAttemptPushAt;
                        didPushUpdate = true;
                        clearSyncBlock();
                    }
                } else if (attemptFingerprint !== lastAttemptPayloadHash && pushCooldownActive) {
                    debugSync('push_attempt_snapshot_skipped', {
                        reason: 'push_cooldown',
                        sinceLastPushMs: nowMs - lastAttemptPushAt,
                        cooldownMs: PUSH_COOLDOWN_MS
                    });
                } else {
                    debugSync('push_attempt_snapshot_skipped', {
                        reason: 'same_attempt_fingerprint'
                    });
                }

                const networkCooldownActive = !Boolean(force) && !didPushUpdate && (Date.now() - lastNetworkSyncAt) < API_SYNC_MIN_GAP_MS;
                const shouldQueryBase =
                    Boolean(force) ||
                    didPushUpdate ||
                    questionSignature !== lastStatsQuerySignature ||
                    !lastStatsResponse;
                const shouldRespectCooldown = !didPushUpdate && !Boolean(force);
                const shouldQuery = !networkCooldownActive && shouldQueryBase && (!shouldRespectCooldown || (Date.now() - lastStatsQueryAt) >= QUERY_COOLDOWN_MS);

                if (shouldQuery) {
                    statsResult = await pullStatistics(questions);
                    if (statsResult.ok) {
                        lastStatsQuerySignature = questionSignature;
                        lastStatsQueryAt = Date.now();
                        lastNetworkSyncAt = lastStatsQueryAt;
                        lastStatsResponse = statsResult.data || { statsByQuestion: null };
                        clearSyncBlock();
                    }
                } else {
                    debugSync('pull_statistics_skipped', {
                        reason: networkCooldownActive ? 'api_sync_min_gap' : 'cooldown_or_signature_not_changed',
                        sinceLastMs: Date.now() - lastStatsQueryAt,
                        sinceLastNetworkSyncMs: Date.now() - lastNetworkSyncAt
                    });
                }

                if (!pushResult.ok && !statsResult.ok && Number(pushResult.status || 0) === 0 && Number(statsResult.status || 0) === 0) {
                    blockSync('network_0', 45000);
                    debugSync('cycle_network_backoff', {
                        pushError: pushResult.error || '',
                        statsError: statsResult.error || '',
                        syncBlockedUntil
                    });
                }
            } else {
                debugSync('cycle_network_skipped', {
                    source,
                    reason: 'ui_refresh_only'
                });
            }

            const effectiveStatsResponse = (statsResult.ok && statsResult.data && typeof statsResult.data === 'object')
                ? statsResult.data
                : (lastStatsResponse && typeof lastStatsResponse === 'object'
                    ? lastStatsResponse
                    : { statsByQuestion: null });
            const statsByQuestion = effectiveStatsResponse && typeof effectiveStatsResponse === 'object'
                ? effectiveStatsResponse.statsByQuestion || null
                : null;

            const mergedStatsByQuestion = mergeStatsByQuestion(statsByQuestion, localStatsByQuestion, questions);
            debugSync('cycle_stats_merged', {
                pushOk: pushResult.ok,
                pushStatus: pushResult.status,
                pushError: pushResult.error || '',
                statsOk: statsResult.ok,
                statsStatus: statsResult.status,
                statsError: statsResult.error || '',
                allowNetwork,
                mergedKeys: mergedStatsByQuestion && typeof mergedStatsByQuestion === 'object'
                    ? Object.keys(mergedStatsByQuestion).length
                    : 0
            });

            renderInlineWands(mergedStatsByQuestion, questions);
            lastMergedStatsByQuestion = mergedStatsByQuestion;

            const pushActuallyFailed = allowNetwork && !pushResult.ok && pushResult.error !== 'not_changed';
            const statsActuallyFailed = allowNetwork && !statsResult.ok && statsResult.error !== 'cached';
            const anyCallAttempted = allowNetwork && (
                pushActuallyFailed || statsActuallyFailed ||
                (pushResult.ok && pushResult.error !== 'not_changed') ||
                (statsResult.ok && statsResult.error !== 'cached')
            );

            if (allowNetwork) {
                onlineState = { online: true, text: 'API доступен' };
                if (pushActuallyFailed && statsActuallyFailed) {
                    const pushErr = describeRequestError(pushResult);
                    const statsErr = describeRequestError(statsResult);
                    const errText = [pushErr, statsErr].filter(Boolean).join(' / ');
                    onlineState = { online: false, text: 'API недоступен: ' + (errText || 'network') };
                }
            }

            if (anyCallAttempted) {
                if (!onlineState.online) {
                    consecutiveCycleFailures += 1;
                    if (consecutiveCycleFailures >= MAX_CONSECUTIVE_FAILURES) {
                        cyclesStopped = true;
                        onlineState = { online: false, text: 'Ошибка синхронизации (' + consecutiveCycleFailures + '/' + MAX_CONSECUTIVE_FAILURES + '). Обновите страницу.' };
                        debugSync('cycle_stopped_max_failures', { consecutiveCycleFailures });
                    }
                } else {
                    consecutiveCycleFailures = 0;
                }
            }

            topFrameOnlineState = onlineState;
            window.__PARAMEXT_TOPFRAME_ONLINE_STATE = topFrameOnlineState;

            if (isTopFrame) {
                setStickOnline(onlineState.online, onlineState.text);
                renderStick(mergedStatsByQuestion, questions);
            } else {
                syncIframeStateToTop(mergedStatsByQuestion, questions, onlineState);
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

        const now = Date.now();

        const isComplete = activeTab.classList.contains('complete');
        if (!isComplete && settings.openedu.activeTabRefreshEnabled) {
            const canRefreshActiveTab =
                (now - lastSubmitActionAt) <= ACTIVE_TAB_REFRESH_AFTER_SUBMIT_WINDOW_MS &&
                (now - lastActiveTabRefreshAt) >= ACTIVE_TAB_REFRESH_MIN_GAP_MS;

            if (canRefreshActiveTab) {
                lastActiveTabRefreshAt = now;
                activeTab.click();
            }
            return;
        }

        if (!isComplete && settings.openedu.requiredCompletionOnly) {
            return;
        }

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

    function installPageMonitors() {
        document.addEventListener('click', (event) => {
            const source = event.target instanceof Element ? event.target : null;
            if (!source) {
                return;
            }

            const actionable = source.closest('.submit, .submit.btn-brand, .problem button, .sequence-navigation-tabs button, .next-btn, .next-button');
            if (!actionable) {
                return;
            }

            if (isTopFrame && actionable.matches('.sequence-navigation-tabs button, .next-btn, .next-button')) {
                setTimeout(() => {
                    maybeClickNextOnSequencePage();
                }, 180);
            }

            const actionText = normalizeText(textOf(actionable));
            const isSubmit = actionable.matches('.submit, .submit.btn-brand, .problem button[type="submit"]')
                || (actionable.matches('.problem button') && /(провер|submit|check|save|отправ|answer)/.test(actionText));
            if (isSubmit) {
                lastSubmitActionAt = Date.now();
                let rerenderAttempts = 0;
                const tryRerender = () => {
                    rerenderAttempts++;
                    quickRerender();
                    if (rerenderAttempts < 8) {
                        setTimeout(tryRerender, 150);
                    }
                };
                setTimeout(tryRerender, 200);
                scheduleCycle(false, 'submit-preview', { allowNetwork: false });
                schedulePostSubmitSyncs();
                return;
            }

            if (shouldHandleDomRefreshTrigger()) {
                setTimeout(() => {
                    scheduleCycle(false, 'click', { allowNetwork: false });
                }, 250);
            }
        }, true);

        window.addEventListener('message', (event) => {
            const data = event.data;
            if (!data) {
                return;
            }

            if (typeof data === 'object' && Object.keys(data).length === 1 && Object.prototype.hasOwnProperty.call(data, 'offset')) {
                return;
            }

            if (typeof data === 'object' && typeof data.type === 'string' && data.type.startsWith('PARAMEXT_')) {
                return;
            }

            let text = '';
            if (typeof data === 'string') {
                text = data.toLowerCase();
            } else {
                const typeValue = typeof data?.type === 'string' ? data.type.toLowerCase() : '';
                const eventValue = typeof data?.event === 'string' ? data.event.toLowerCase() : '';
                const actionValue = typeof data?.action === 'string' ? data.action.toLowerCase() : '';
                text = [typeValue, eventValue, actionValue].filter(Boolean).join('|');
            }

            if (!text) {
                return;
            }

            if (/(problem|submission|submitted|grade|correct|incorrect|capa)/.test(text)) {
                if (shouldHandleDomRefreshTrigger()) {
                    scheduleCycle(false, 'message', { allowNetwork: false });
                }
            }
        });

        const isRelevantMutationNode = (node) => {
            if (!(node instanceof Element)) {
                return false;
            }

            const selector = QUESTION_ROOT_SELECTOR + ', .response-label, .status, .message, .feedback, .sequence-navigation-tabs';
            if (node.matches(selector)) {
                return true;
            }

            if (node.closest(selector)) {
                return true;
            }

            return Boolean(node.querySelector(selector));
        };

        const observer = new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.type === 'attributes') {
                    const node = mutation.target;
                    if (node instanceof Element && node.matches('.status, .message, .feedback, .choicegroup, .response-label, .sequence-navigation-tabs button, [data-problem-id]')) {
                        if (shouldHandleDomRefreshTrigger()) {
                            scheduleCycle(false, 'mutation', { allowNetwork: false });
                        }
                        return;
                    }
                }

                if (mutation.type === 'childList' && (mutation.addedNodes.length > 0 || mutation.removedNodes.length > 0)) {
                    const changedNodes = [];
                    mutation.addedNodes.forEach((node) => changedNodes.push(node));
                    mutation.removedNodes.forEach((node) => changedNodes.push(node));

                    if (changedNodes.some((node) => isRelevantMutationNode(node))) {
                        if (shouldHandleDomRefreshTrigger()) {
                            scheduleCycle(false, 'mutation', { allowNetwork: false });
                        }
                        return;
                    }
                }
            }
        });

        observer.observe(document.documentElement || document.body, {
            subtree: true,
            childList: true,
            attributes: true,
            attributeFilter: ['class', 'aria-label', 'data-tooltip']
        });
    }

    function installKeyboardToggle() {
        document.addEventListener('keydown', (event) => {
            if (event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement) {
                return;
            }

            if (window.ParamExtSettings.hotkeyMatches(event, settings.openedu.stickHotkey)) {
                event.preventDefault();
                setWandsHidden(!wandsHidden, true);
                if (!wandsHidden && isTopFrame) {
                    toggleStick(false);
                }
            }
        });
    }

    function installStorageSync() {
        chrome.storage.onChanged.addListener(async (changes, areaName) => {
            if (areaName !== 'local') {
                return;
            }

            const hasSettingsChange = Object.prototype.hasOwnProperty.call(changes, window.ParamExtSettings.STORAGE_KEY);
            const hasWandVisibilityChange = Object.prototype.hasOwnProperty.call(changes, WAND_VISIBILITY_KEY);
            if (!hasSettingsChange && !hasWandVisibilityChange) {
                return;
            }

            if (hasSettingsChange) {
                settings = await window.ParamExtSettings.getSettings();
                clearSyncBlock();
                consecutiveCycleFailures = 0;
                cyclesStopped = false;
                runStickCycle(true, { source: 'settings', allowNetwork: true });
            }

            if (hasWandVisibilityChange) {
                const hidden = Boolean(changes[WAND_VISIBILITY_KEY]?.newValue);
                setWandsHidden(hidden, false);
            }
        });
    }

    async function boot() {
        settings = await window.ParamExtSettings.getSettings();
        wandsHidden = await loadWandsHiddenState();

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
        setWandsHidden(wandsHidden, false);
        installKeyboardToggle();
        installPageMonitors();
        installStorageSync();

        if (isTopFrame) {
            if (!normalizeApiBaseUrl()) {
                setStickOnline(false, 'Не указан API URL');
            } else {
                setStickOnline(false, 'Ожидание данных из iframe');
            }
        }

        runStickCycle(true, { source: 'boot', allowNetwork: true });
        scheduleBootstrapSyncs();

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
