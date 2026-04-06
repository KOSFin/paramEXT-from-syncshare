document.addEventListener('DOMContentLoaded', async () => {
    if (!window.ParamExtSettings) {
        return;
    }

    if (window.ParamExtTelemetry) {
        window.ParamExtTelemetry.installGlobalHandlers('popup');
    }

    const mainLogo = document.getElementById('mainLogo');
    const platformMoodle = document.getElementById('platformMoodle');
    const platformOpenedu = document.getElementById('platformOpenedu');
    const moodleSettingsSection = document.getElementById('moodleSettings');
    const openeduSettingsSection = document.getElementById('openeduSettings');
    const toggleBackendSettingsBtn = document.getElementById('toggleBackendSettingsBtn');
    const backendSettingsPanel = document.getElementById('backendSettingsPanel');
    const backendPlatformMoodle = document.getElementById('backendPlatformMoodle');
    const backendPlatformOpenedu = document.getElementById('backendPlatformOpenedu');

    const backendApiBaseUrl = document.getElementById('backendApiBaseUrl');
    const backendApiToken = document.getElementById('backendApiToken');
    const backendRequestTimeoutMs = document.getElementById('backendRequestTimeoutMs');
    const backendPingBtn = document.getElementById('backendPingBtn');
    const backendResetUrlBtn = document.getElementById('backendResetUrlBtn');
    const backendPingStatus = document.getElementById('backendPingStatus');

    const moodleModeRadios = document.getElementsByName('moodleMode');
    const openeduModeRadios = document.getElementsByName('openeduMode');
    const autoSolveControls = document.getElementById('autoSolveControls');
    const btnStart = document.getElementById('btnStart');
    const btnStop = document.getElementById('btnStop');
    const wandKeyInput = document.getElementById('wandKey');
    const nextBtnSelectorInput = document.getElementById('nextBtnSelector');
    const openeduHotkeyInput = document.getElementById('openeduHotkey');
    const openeduAutoAdvanceEnabled = document.getElementById('openeduAutoAdvanceEnabled');
    const openeduRequiredCompletionOnly = document.getElementById('openeduRequiredCompletionOnly');
    const openeduActiveTabRefreshEnabled = document.getElementById('openeduActiveTabRefreshEnabled');
    const openeduShowFallbackStats = document.getElementById('openeduShowFallbackStats');
    const openeduAutoAdvanceDelayMs = document.getElementById('openeduAutoAdvanceDelayMs');
    const btnSave = document.getElementById('btnSave');

    const ENV_KEYS = {
        moodleApiBaseUrl: 'MOODLE_API_BASE_URL',
        moodleApiToken: 'MOODLE_API_TOKEN',
        moodleTimeoutMs: 'MOODLE_API_TIMEOUT_MS',
        openeduApiBaseUrl: 'OPENEDU_API_BASE_URL',
        openeduApiToken: 'OPENEDU_API_TOKEN',
        openeduTimeoutMs: 'OPENEDU_API_TIMEOUT_MS',
        botLink: 'BOT_LINK'
    };

    let activeBackendPlatform = 'openedu';

    if (mainLogo) {
        mainLogo.addEventListener('error', () => {
            mainLogo.src = '../../logo_main.png';
        });
    }

    let settings = await window.ParamExtSettings.getSettings();

    async function readEnvDefaults() {
        try {
            const response = await fetch(chrome.runtime.getURL('env.example'));
            if (!response.ok) {
                return {};
            }

            const text = await response.text();
            const output = {};

            text.split(/\r?\n/).forEach((line) => {
                const trimmed = line.trim();
                if (!trimmed || trimmed.startsWith('#')) {
                    return;
                }

                const separatorIndex = trimmed.indexOf('=');
                if (separatorIndex <= 0) {
                    return;
                }

                const key = trimmed.slice(0, separatorIndex).trim();
                let value = trimmed.slice(separatorIndex + 1).trim();
                value = value.replace(/^['\"]|['\"]$/g, '');
                output[key] = value;
            });

            return output;
        } catch (_) {
            return {};
        }
    }

    function toIntOrFallback(value, fallback) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return fallback;
        }
        return Math.max(1000, parsed);
    }

    function applyEnvDefaultsIfNeeded(current, envMap) {
        const nextState = JSON.parse(JSON.stringify(current));
        let changed = false;

        const moodleBackend = nextState.backend.moodle;
        const openeduBackend = nextState.backend.openedu;
        const defaultBackend = window.ParamExtSettings.DEFAULT_SETTINGS.backend;

        const moodleUrl = (envMap[ENV_KEYS.moodleApiBaseUrl] || '').trim().replace(/\/$/, '');
        const moodleToken = (envMap[ENV_KEYS.moodleApiToken] || '').trim();
        const moodleTimeout = envMap[ENV_KEYS.moodleTimeoutMs];

        const openeduUrl = (envMap[ENV_KEYS.openeduApiBaseUrl] || '').trim().replace(/\/$/, '');
        const openeduToken = (envMap[ENV_KEYS.openeduApiToken] || '').trim();
        const openeduTimeout = envMap[ENV_KEYS.openeduTimeoutMs];

        if (moodleUrl && (!moodleBackend.apiBaseUrl || moodleBackend.apiBaseUrl === defaultBackend.moodle.apiBaseUrl)) {
            moodleBackend.apiBaseUrl = moodleUrl;
            changed = true;
        }
        if (moodleToken && (!moodleBackend.apiToken || moodleBackend.apiToken === defaultBackend.moodle.apiToken)) {
            moodleBackend.apiToken = moodleToken;
            changed = true;
        }
        if (moodleTimeout && (
            Number(moodleBackend.requestTimeoutMs || 0) <= 0 ||
            Number(moodleBackend.requestTimeoutMs) === Number(defaultBackend.moodle.requestTimeoutMs)
        )) {
            moodleBackend.requestTimeoutMs = toIntOrFallback(moodleTimeout, 4000);
            changed = true;
        }

        if (openeduUrl && (!openeduBackend.apiBaseUrl || openeduBackend.apiBaseUrl === defaultBackend.openedu.apiBaseUrl)) {
            openeduBackend.apiBaseUrl = openeduUrl;
            changed = true;
        }
        if (openeduToken && (!openeduBackend.apiToken || openeduBackend.apiToken === defaultBackend.openedu.apiToken)) {
            openeduBackend.apiToken = openeduToken;
            changed = true;
        }
        if (openeduTimeout && (
            Number(openeduBackend.requestTimeoutMs || 0) <= 0 ||
            Number(openeduBackend.requestTimeoutMs) === Number(defaultBackend.openedu.requestTimeoutMs)
        )) {
            openeduBackend.requestTimeoutMs = toIntOrFallback(openeduTimeout, 4000);
            changed = true;
        }

        return {
            changed,
            next: window.ParamExtSettings.normalizeSettings(nextState)
        };
    }

    const envDefaults = await readEnvDefaults();
    const envApplied = applyEnvDefaultsIfNeeded(settings, envDefaults);
    if (envApplied.changed) {
        settings = await window.ParamExtSettings.saveSettings(envApplied.next);
    }

    const botLinkEl = document.getElementById('botLink');
    const botLinkUrl = (envDefaults[ENV_KEYS.botLink] || '').trim();
    if (botLinkEl && botLinkUrl) {
        botLinkEl.href = botLinkUrl;
        botLinkEl.classList.remove('hidden');
    }

    const openeduBotLink = document.getElementById('openeduBotLink');
    const openeduBotLinkFallback = document.getElementById('openeduBotLinkFallback');
    if (openeduBotLink && botLinkUrl) {
        openeduBotLink.href = botLinkUrl;
        openeduBotLink.classList.remove('hidden');
        if (openeduBotLinkFallback) {
            openeduBotLinkFallback.classList.add('hidden');
        }
    }

    if (window.ParamExtTelemetry) {
        window.ParamExtTelemetry.push('system_state', {
            activePlatform: settings.activePlatform,
            moodleApiConfigured: Boolean(settings.backend.moodle.apiBaseUrl),
            openeduApiConfigured: Boolean(settings.backend.openedu.apiBaseUrl)
        }, 'popup');
    }

    function setRadioByValue(radioList, value) {
        Array.from(radioList).forEach((radio) => {
            radio.checked = radio.value === value;
        });
    }

    function getSelectedRadioValue(radioList, fallback) {
        const checked = Array.from(radioList).find((radio) => radio.checked);
        return checked ? checked.value : fallback;
    }

    function updateMoodleAutoControls() {
        const mode = getSelectedRadioValue(moodleModeRadios, settings.moodle.mode);
        if (mode === 'autoSolve') {
            autoSolveControls.classList.remove('hidden');
        } else {
            autoSolveControls.classList.add('hidden');
        }

        if (settings.moodle.autoSolving) {
            btnStart.classList.add('hidden');
            btnStop.classList.remove('hidden');
        } else {
            btnStart.classList.remove('hidden');
            btnStop.classList.add('hidden');
        }
    }

    function setPlatform(platform) {
        settings.activePlatform = platform;
        platformMoodle.classList.toggle('active', platform === 'moodle');
        platformOpenedu.classList.toggle('active', platform === 'openedu');
        moodleSettingsSection.classList.toggle('hidden', platform !== 'moodle');
        openeduSettingsSection.classList.toggle('hidden', platform !== 'openedu');

        if (!backendSettingsPanel.classList.contains('hidden')) {
            setBackendPlatform(platform);
        }
    }

    function setBackendPlatform(platform) {
        activeBackendPlatform = platform === 'moodle' ? 'moodle' : 'openedu';
        backendPlatformMoodle.classList.toggle('active', activeBackendPlatform === 'moodle');
        backendPlatformOpenedu.classList.toggle('active', activeBackendPlatform === 'openedu');
        applyBackendPlatformFields();
    }

    function getActiveBackendConfig(state) {
        const fallback = {
            apiBaseUrl: '',
            apiToken: '',
            requestTimeoutMs: 4000
        };
        if (!state || !state.backend) {
            return fallback;
        }
        if (state.backend[activeBackendPlatform]) {
            return state.backend[activeBackendPlatform];
        }
        return fallback;
    }

    function applyBackendPlatformFields() {
        const backendConfig = getActiveBackendConfig(settings);
        backendApiBaseUrl.value = backendConfig.apiBaseUrl || '';
        backendApiToken.value = backendConfig.apiToken || '';
        backendRequestTimeoutMs.value = String(backendConfig.requestTimeoutMs || 4000);
        backendPingStatus.textContent = 'Не проверено';
        backendPingStatus.classList.remove('online', 'offline');
    }

    function writeBackendFieldsToState(state) {
        const nextState = JSON.parse(JSON.stringify(state));
        const current = nextState.backend[activeBackendPlatform];

        const normalizedApiBase = backendApiBaseUrl.value.trim().replace(/\/$/, '');
        current.apiBaseUrl = normalizedApiBase || current.apiBaseUrl;
        current.apiToken = backendApiToken.value.trim();
        current.requestTimeoutMs = Math.max(1000, Number(backendRequestTimeoutMs.value || current.requestTimeoutMs || 4000));

        return nextState;
    }

    function bindHotkeyRecorder(input) {
        input.addEventListener('keydown', (event) => {
            if (event.key === 'Tab') {
                return;
            }

            event.preventDefault();

            if ((event.key === 'Backspace' || event.key === 'Delete') && !event.ctrlKey && !event.altKey && !event.shiftKey && !event.metaKey) {
                input.value = '';
                return;
            }

            const captured = window.ParamExtSettings.serializeHotkey(event);
            if (captured) {
                input.value = captured;
            }
        });
    }

    function applyStateToUi() {
        setPlatform(settings.activePlatform);
        setRadioByValue(moodleModeRadios, settings.moodle.mode);
        setRadioByValue(openeduModeRadios, settings.openedu.mode);

        wandKeyInput.value = settings.moodle.wandHotkey;
        nextBtnSelectorInput.value = settings.moodle.nextButtonText;

        openeduHotkeyInput.value = settings.openedu.stickHotkey;
        openeduAutoAdvanceEnabled.checked = settings.openedu.autoAdvanceEnabled;
        openeduRequiredCompletionOnly.checked = settings.openedu.requiredCompletionOnly;
        openeduActiveTabRefreshEnabled.checked = settings.openedu.activeTabRefreshEnabled;
        openeduShowFallbackStats.checked = settings.openedu.showFallbackStats;
        openeduAutoAdvanceDelayMs.value = String(settings.openedu.autoAdvanceDelayMs);

        setBackendPlatform(settings.activePlatform);

        updateMoodleAutoControls();
    }

    function collectStateFromUi() {
        let nextState = JSON.parse(JSON.stringify(settings));

        nextState.moodle.mode = getSelectedRadioValue(moodleModeRadios, nextState.moodle.mode);
        nextState.moodle.wandHotkey = wandKeyInput.value.trim() || nextState.moodle.wandHotkey;
        nextState.moodle.nextButtonText = nextBtnSelectorInput.value.trim() || nextState.moodle.nextButtonText;

        nextState.openedu.mode = getSelectedRadioValue(openeduModeRadios, nextState.openedu.mode);
        nextState.openedu.stickHotkey = openeduHotkeyInput.value.trim() || nextState.openedu.stickHotkey;
        nextState.openedu.autoAdvanceEnabled = openeduAutoAdvanceEnabled.checked;
        nextState.openedu.requiredCompletionOnly = openeduRequiredCompletionOnly.checked;
        nextState.openedu.activeTabRefreshEnabled = openeduActiveTabRefreshEnabled.checked;
        nextState.openedu.showFallbackStats = openeduShowFallbackStats.checked;
        nextState.openedu.autoAdvanceDelayMs = Math.max(500, Number(openeduAutoAdvanceDelayMs.value || nextState.openedu.autoAdvanceDelayMs));

        nextState = writeBackendFieldsToState(nextState);

        return window.ParamExtSettings.normalizeSettings(nextState);
    }

    function sendToActiveTab(message) {
        chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
            const tabId = tabs && tabs[0] ? tabs[0].id : null;
            if (!tabId) {
                return;
            }
            chrome.tabs.sendMessage(tabId, message, () => {
                const lastError = chrome.runtime.lastError;
                if (lastError && window.ParamExtTelemetry) {
                    window.ParamExtTelemetry.push('send_message_error', {
                        message: lastError.message,
                        type: message.type || 'unknown'
                    }, 'popup');
                }
            });
        });
    }

    async function pingBackend() {
        backendPingStatus.textContent = 'Проверка...';
        backendPingStatus.classList.remove('online', 'offline');

        const baseUrl = backendApiBaseUrl.value.trim().replace(/\/$/, '');
        if (!baseUrl) {
            backendPingStatus.textContent = 'Не указан URL';
            backendPingStatus.classList.add('offline');
            return;
        }

        const token = backendApiToken.value.trim();
        const headers = {};
        if (token.length > 0) {
            headers.Authorization = 'Bearer ' + token;
        }

        const probePaths = ['/healthz', '/health', '/v2/status'];
        let hasHttpResponse = false;

        for (const path of probePaths) {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 3500);
            try {
                const response = await fetch(baseUrl + path, {
                    method: 'GET',
                    headers,
                    signal: controller.signal
                });

                hasHttpResponse = true;

                if (response.status !== 404) {
                    backendPingStatus.textContent = 'Онлайн';
                    backendPingStatus.classList.add('online');
                    return;
                }
            } catch (_) {
                // Continue probing other known endpoints.
            } finally {
                clearTimeout(timeout);
            }
        }

        if (hasHttpResponse) {
            backendPingStatus.textContent = 'Сервер доступен (эндпоинт не найден)';
            backendPingStatus.classList.add('online');
            return;
        }

        backendPingStatus.textContent = 'Оффлайн';
        backendPingStatus.classList.add('offline');
    }

    async function resetBackendPathToDefault() {
        if (!window.ParamExtSettings.clearBackendApiBaseUrl) {
            return;
        }

        const originalButtonText = backendResetUrlBtn.textContent;
        backendResetUrlBtn.disabled = true;
        backendResetUrlBtn.textContent = 'Сброс...';

        try {
            settings = await window.ParamExtSettings.clearBackendApiBaseUrl(activeBackendPlatform);

            const envAppliedReset = applyEnvDefaultsIfNeeded(settings, envDefaults);
            if (envAppliedReset.changed) {
                settings = await window.ParamExtSettings.saveSettings(envAppliedReset.next);
            }

            applyBackendPlatformFields();
            backendPingStatus.textContent = 'Путь по умолчанию применен';
            backendPingStatus.classList.remove('online', 'offline');
        } catch (_) {
            backendPingStatus.textContent = 'Ошибка сброса';
            backendPingStatus.classList.remove('online');
            backendPingStatus.classList.add('offline');
        } finally {
            backendResetUrlBtn.disabled = false;
            backendResetUrlBtn.textContent = originalButtonText;
        }
    }

    Array.from(moodleModeRadios).forEach((radio) => {
        radio.addEventListener('change', updateMoodleAutoControls);
    });

    platformMoodle.addEventListener('click', () => setPlatform('moodle'));
    platformOpenedu.addEventListener('click', () => setPlatform('openedu'));
    backendPlatformMoodle.addEventListener('click', () => {
        settings = writeBackendFieldsToState(settings);
        setBackendPlatform('moodle');
    });
    backendPlatformOpenedu.addEventListener('click', () => {
        settings = writeBackendFieldsToState(settings);
        setBackendPlatform('openedu');
    });

    toggleBackendSettingsBtn.addEventListener('click', () => {
        const visible = !backendSettingsPanel.classList.contains('hidden');
        backendSettingsPanel.classList.toggle('hidden', visible);
        toggleBackendSettingsBtn.textContent = visible ? 'Настройки API' : 'Скрыть настройки API';
        if (!visible) {
            setBackendPlatform(settings.activePlatform);
        }
    });

    backendPingBtn.addEventListener('click', pingBackend);
    backendResetUrlBtn.addEventListener('click', resetBackendPathToDefault);

    btnSave.addEventListener('click', async () => {
        settings = collectStateFromUi();
        settings = await window.ParamExtSettings.saveSettings(settings);
        applyStateToUi();
        sendToActiveTab({ type: 'SETTINGS_UPDATED', settings });
    });

    btnStart.addEventListener('click', async () => {
        settings.moodle.autoSolving = true;
        settings = await window.ParamExtSettings.saveSettings(settings);
        updateMoodleAutoControls();
        sendToActiveTab({ type: 'START_AUTO_SOLVE' });
    });

    btnStop.addEventListener('click', async () => {
        settings.moodle.autoSolving = false;
        settings = await window.ParamExtSettings.saveSettings(settings);
        updateMoodleAutoControls();
        sendToActiveTab({ type: 'STOP_AUTO_SOLVE' });
    });

    bindHotkeyRecorder(wandKeyInput);
    bindHotkeyRecorder(openeduHotkeyInput);
    applyStateToUi();
});
