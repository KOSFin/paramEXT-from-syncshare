(function (root) {
    let fingerprintPunctRe = null;
    try {
        fingerprintPunctRe = new RegExp('[^\\p{L}\\p{N}_\\s]', 'gu');
    } catch (_) {
        fingerprintPunctRe = /[^\w\s]/g;
    }

    const FNV64_OFFSET_A = 0xcbf29ce484222325n;
    const FNV64_OFFSET_B = 0x84222325cbf29ce4n;
    const FNV64_PRIME = 0x100000001b3n;

    function collapseWhitespace(value) {
        return String(value || '').replace(/\s+/g, ' ').trim();
    }

    function normalizeText(value) {
        return collapseWhitespace(value).toLowerCase();
    }

    function normalizeFingerprintText(value) {
        return collapseWhitespace(String(value || '').replace(fingerprintPunctRe, '')).toLowerCase();
    }

    function hashHex64(input, offset) {
        let hash = offset;
        const source = String(input || '');
        for (let i = 0; i < source.length; i += 1) {
            hash ^= BigInt(source.charCodeAt(i));
            hash = BigInt.asUintN(64, hash * FNV64_PRIME);
        }
        return hash.toString(16).padStart(16, '0');
    }

    function hashHex128(input) {
        const source = String(input || '');
        return hashHex64('a|' + source, FNV64_OFFSET_A) + hashHex64('b|' + source, FNV64_OFFSET_B);
    }

    function buildQuestionFingerprint(prompt, answerTexts) {
        const promptNorm = normalizeFingerprintText(prompt);
        const normalizedAnswers = [];
        const seen = new Set();

        (Array.isArray(answerTexts) ? answerTexts : []).forEach((answerText) => {
            const answerNorm = normalizeFingerprintText(answerText);
            if (!answerNorm || seen.has(answerNorm)) {
                return;
            }
            seen.add(answerNorm);
            normalizedAnswers.push(answerNorm);
        });

        normalizedAnswers.sort();
        if (!promptNorm && normalizedAnswers.length === 0) {
            return '';
        }

        return hashHex128(JSON.stringify({
            prompt: promptNorm,
            answers: normalizedAnswers
        }));
    }

    function buildStableQuestionKeyBase(payload) {
        const sourcePath = collapseWhitespace(payload?.sourcePath || '');
        const prompt = String(payload?.prompt || '');
        const answerTexts = Array.isArray(payload?.answerTexts) ? payload.answerTexts : [];
        const choiceCount = Math.max(0, Number(payload?.choiceCount || 0));
        const textInputCount = Math.max(0, Number(payload?.textInputCount || 0));
        const allowsMultipleAnswers = Boolean(payload?.allowsMultipleAnswers);

        return 'q2_' + hashHex128(JSON.stringify({
            sourcePath,
            promptNorm: normalizeFingerprintText(prompt),
            questionFingerprint: buildQuestionFingerprint(prompt, answerTexts),
            choiceCount,
            textInputCount,
            allowsMultipleAnswers
        }));
    }

    function normalizeMediaSource(raw) {
        const value = collapseWhitespace(raw);
        if (!value) {
            return '';
        }

        try {
            const parsed = new URL(value, 'https://openedu.ru');
            return collapseWhitespace(parsed.pathname + (parsed.search || ''));
        } catch (_) {
            return value.replace(/^https?:\/\/[^/]+/i, '');
        }
    }

    function getMediaFileName(path) {
        const normalized = collapseWhitespace(path);
        if (!normalized) {
            return '';
        }

        const parts = normalized.split('/');
        return collapseWhitespace(parts[parts.length - 1] || '');
    }

    function buildMediaToken(item, index) {
        const kind = normalizeText(item?.kind || item?.tag || 'media') || 'media';
        const source = normalizeMediaSource(item?.src || item?.href || '');
        const title = collapseWhitespace(item?.title || item?.ariaLabel || item?.alt || '');
        const fileName = getMediaFileName(source);
        const primary = fileName || source || title || ('item-' + String(index + 1));
        return title && title !== primary
            ? (kind + ':' + primary + ' | ' + title)
            : (kind + ':' + primary);
    }

    function deriveOptionAnswerText(payload) {
        const text = collapseWhitespace(payload?.text || '');
        if (text) {
            return text;
        }

        const labelled = collapseWhitespace(payload?.ariaLabel || payload?.title || '');
        if (labelled) {
            return labelled;
        }

        const mediaDescriptors = Array.isArray(payload?.mediaDescriptors) ? payload.mediaDescriptors : [];
        if (mediaDescriptors.length > 0) {
            return mediaDescriptors.map((item, index) => buildMediaToken(item, index)).join(' + ');
        }

        const inputValue = collapseWhitespace(payload?.inputValue || '');
        if (inputValue) {
            return inputValue;
        }

        return '';
    }

    function normalizeOptionList(options) {
        if (!Array.isArray(options)) {
            return [];
        }

        return options
            .map((option) => normalizeText(option?.answerText || ''))
            .filter(Boolean)
            .sort();
    }

    function matchesQuestionReference(candidate, reference) {
        if (!candidate || !reference) {
            return false;
        }

        const candidateKey = collapseWhitespace(candidate.questionKey);
        const referenceKey = collapseWhitespace(reference.questionKey);
        if (candidateKey && referenceKey && candidateKey === referenceKey) {
            return true;
        }

        const candidateDomId = collapseWhitespace(candidate.domId);
        const referenceDomId = collapseWhitespace(reference.domId);
        if (candidateDomId && referenceDomId && candidateDomId === referenceDomId) {
            return true;
        }

        const candidatePrompt = normalizeText(candidate.prompt);
        const referencePrompt = normalizeText(reference.prompt);
        if (!candidatePrompt || !referencePrompt || candidatePrompt !== referencePrompt) {
            return false;
        }

        const candidateOptions = normalizeOptionList(candidate.options);
        const referenceOptions = normalizeOptionList(reference.options);
        if (candidateOptions.length === 0 || referenceOptions.length === 0) {
            return true;
        }

        return candidateOptions.join('|') === referenceOptions.join('|');
    }

    function shouldRetainRenderedAnswers(state) {
        const questionCount = Math.max(0, Number(state?.questionCount || 0));
        if (questionCount > 0) {
            return false;
        }

        if (!state?.hadRenderedAnswers) {
            return false;
        }

        const msSinceLastMeaningfulQuestions = Math.max(0, Number(state?.msSinceLastMeaningfulQuestions || Number.MAX_SAFE_INTEGER));
        const msSinceLastSubmit = Math.max(0, Number(state?.msSinceLastSubmit || Number.MAX_SAFE_INTEGER));
        const transientGraceMs = Math.max(0, Number(state?.transientGraceMs || 8000));
        const submitGraceMs = Math.max(0, Number(state?.submitGraceMs || 15000));

        return msSinceLastMeaningfulQuestions <= transientGraceMs || msSinceLastSubmit <= submitGraceMs;
    }

    const api = {
        collapseWhitespace,
        normalizeText,
        normalizeFingerprintText,
        normalizeMediaSource,
        deriveOptionAnswerText,
        buildQuestionFingerprint,
        buildStableQuestionKeyBase,
        matchesQuestionReference,
        shouldRetainRenderedAnswers
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }

    if (root && typeof root === 'object') {
        root.ParamExtOpeneduShared = api;
    }
})(typeof globalThis !== 'undefined' ? globalThis : this);
