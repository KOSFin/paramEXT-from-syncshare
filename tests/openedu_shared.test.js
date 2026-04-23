const test = require('node:test');
const assert = require('node:assert/strict');

const openeduShared = require('../js/openedu_shared.js');

test('deriveOptionAnswerText keeps visible text answers unchanged', () => {
    const answerText = openeduShared.deriveOptionAnswerText({
        text: '  Верный   ответ  '
    });

    assert.equal(answerText, 'Верный ответ');
});

test('deriveOptionAnswerText builds stable token for image-only answers', () => {
    const answerText = openeduShared.deriveOptionAnswerText({
        text: '',
        mediaDescriptors: [{
            kind: 'img',
            src: '//cdn2.openedu.ru/assets/courseware/v1/69ef4e29da38c45dd48f9b0a90a2d277/asset-v1:urfu+GEOM+spring_2026+type@asset+block/t1.1.1.jpg'
        }]
    });

    assert.equal(answerText, 'img:t1.1.1.jpg');
});

test('matchesQuestionReference falls back to prompt and options when question key changes', () => {
    const candidate = {
        questionKey: 'new-key',
        domId: 'problem_1::n:input',
        prompt: 'Отметьте чертежи, полученные ортогональным проецированием',
        options: [
            { answerText: 'img:t1.1.1.jpg' },
            { answerText: 'img:t1.1.2.jpg' }
        ]
    };
    const reference = {
        questionKey: 'old-key',
        domId: '',
        prompt: 'Отметьте чертежи, полученные ортогональным проецированием',
        options: [
            { answerText: 'img:t1.1.2.jpg' },
            { answerText: 'img:t1.1.1.jpg' }
        ]
    };

    assert.equal(openeduShared.matchesQuestionReference(candidate, reference), true);
});

test('buildStableQuestionKeyBase stays stable for the same question content', () => {
    const first = openeduShared.buildStableQuestionKeyBase({
        sourcePath: '/courses/1',
        prompt: 'Что такое FLOPS?',
        answerTexts: ['количество операций в секунду', 'частота процессора'],
        choiceCount: 2,
        textInputCount: 0,
        allowsMultipleAnswers: false
    });
    const second = openeduShared.buildStableQuestionKeyBase({
        sourcePath: '/courses/1',
        prompt: 'Что такое FLOPS',
        answerTexts: ['частота процессора', 'количество операций в секунду'],
        choiceCount: 2,
        textInputCount: 0,
        allowsMultipleAnswers: false
    });

    assert.equal(first, second);
});

test('buildStableQuestionKeyBase changes when answer set changes', () => {
    const first = openeduShared.buildStableQuestionKeyBase({
        sourcePath: '/courses/1',
        prompt: 'Что такое FLOPS?',
        answerTexts: ['количество операций в секунду', 'частота процессора'],
        choiceCount: 2,
        textInputCount: 0,
        allowsMultipleAnswers: false
    });
    const second = openeduShared.buildStableQuestionKeyBase({
        sourcePath: '/courses/1',
        prompt: 'Что такое FLOPS?',
        answerTexts: ['модель процессора', 'частота процессора'],
        choiceCount: 2,
        textInputCount: 0,
        allowsMultipleAnswers: false
    });

    assert.notEqual(first, second);
});

test('shouldRetainRenderedAnswers keeps UI during transient empty rerender after submit', () => {
    const keepUi = openeduShared.shouldRetainRenderedAnswers({
        questionCount: 0,
        hadRenderedAnswers: true,
        msSinceLastMeaningfulQuestions: 1200,
        msSinceLastSubmit: 900,
        transientGraceMs: 9000,
        submitGraceMs: 15000
    });

    assert.equal(keepUi, true);
});

test('shouldRetainRenderedAnswers clears UI when page is truly empty for long enough', () => {
    const keepUi = openeduShared.shouldRetainRenderedAnswers({
        questionCount: 0,
        hadRenderedAnswers: true,
        msSinceLastMeaningfulQuestions: 25000,
        msSinceLastSubmit: 25000,
        transientGraceMs: 9000,
        submitGraceMs: 15000
    });

    assert.equal(keepUi, false);
});
