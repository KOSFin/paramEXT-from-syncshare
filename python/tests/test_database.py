import pytest
from app.database import normalize_prompt, normalize_answer_text, compute_question_fingerprint

def test_normalize_prompt():
    assert normalize_prompt("Hello World!") == "hello world"
    assert normalize_prompt("  Multiple   Spaces  ") == "multiple spaces"
    assert normalize_prompt("Punctuation... Case???") == "punctuation case"
    assert normalize_prompt("Cyrillic Привет") == "cyrillic привет"
    assert normalize_prompt("!!!") == ""
    assert normalize_prompt("") == ""

def test_normalize_answer_text():
    # normalize_answer_text just calls normalize_prompt
    assert normalize_answer_text(" Answer! ") == "answer"

def test_compute_question_fingerprint_happy_path():
    prompt = "What is 2+2?"
    answers = ["Four", "4", "4.0"]
    fp1 = compute_question_fingerprint(prompt, answers)

    assert len(fp1) == 32
    # Deterministic
    assert compute_question_fingerprint(prompt, answers) == fp1

    # Order of answers shouldn't matter
    assert compute_question_fingerprint(prompt, ["4", "Four", "4.0"]) == fp1

def test_compute_question_fingerprint_normalization():
    # Differences in casing/punctuation in prompt and answers should result in same fingerprint
    fp1 = compute_question_fingerprint("Prompt?", ["Answer 1", "Answer 2"])
    fp2 = compute_question_fingerprint("prompt", ["answer 1!", "ANSWER 2"])
    assert fp1 == fp2

def test_compute_question_fingerprint_deduplication():
    # Duplicate answers (after normalization) should be ignored
    fp1 = compute_question_fingerprint("Prompt", ["answer", "ANSWER", "answer!"])
    fp2 = compute_question_fingerprint("Prompt", ["answer"])
    assert fp1 == fp2

def test_compute_question_fingerprint_empty_filtering():
    # Answers that normalize to empty should be ignored
    fp1 = compute_question_fingerprint("Prompt", ["answer", "!!!", "  "])
    fp2 = compute_question_fingerprint("Prompt", ["answer"])
    assert fp1 == fp2

def test_compute_question_fingerprint_empty_cases():
    # Both empty -> ''
    assert compute_question_fingerprint("", []) == ""
    # Both normalize to empty -> ''
    assert compute_question_fingerprint("!!!", ["???", "   "]) == ""

    # One not empty
    assert compute_question_fingerprint("Prompt", []) != ""
    assert compute_question_fingerprint("", ["Answer"]) != ""

def test_compute_question_fingerprint_unicode():
    prompt = "Как дела?"
    answers = ["Хорошо", "Отлично"]
    fp = compute_question_fingerprint(prompt, answers)
    assert fp != ""
    assert compute_question_fingerprint("как дела", ["хорошо", "отлично"]) == fp
