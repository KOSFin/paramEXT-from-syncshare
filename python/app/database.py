import hashlib
import json
import re
import uuid
from typing import Any

import asyncpg

from .config import settings

_NORM_PUNCT_RE = re.compile(r'[^\w\s]', re.UNICODE)
_NORM_WS_RE = re.compile(r'\s+')


def normalize_prompt(prompt: str) -> str:
    text = _NORM_PUNCT_RE.sub('', prompt)
    return _NORM_WS_RE.sub(' ', text).strip().lower()


class Database:
    def __init__(self) -> None:
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            host=settings.database_host,
            port=settings.database_port,
            user=settings.database_user,
            password=settings.database_password,
            database=settings.database_name,
            min_size=settings.database_min_connections,
            max_size=settings.database_max_connections,
            command_timeout=15,
        )
        await self._init_schema()

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()

    async def _init_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    telegram_username TEXT NOT NULL DEFAULT '',
                    telegram_first_name TEXT NOT NULL DEFAULT '',
                    api_token TEXT UNIQUE NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS openedu_tests (
                    test_key TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    path TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS openedu_questions (
                    test_key TEXT NOT NULL,
                    question_key TEXT NOT NULL,
                    prompt TEXT NOT NULL DEFAULT '',
                    completed_count BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (test_key, question_key)
                );

                CREATE TABLE IF NOT EXISTS openedu_answer_stats (
                    test_key TEXT NOT NULL,
                    question_key TEXT NOT NULL,
                    answer_key TEXT NOT NULL,
                    answer_text TEXT NOT NULL,
                    verified_count BIGINT NOT NULL DEFAULT 0,
                    fallback_count BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (test_key, question_key, answer_key)
                );

                CREATE TABLE IF NOT EXISTS openedu_participant_question_state (
                    test_key TEXT NOT NULL,
                    participant_key TEXT NOT NULL,
                    question_key TEXT NOT NULL,
                    selected_answer_keys TEXT[] NOT NULL DEFAULT '{}',
                    verified_answer_keys TEXT[] NOT NULL DEFAULT '{}',
                    is_correct BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (test_key, participant_key, question_key)
                );

                CREATE TABLE IF NOT EXISTS openedu_attempts (
                    id BIGSERIAL PRIMARY KEY,
                    test_key TEXT NOT NULL,
                    completed BOOLEAN NOT NULL DEFAULT FALSE,
                    source TEXT NOT NULL DEFAULT 'extension',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS extension_logs (
                    id BIGSERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    system JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_openedu_attempts_test_key ON openedu_attempts (test_key);
                CREATE INDEX IF NOT EXISTS idx_openedu_questions_test_key ON openedu_questions (test_key);
                CREATE INDEX IF NOT EXISTS idx_openedu_stats_test_key ON openedu_answer_stats (test_key);
                CREATE INDEX IF NOT EXISTS idx_openedu_participant_state_test_key ON openedu_participant_question_state (test_key);
                CREATE INDEX IF NOT EXISTS idx_extension_logs_kind ON extension_logs (kind);
                """
            )

            # Schema evolution — add columns / indexes that may not exist yet.
            for stmt in [
                "ALTER TABLE openedu_attempts ADD COLUMN IF NOT EXISTS fingerprint TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE openedu_attempts ADD COLUMN IF NOT EXISTS user_id BIGINT DEFAULT NULL",
                "ALTER TABLE openedu_participant_question_state ADD COLUMN IF NOT EXISTS user_id BIGINT DEFAULT NULL",
                "ALTER TABLE openedu_questions ADD COLUMN IF NOT EXISTS prompt_norm TEXT NOT NULL DEFAULT ''",
            ]:
                await conn.execute(stmt)

            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_openedu_attempts_fingerprint
                    ON openedu_attempts (fingerprint) WHERE fingerprint != ''
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_openedu_questions_prompt_norm
                    ON openedu_questions (test_key, prompt_norm) WHERE prompt_norm != ''
                """
            )

        # Backfill prompt_norm using Python (SQL \w doesn't handle Cyrillic).
        await self._backfill_prompt_norms()

    async def _backfill_prompt_norms(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            # Reset broken values left by the old SQL backfill (Cyrillic was
            # stripped, leaving whitespace-only strings like ' ').
            await conn.execute(
                """
                UPDATE openedu_questions
                SET prompt_norm = ''
                WHERE prompt_norm != '' AND btrim(prompt_norm) = ''
                """
            )

            rows = await conn.fetch(
                "SELECT test_key, question_key, prompt FROM openedu_questions WHERE prompt_norm = '' AND prompt != ''"
            )
            if not rows:
                return

            updates = [
                (normalize_prompt(row['prompt']), row['test_key'], row['question_key'])
                for row in rows
            ]
            # Filter out rows where normalization yields empty string.
            updates = [u for u in updates if u[0]]

            if updates:
                await conn.executemany(
                    "UPDATE openedu_questions SET prompt_norm = $1 WHERE test_key = $2 AND question_key = $3",
                    updates,
                )

    # ── Users ──────────────────────────────────────────────────────

    async def get_user_by_token(self, token: str) -> asyncpg.Record | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM users WHERE api_token = $1 AND is_active = TRUE",
                token,
            )

    async def get_user_by_telegram_id(self, telegram_id: int) -> asyncpg.Record | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM users WHERE telegram_id = $1",
                telegram_id,
            )

    async def create_user(self, telegram_id: int, username: str, first_name: str) -> asyncpg.Record:
        assert self.pool is not None
        token = str(uuid.uuid4())
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO users (telegram_id, telegram_username, telegram_first_name, api_token)
                VALUES ($1, $2, $3, $4)
                RETURNING *
                """,
                telegram_id,
                username,
                first_name,
                token,
            )

    async def regenerate_user_token(self, user_id: int) -> str:
        assert self.pool is not None
        new_token = str(uuid.uuid4())
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET api_token = $1 WHERE id = $2",
                new_token,
                user_id,
            )
        return new_token

    async def touch_user_activity(self, user_id: int) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_active_at = NOW() WHERE id = $1",
                user_id,
            )

    async def get_user_stats(self, telegram_id: int) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            user = await conn.fetchrow(
                "SELECT id, participant_key FROM (SELECT id, 'p_' || id::text AS participant_key FROM users WHERE telegram_id = $1) u",
                telegram_id,
            )
            if not user:
                return {'tests': 0, 'questions': 0, 'completions': 0}

            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(DISTINCT test_key) AS tests,
                    COUNT(*) AS questions,
                    COUNT(*) FILTER (WHERE is_correct) AS completions
                FROM openedu_participant_question_state
                WHERE user_id = $1
                """,
                user['id'],
            )
            return {
                'tests': int(row['tests']) if row else 0,
                'questions': int(row['questions']) if row else 0,
                'completions': int(row['completions']) if row else 0,
            }

    # ── OpenEdu attempts ───────────────────────────────────────────

    @staticmethod
    def _compute_attempt_fingerprint(context: dict, questions: list) -> str:
        blob = json.dumps({
            'testKey': context.get('testKey', ''),
            'path': context.get('path', ''),
            'questions': [
                {
                    'questionKey': q.get('questionKey', ''),
                    'isCorrect': bool(q.get('isCorrect')),
                    'answers': sorted(
                        [
                            {
                                'answerKey': a.get('answerKey', ''),
                                'selected': bool(a.get('selected')),
                                'correct': bool(a.get('correct')),
                            }
                            for a in q.get('answers', [])
                        ],
                        key=lambda a: a['answerKey'],
                    ),
                }
                for q in questions
            ],
        }, sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]

    async def upsert_openedu_attempt(self, payload: dict[str, Any], user_id: int | None = None) -> None:
        assert self.pool is not None
        context = payload['context']
        questions = payload.get('questions', [])
        completed = bool(payload.get('completed', False))
        participant_key = str(context.get('participantKey') or '').strip() or 'anonymous'
        fingerprint = self._compute_attempt_fingerprint(context, questions)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO openedu_tests (test_key, host, path, title, updated_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (test_key)
                    DO UPDATE SET host = EXCLUDED.host, path = EXCLUDED.path, title = EXCLUDED.title, updated_at = NOW()
                    """,
                    context['testKey'],
                    context['host'],
                    context['path'],
                    context.get('title', ''),
                )

                await conn.execute(
                    """
                    INSERT INTO openedu_attempts (test_key, completed, source, fingerprint, user_id)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (fingerprint) WHERE fingerprint != ''
                    DO UPDATE SET completed = EXCLUDED.completed, user_id = COALESCE(EXCLUDED.user_id, openedu_attempts.user_id)
                    """,
                    context['testKey'],
                    completed,
                    payload.get('source', 'extension'),
                    fingerprint,
                    user_id,
                )

                for question in questions:
                    question_key = str(question.get('questionKey') or '').strip()
                    if not question_key:
                        continue

                    question_correct = bool(question.get('isCorrect'))
                    answers = question.get('answers', [])
                    selected_answers_count = sum(1 for a in answers if bool(a.get('selected')))
                    explicit_correct_answers_count = sum(1 for a in answers if bool(a.get('correct')))
                    has_explicit_correct_answers = explicit_correct_answers_count > 0

                    if has_explicit_correct_answers and selected_answers_count <= 1 and explicit_correct_answers_count > 1:
                        has_explicit_correct_answers = False

                    answer_text_by_key: dict[str, str] = {}
                    selected_answer_keys: set[str] = set()
                    verified_answer_keys: set[str] = set()

                    for answer in answers:
                        answer_key = str(answer.get('answerKey') or '').strip()
                        if not answer_key:
                            continue
                        answer_text_by_key[answer_key] = str(answer.get('answerText') or '').strip()
                        if bool(answer.get('selected')):
                            selected_answer_keys.add(answer_key)
                        if (
                            question_correct
                            and has_explicit_correct_answers
                            and bool(answer.get('selected'))
                            and bool(answer.get('correct'))
                        ):
                            verified_answer_keys.add(answer_key)

                    previous_state = await conn.fetchrow(
                        """
                        SELECT selected_answer_keys, verified_answer_keys, is_correct
                        FROM openedu_participant_question_state
                        WHERE test_key = $1 AND participant_key = $2 AND question_key = $3
                        """,
                        context['testKey'],
                        participant_key,
                        question_key,
                    )

                    prev_selected_keys = set(previous_state['selected_answer_keys'] or []) if previous_state else set()
                    prev_verified_keys = set(previous_state['verified_answer_keys'] or []) if previous_state else set()
                    prev_is_correct = bool(previous_state['is_correct']) if previous_state else False

                    completed_delta = 0
                    if question_correct and not prev_is_correct:
                        completed_delta = 1
                    elif prev_is_correct and not question_correct:
                        completed_delta = -1

                    prompt_raw = question.get('prompt', '')
                    prompt_norm = normalize_prompt(prompt_raw)

                    await conn.execute(
                        """
                        INSERT INTO openedu_questions (test_key, question_key, prompt, prompt_norm, completed_count, updated_at)
                        VALUES ($1, $2, $3, $4, $5, NOW())
                        ON CONFLICT (test_key, question_key)
                        DO UPDATE SET prompt = EXCLUDED.prompt,
                                      prompt_norm = EXCLUDED.prompt_norm,
                                      completed_count = GREATEST(0, openedu_questions.completed_count + $5),
                                      updated_at = NOW()
                        """,
                        context['testKey'],
                        question_key,
                        prompt_raw,
                        prompt_norm,
                        completed_delta,
                    )

                    added_selected = selected_answer_keys - prev_selected_keys
                    removed_selected = prev_selected_keys - selected_answer_keys
                    added_verified = verified_answer_keys - prev_verified_keys
                    removed_verified = prev_verified_keys - verified_answer_keys

                    for answer_key in (added_selected | added_verified):
                        selected_inc = 1 if answer_key in added_selected else 0
                        verified_inc = 1 if answer_key in added_verified else 0
                        if selected_inc == 0 and verified_inc == 0:
                            continue
                        await conn.execute(
                            """
                            INSERT INTO openedu_answer_stats (test_key, question_key, answer_key, answer_text, verified_count, fallback_count, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, NOW())
                            ON CONFLICT (test_key, question_key, answer_key)
                            DO UPDATE SET answer_text = EXCLUDED.answer_text,
                                          verified_count = openedu_answer_stats.verified_count + EXCLUDED.verified_count,
                                          fallback_count = openedu_answer_stats.fallback_count + EXCLUDED.fallback_count,
                                          updated_at = NOW()
                            """,
                            context['testKey'], question_key, answer_key,
                            answer_text_by_key.get(answer_key, ''),
                            verified_inc, selected_inc,
                        )

                    for answer_key in (removed_selected | removed_verified):
                        selected_dec = 1 if answer_key in removed_selected else 0
                        verified_dec = 1 if answer_key in removed_verified else 0
                        if selected_dec == 0 and verified_dec == 0:
                            continue
                        await conn.execute(
                            """
                            UPDATE openedu_answer_stats
                            SET verified_count = GREATEST(0, verified_count - $4),
                                fallback_count = GREATEST(0, fallback_count - $5),
                                updated_at = NOW()
                            WHERE test_key = $1 AND question_key = $2 AND answer_key = $3
                            """,
                            context['testKey'], question_key, answer_key,
                            verified_dec, selected_dec,
                        )
                        await conn.execute(
                            """
                            DELETE FROM openedu_answer_stats
                            WHERE test_key = $1 AND question_key = $2 AND answer_key = $3
                              AND verified_count = 0 AND fallback_count = 0
                            """,
                            context['testKey'], question_key, answer_key,
                        )

                    await conn.execute(
                        """
                        INSERT INTO openedu_participant_question_state
                            (test_key, participant_key, question_key, selected_answer_keys, verified_answer_keys, is_correct, user_id, updated_at)
                        VALUES ($1, $2, $3, $4::text[], $5::text[], $6, $7, NOW())
                        ON CONFLICT (test_key, participant_key, question_key)
                        DO UPDATE SET selected_answer_keys = EXCLUDED.selected_answer_keys,
                                      verified_answer_keys = EXCLUDED.verified_answer_keys,
                                      is_correct = EXCLUDED.is_correct,
                                      user_id = COALESCE(EXCLUDED.user_id, openedu_participant_question_state.user_id),
                                      updated_at = NOW()
                        """,
                        context['testKey'], participant_key, question_key,
                        sorted(selected_answer_keys), sorted(verified_answer_keys),
                        question_correct, user_id,
                    )

    # ── Stats query ────────────────────────────────────────────────

    async def query_openedu_stats(self, test_key: str, question_keys: list[str]) -> dict[str, Any]:
        assert self.pool is not None
        if not question_keys:
            return {}

        async with self.pool.acquire() as conn:
            question_rows = await conn.fetch(
                "SELECT question_key, completed_count FROM openedu_questions WHERE test_key = $1 AND question_key = ANY($2::text[])",
                test_key, question_keys,
            )
            stat_rows = await conn.fetch(
                """
                SELECT question_key, answer_key, answer_text, verified_count, fallback_count
                FROM openedu_answer_stats
                WHERE test_key = $1 AND question_key = ANY($2::text[])
                ORDER BY question_key, fallback_count DESC, verified_count DESC
                """,
                test_key, question_keys,
            )

        completed_map = {row['question_key']: int(row['completed_count']) for row in question_rows}
        result: dict[str, Any] = {}
        for qk in question_keys:
            result[qk] = {'completedCount': completed_map.get(qk, 0), 'verifiedAnswers': [], 'fallbackAnswers': []}

        for row in stat_rows:
            entry = result.get(row['question_key'])
            if not entry:
                continue
            v = int(row['verified_count'])
            f = int(row['fallback_count'])
            if v > 0:
                entry['verifiedAnswers'].append({'answerKey': row['answer_key'], 'answerText': row['answer_text'], 'count': v})
            if f > 0:
                entry['fallbackAnswers'].append({'answerKey': row['answer_key'], 'answerText': row['answer_text'], 'count': f})

        return result

    # ── Similar-question fallback ────────────────────────────────────

    async def find_similar_question_stats(
        self, test_key: str, missing: list[dict[str, str]],
    ) -> dict[str, Any]:
        """For questions with no exact stats, find others with the same prompt_norm."""
        assert self.pool is not None
        if not missing:
            return {}

        prompt_norms = [m['promptNorm'] for m in missing]
        original_keys = [m['questionKey'] for m in missing]

        async with self.pool.acquire() as conn:
            # Find best matching question_key per (original_key, prompt_norm).
            matched_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (m.original_key)
                    m.original_key,
                    q.question_key AS matched_key,
                    q.completed_count
                FROM unnest($1::text[], $2::text[]) AS m(original_key, prompt_norm)
                JOIN openedu_questions q
                    ON q.test_key = $3
                    AND q.prompt_norm = m.prompt_norm
                    AND q.prompt_norm != ''
                    AND q.question_key != m.original_key
                ORDER BY m.original_key, q.completed_count DESC
                """,
                original_keys,
                prompt_norms,
                test_key,
            )

            if not matched_rows:
                return {}

            matched_map: dict[str, tuple[str, int]] = {}
            matched_keys: list[str] = []
            for row in matched_rows:
                matched_map[row['original_key']] = (row['matched_key'], int(row['completed_count']))
                matched_keys.append(row['matched_key'])

            stat_rows = await conn.fetch(
                """
                SELECT question_key, answer_key, answer_text, verified_count, fallback_count
                FROM openedu_answer_stats
                WHERE test_key = $1 AND question_key = ANY($2::text[])
                ORDER BY question_key, fallback_count DESC, verified_count DESC
                """,
                test_key,
                matched_keys,
            )

        # Group stats by matched_key.
        stats_by_matched: dict[str, list] = {}
        for row in stat_rows:
            stats_by_matched.setdefault(row['question_key'], []).append(row)

        result: dict[str, Any] = {}
        for original_key, (matched_key, completed_count) in matched_map.items():
            rows = stats_by_matched.get(matched_key, [])
            verified = []
            fallback = []
            for row in rows:
                v = int(row['verified_count'])
                f = int(row['fallback_count'])
                if v > 0:
                    verified.append({'answerKey': row['answer_key'], 'answerText': row['answer_text'], 'count': v})
                if f > 0:
                    fallback.append({'answerKey': row['answer_key'], 'answerText': row['answer_text'], 'count': f})

            if not verified and not fallback:
                continue

            result[original_key] = {
                'completedCount': completed_count,
                'verifiedAnswers': verified,
                'fallbackAnswers': fallback,
                'similarMatch': True,
                'matchedQuestionKey': matched_key,
            }

        return result

    # ── Logs (retired — no-op, kept for interface compat) ──────────

    async def write_log(self, kind: str, payload: dict[str, Any], system: dict[str, Any]) -> None:
        pass

    # ── Admin queries ──────────────────────────────────────────────

    async def get_admin_overview(self) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            counters = await conn.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM users) AS users_count,
                    (SELECT COUNT(*) FROM users WHERE last_active_at > NOW() - INTERVAL '24 hours') AS active_users_24h,
                    (SELECT COUNT(*) FROM openedu_tests) AS tests_count,
                    (SELECT COUNT(*) FROM openedu_questions) AS questions_count,
                    (SELECT COUNT(*) FROM openedu_attempts) AS attempts_count
                """
            )
            top_tests = await conn.fetch(
                """
                SELECT t.test_key, t.host, t.path, t.title,
                       COALESCE(SUM(q.completed_count), 0) AS completed_count,
                       COUNT(DISTINCT q.question_key) AS question_count
                FROM openedu_tests t
                LEFT JOIN openedu_questions q ON q.test_key = t.test_key
                GROUP BY t.test_key, t.host, t.path, t.title
                ORDER BY completed_count DESC, t.updated_at DESC
                LIMIT 20
                """
            )
        return {
            'counters': dict(counters or {}),
            'top_tests': [dict(r) for r in top_tests],
        }

    async def get_admin_users_page(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM users")
            rows = await conn.fetch(
                """
                SELECT u.id, u.telegram_id, u.telegram_username, u.telegram_first_name,
                       u.is_active, u.created_at, u.last_active_at,
                       COUNT(DISTINCT ps.test_key) AS tests_count,
                       COUNT(ps.*) AS questions_count,
                       COUNT(*) FILTER (WHERE ps.is_correct) AS completions_count
                FROM users u
                LEFT JOIN openedu_participant_question_state ps ON ps.user_id = u.id
                GROUP BY u.id
                ORDER BY u.last_active_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset,
            )
        return {'total': total, 'users': [dict(r) for r in rows]}

    async def get_admin_tests_page(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM openedu_tests")
            rows = await conn.fetch(
                """
                SELECT t.test_key, t.host, t.path, t.title, t.updated_at,
                       COUNT(DISTINCT q.question_key) AS question_count,
                       COALESCE(SUM(q.completed_count), 0) AS completed_count,
                       COUNT(DISTINCT a.user_id) AS unique_users
                FROM openedu_tests t
                LEFT JOIN openedu_questions q ON q.test_key = t.test_key
                LEFT JOIN openedu_attempts a ON a.test_key = t.test_key AND a.user_id IS NOT NULL
                GROUP BY t.test_key, t.host, t.path, t.title, t.updated_at
                ORDER BY t.updated_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset,
            )
        return {'total': total, 'tests': [dict(r) for r in rows]}


database = Database()
