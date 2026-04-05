import json
from typing import Any

import asyncpg

from .config import settings


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
                CREATE INDEX IF NOT EXISTS idx_extension_logs_kind ON extension_logs (kind);
                """
            )

    async def upsert_openedu_attempt(self, payload: dict[str, Any]) -> None:
        assert self.pool is not None
        context = payload['context']
        questions = payload.get('questions', [])
        completed = bool(payload.get('completed', False))

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
                    INSERT INTO openedu_attempts (test_key, completed, source)
                    VALUES ($1, $2, $3)
                    """,
                    context['testKey'],
                    completed,
                    payload.get('source', 'extension'),
                )

                for question in questions:
                    question_verified = bool(question.get('verified'))
                    question_correct = bool(question.get('isCorrect')) or question_verified
                    answers = question.get('answers', [])
                    has_explicit_correct_answers = any(bool(answer.get('correct')) for answer in answers)

                    await conn.execute(
                        """
                        INSERT INTO openedu_questions (test_key, question_key, prompt, completed_count, updated_at)
                        VALUES ($1, $2, $3, $4, NOW())
                        ON CONFLICT (test_key, question_key)
                        DO UPDATE
                        SET prompt = EXCLUDED.prompt,
                            completed_count = openedu_questions.completed_count + $4,
                            updated_at = NOW()
                        """,
                        context['testKey'],
                        question['questionKey'],
                        question.get('prompt', ''),
                        1 if completed else 0,
                    )

                    for answer in answers:
                        answer_selected = bool(answer.get('selected'))
                        answer_correct = bool(answer.get('correct'))

                        verified_increment = 1 if question_correct and answer_correct else 0

                        # Fallback stats are allowed only when the question itself is correct,
                        # and only when explicit correct flags are unavailable.
                        fallback_increment = 0
                        if question_correct and not has_explicit_correct_answers and answer_selected:
                            fallback_increment = 1

                        if fallback_increment == 0 and verified_increment == 0:
                            continue

                        await conn.execute(
                            """
                            INSERT INTO openedu_answer_stats (
                                test_key,
                                question_key,
                                answer_key,
                                answer_text,
                                verified_count,
                                fallback_count,
                                updated_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, NOW())
                            ON CONFLICT (test_key, question_key, answer_key)
                            DO UPDATE
                            SET answer_text = EXCLUDED.answer_text,
                                verified_count = openedu_answer_stats.verified_count + EXCLUDED.verified_count,
                                fallback_count = openedu_answer_stats.fallback_count + EXCLUDED.fallback_count,
                                updated_at = NOW()
                            """,
                            context['testKey'],
                            question['questionKey'],
                            answer['answerKey'],
                            answer['answerText'],
                            verified_increment,
                            fallback_increment,
                        )

    async def query_openedu_stats(self, test_key: str, question_keys: list[str]) -> dict[str, Any]:
        assert self.pool is not None
        if not question_keys:
            return {}

        async with self.pool.acquire() as conn:
            question_rows = await conn.fetch(
                """
                SELECT question_key, completed_count
                FROM openedu_questions
                WHERE test_key = $1
                  AND question_key = ANY($2::text[])
                """,
                test_key,
                question_keys,
            )

            stat_rows = await conn.fetch(
                """
                                SELECT question_key, answer_key, answer_text, verified_count, fallback_count
                FROM openedu_answer_stats
                WHERE test_key = $1
                  AND question_key = ANY($2::text[])
                ORDER BY question_key, verified_count DESC, fallback_count DESC
                """,
                test_key,
                question_keys,
            )

        completed_map = {row['question_key']: int(row['completed_count']) for row in question_rows}
        result: dict[str, Any] = {}

        for question_key in question_keys:
            result[question_key] = {
                'completedCount': completed_map.get(question_key, 0),
                'verifiedAnswers': [],
                'fallbackAnswers': [],
            }

        for row in stat_rows:
            question_key = row['question_key']
            entry = result.get(question_key)
            if not entry:
                continue

            verified_count = int(row['verified_count'])
            fallback_count = int(row['fallback_count'])
            answer_key = row['answer_key']
            answer_text = row['answer_text']

            if verified_count > 0:
                entry['verifiedAnswers'].append({'answerKey': answer_key, 'answerText': answer_text, 'count': verified_count})
            if fallback_count > 0:
                entry['fallbackAnswers'].append({'answerKey': answer_key, 'answerText': answer_text, 'count': fallback_count})

        return result

    async def write_log(self, kind: str, payload: dict[str, Any], system: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO extension_logs (kind, payload, system)
                VALUES ($1, $2::jsonb, $3::jsonb)
                """,
                kind,
                json.dumps(payload),
                json.dumps(system),
            )

    async def get_admin_snapshot(self) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            counters = await conn.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM openedu_tests) AS tests_count,
                    (SELECT COUNT(*) FROM openedu_questions) AS questions_count,
                    (SELECT COUNT(*) FROM openedu_attempts) AS attempts_count,
                    (SELECT COUNT(*) FROM extension_logs) AS logs_count
                """
            )

            top_tests = await conn.fetch(
                """
                SELECT t.test_key, t.host, t.path, COALESCE(SUM(q.completed_count), 0) AS completed_count
                FROM openedu_tests t
                LEFT JOIN openedu_questions q ON q.test_key = t.test_key
                GROUP BY t.test_key, t.host, t.path
                ORDER BY completed_count DESC, t.updated_at DESC
                LIMIT 20
                """
            )

            recent_logs = await conn.fetch(
                """
                SELECT kind, payload, system, created_at
                FROM extension_logs
                ORDER BY created_at DESC
                LIMIT 20
                """
            )

        return {
            'counters': dict(counters or {}),
            'top_tests': [dict(row) for row in top_tests],
            'recent_logs': [dict(row) for row in recent_logs],
        }


database = Database()
