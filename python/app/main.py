from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .admin import admin_router
from .bot import start_bot, stop_bot
from .config import settings
from .database import database
from .schemas import LogPayloadIn, OpenEduAttemptIn, OpenEduSolutionsQueryIn
from .security import require_api_token, set_database_ref
from .telegram import spawn_forward


@asynccontextmanager
async def lifespan(_: FastAPI):
    await database.connect()
    set_database_ref(database)
    await start_bot(database)
    try:
        yield
    finally:
        await stop_bot()
        await database.disconnect()


app = FastAPI(title=settings.app_name, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        'https://paramext.ruka.me',
        'https://syncshare.naloaty.me',
        'https://syncshare.ru',
    ],
    allow_origin_regex=r'^(https://([a-z0-9-]+\.)?openedu\.ru|chrome-extension://[a-z]{32})$',
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*'],
    expose_headers=['*'],
    max_age=86400,
)

app.include_router(admin_router)


# ── Health ─────────────────────────────────────────────────────────

@app.get('/')
@app.get('/api')
async def root() -> dict:
    return {'service': settings.app_name, 'env': settings.app_env, 'status': 'ok'}


@app.get('/health')
@app.get('/healthz')
@app.get('/api/health')
@app.get('/api/healthz')
async def healthcheck() -> dict:
    return {
        'status': 'ok',
        'service': settings.app_name,
        'env': settings.app_env,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    }


@app.get('/v2/status')
@app.get('/api/v2/status')
async def legacy_status() -> dict:
    return {'maintenance': False, 'highDemand': False}


@app.get('/v2/update')
@app.get('/api/v2/update')
async def legacy_update() -> dict:
    return {'updateRequired': False, 'latestVersion': '2.9.0'}


# ── OpenEdu API ────────────────────────────────────────────────────

@app.post('/v1/openedu/attempts')
@app.post('/api/v1/openedu/attempts')
async def post_openedu_attempt(payload: OpenEduAttemptIn, user_id: int | None = Depends(require_api_token)) -> dict:
    await database.upsert_openedu_attempt(payload.model_dump(), user_id=user_id)
    return {'ok': True}


@app.post('/v1/openedu/solutions/query')
@app.post('/api/v1/openedu/solutions/query')
async def post_openedu_query(payload: OpenEduSolutionsQueryIn, user_id: int | None = Depends(require_api_token)) -> dict:
    from .database import (
        compute_question_fingerprint as _fingerprint,
        is_exact_question_content_match as _is_exact_match,
        normalize_answer_text as _norm_answer,
        normalize_prompt as _norm,
    )

    question_keys = payload.questionKeys
    if not question_keys and payload.questions:
        question_keys = [q.questionKey for q in payload.questions]

    stats = await database.query_openedu_stats(payload.context.testKey, question_keys)
    question_meta = await database.query_openedu_question_metadata(payload.context.testKey, question_keys)

    # Fallback chain for questions with no exact stats:
    # 1) Content fingerprint (same prompt + same answer set).
    # 2) Similar by prompt with answer-overlap gating.
    if payload.questions:
        missing = []
        for q in payload.questions:
            entry = stats.get(q.questionKey)
            has_answers = entry and (entry.get('verifiedAnswers') or entry.get('fallbackAnswers'))
            if has_answers:
                meta = question_meta.get(q.questionKey) or {}
                if meta and not _is_exact_match(
                    meta.get('promptNorm', ''),
                    meta.get('questionFingerprint', ''),
                    q.prompt,
                    q.answers or [],
                ):
                    stats[q.questionKey] = {'completedCount': 0, 'verifiedAnswers': [], 'fallbackAnswers': []}
                    entry = stats[q.questionKey]
                    has_answers = False
            if not has_answers and q.prompt:
                answer_norms_set = set()
                for answer in (q.answers or []):
                    answer_norm = _norm_answer(answer)
                    if answer_norm:
                        answer_norms_set.add(answer_norm)
                answer_norms = sorted(answer_norms_set)
                missing.append(
                    {
                        'questionKey': q.questionKey,
                        'promptNorm': _norm(q.prompt),
                        'answerNorms': answer_norms,
                        'questionFingerprint': _fingerprint(q.prompt, q.answers or []),
                    }
                )
        if missing:
            content_matches = await database.find_question_stats_by_fingerprint(payload.context.testKey, missing)
            for qk, content_stats in content_matches.items():
                stats[qk] = content_stats

            remaining = [item for item in missing if item['questionKey'] not in content_matches]
            if remaining:
                similar = await database.find_similar_question_stats(payload.context.testKey, remaining)
                for qk, sim_stats in similar.items():
                    stats[qk] = sim_stats

    return {'statsByQuestion': stats}


# ── Client logs (DB write retired, Telegram forwarding kept) ───────

@app.post('/v1/logs/client')
@app.post('/api/v1/logs/client')
async def post_extension_log(payload: LogPayloadIn, user_id: int | None = Depends(require_api_token)) -> dict:
    serialized = payload.model_dump()
    spawn_forward(serialized['kind'], serialized['payload'], serialized['system'])
    return {'ok': True}
