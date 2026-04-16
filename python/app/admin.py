from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from urllib.parse import quote_plus

from .config import settings
from .database import database
from .security import ADMIN_COOKIE, ADMIN_MAX_AGE, create_admin_cookie_value, require_admin_session

templates = Jinja2Templates(directory='app/templates')

admin_router = APIRouter()

PAGE_SIZE = 50


# ── Login / Logout ─────────────────────────────────────────────────

@admin_router.get('/admin/login', response_class=HTMLResponse)
@admin_router.get('/api/admin/login', response_class=HTMLResponse)
async def admin_login_page(request: Request):
    return templates.TemplateResponse(request=request, name='admin_login.html', context={'error': None})


@admin_router.post('/admin/login', response_class=HTMLResponse)
@admin_router.post('/api/admin/login', response_class=HTMLResponse)
async def admin_login_submit(request: Request, token: str = Form(...)):
    if token != settings.admin_token:
        return templates.TemplateResponse(
            request=request,
            name='admin_login.html',
            context={'error': 'Неверный токен'},
            status_code=401,
        )
    response = RedirectResponse(url='/admin', status_code=303)
    response.set_cookie(
        key=ADMIN_COOKIE,
        value=create_admin_cookie_value(),
        max_age=ADMIN_MAX_AGE,
        httponly=True,
        samesite='lax',
    )
    return response


@admin_router.get('/admin/logout')
@admin_router.get('/api/admin/logout')
async def admin_logout():
    response = RedirectResponse(url='/admin/login', status_code=303)
    response.delete_cookie(ADMIN_COOKIE)
    return response


# ── Overview ───────────────────────────────────────────────────────

@admin_router.get('/admin', response_class=HTMLResponse)
@admin_router.get('/api/admin', response_class=HTMLResponse)
async def admin_overview(request: Request, token: str | None = None):
    require_admin_session(request, token=token)
    data = await database.get_admin_overview()
    return templates.TemplateResponse(
        request=request,
        name='admin_overview.html',
        context={'data': data, 'active_page': 'overview'},
    )


# ── Users ──────────────────────────────────────────────────────────

@admin_router.get('/admin/users', response_class=HTMLResponse)
@admin_router.get('/api/admin/users', response_class=HTMLResponse)
async def admin_users(request: Request, page: int = 1, token: str | None = None):
    require_admin_session(request, token=token)
    offset = (max(1, page) - 1) * PAGE_SIZE
    data = await database.get_admin_users_page(limit=PAGE_SIZE, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name='admin_users.html',
        context={'data': data, 'page': max(1, page), 'page_size': PAGE_SIZE, 'active_page': 'users'},
    )


# ── Tests ──────────────────────────────────────────────────────────

@admin_router.get('/admin/tests', response_class=HTMLResponse)
@admin_router.get('/api/admin/tests', response_class=HTMLResponse)
async def admin_tests(request: Request, page: int = 1, token: str | None = None):
    require_admin_session(request, token=token)
    offset = (max(1, page) - 1) * PAGE_SIZE
    data = await database.get_admin_tests_page(limit=PAGE_SIZE, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name='admin_tests.html',
        context={'data': data, 'page': max(1, page), 'page_size': PAGE_SIZE, 'active_page': 'tests'},
    )


# ── Questions ──────────────────────────────────────────────────────

@admin_router.get('/admin/questions', response_class=HTMLResponse)
@admin_router.get('/api/admin/questions', response_class=HTMLResponse)
async def admin_questions(request: Request, page: int = 1, q: str = '', token: str | None = None):
    require_admin_session(request, token=token)
    offset = (max(1, page) - 1) * PAGE_SIZE
    data = await database.get_admin_questions_page(search=q, limit=PAGE_SIZE, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name='admin_questions.html',
        context={
            'data': data,
            'page': max(1, page),
            'page_size': PAGE_SIZE,
            'search': q,
            'search_url': quote_plus(q),
            'active_page': 'questions',
        },
    )


# ── Data API (JSON) ───────────────────────────────────────────────

@admin_router.get('/admin/data')
@admin_router.get('/api/admin/data')
async def admin_data(request: Request, token: str | None = None):
    require_admin_session(request, token=token)
    return await database.get_admin_overview()
