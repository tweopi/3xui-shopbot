import os
import logging
import asyncio
import json
import hashlib
import html as html_escape
import base64
import time
import uuid
from hmac import compare_digest
from datetime import datetime
from functools import wraps
from math import ceil
from flask import Flask, request, render_template, redirect, url_for, flash, session, current_app, jsonify
from flask_wtf.csrf import CSRFProtect, generate_csrf
import secrets
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from shop_bot.modules import xui_api
from shop_bot.modules.speedtest_manager import speedtest_manager
from shop_bot.bot import handlers 
from shop_bot.support_bot_controller import SupportBotController
from shop_bot.data_manager.database import (
    get_all_settings, update_setting, get_all_hosts, get_plans_for_host,
    create_host, delete_host, create_plan, delete_plan, update_plan, get_user_count,
    get_total_keys_count, get_total_spent_sum, get_daily_stats_for_charts,
    get_recent_transactions, get_paginated_transactions, get_all_users, get_user_keys,
    ban_user, unban_user, delete_user_keys, get_setting, find_and_complete_ton_transaction,
    get_tickets_paginated, get_open_tickets_count, get_ticket, get_ticket_messages,
    add_support_message, set_ticket_status, delete_ticket,
    get_closed_tickets_count, get_all_tickets_count, update_host_subscription_url,
    update_host_url, update_host_name, update_host_agent_info,
    get_all_keys, get_keys_for_user, get_key_by_id, delete_key_by_id, update_key_comment, update_key_info,
    add_new_key, get_balance, adjust_user_balance, get_referrals_for_user,
    get_user, get_key_by_email
)

_bot_controller = None
_support_bot_controller = SupportBotController()

ALL_SETTINGS_KEYS = [
    "panel_login", "panel_password", "about_text", "terms_url", "privacy_url",
    "support_user", "support_text", "channel_url", "telegram_bot_token",
    "telegram_bot_username", "admin_telegram_id", "yookassa_shop_id",
    "yookassa_secret_key", "sbp_enabled", "receipt_email", "cryptobot_token",
    "heleket_merchant_id", "heleket_api_key", "domain", "referral_percentage",
    "referral_discount", "ton_wallet_address", "tonapi_key", "force_subscription", "trial_enabled", "trial_duration_days", "enable_referrals", "minimum_withdrawal",
    # Реферальные начисления: альтернативный фиксированный бонус
    "enable_fixed_referral_bonus", "fixed_referral_bonus_amount",
    # Тип начисления реферальной системы (без стартового бонуса)
    "referral_reward_type", "referral_on_start_referrer_amount",
    "support_forum_chat_id",
    "support_bot_token", "support_bot_username",
    # UI
    "panel_brand_title"
]

def create_webhook_app(bot_controller_instance):
    global _bot_controller
    _bot_controller = bot_controller_instance

    app_file_path = os.path.abspath(__file__)
    app_dir = os.path.dirname(app_file_path)
    template_dir = os.path.join(app_dir, 'templates')
    template_file = os.path.join(template_dir, 'login.html')

    print("--- DIAGNOSTIC INFORMATION ---", flush=True)
    print(f"Current Working Directory: {os.getcwd()}", flush=True)
    print(f"Path of running app.py: {app_file_path}", flush=True)
    print(f"Directory of running app.py: {app_dir}", flush=True)
    print(f"Expected templates directory: {template_dir}", flush=True)
    print(f"Expected login.html path: {template_file}", flush=True)
    print(f"Does template directory exist? -> {os.path.isdir(template_dir)}", flush=True)
    print(f"Does login.html file exist? -> {os.path.isfile(template_file)}", flush=True)
    print("--- END DIAGNOSTIC INFORMATION ---", flush=True)
    
    flask_app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static'
    )
    
    # SECRET_KEY из окружения или сгенерированный на лету (без хардкода)
    flask_app.config['SECRET_KEY'] = os.getenv('SHOPBOT_SECRET_KEY') or secrets.token_hex(32)
    from datetime import timedelta
    flask_app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

    # CSRF защита для всех POST форм в панели; вебхуки будут исключены
    csrf = CSRFProtect()
    csrf.init_app(flask_app)

    @flask_app.context_processor
    def inject_current_year():
        # Добавляем csrf_token в шаблоны для meta и скрытых полей
        return {
            'current_year': datetime.utcnow().year,
            'csrf_token': generate_csrf
        }

    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'logged_in' not in session:
                return redirect(url_for('login_page'))
            return f(*args, **kwargs)
        return decorated_function

    @flask_app.route('/login', methods=['GET', 'POST'])
    def login_page():
        settings = get_all_settings()
        if request.method == 'POST':
            if request.form.get('username') == settings.get("panel_login") and \
               request.form.get('password') == settings.get("panel_password"):
                session['logged_in'] = True
                # remember-me: делаем сессию постоянной при установленном чекбоксе
                session.permanent = bool(request.form.get('remember_me'))
                return redirect(url_for('dashboard_page'))
            else:
                flash('Неверный логин или пароль', 'danger')
        return render_template('login.html')

    @flask_app.route('/logout', methods=['POST'])
    @login_required
    def logout_page():
        session.pop('logged_in', None)
        flash('Вы успешно вышли.', 'success')
        return redirect(url_for('login_page'))

    def get_common_template_data():
        bot_status = _bot_controller.get_status()
        support_bot_status = _support_bot_controller.get_status()
        settings = get_all_settings()
        required_for_start = ['telegram_bot_token', 'telegram_bot_username', 'admin_telegram_id']
        required_support_for_start = ['support_bot_token', 'support_bot_username', 'admin_telegram_id']
        all_settings_ok = all(settings.get(key) for key in required_for_start)
        support_settings_ok = all(settings.get(key) for key in required_support_for_start)
        try:
            open_tickets_count = get_open_tickets_count()
            closed_tickets_count = get_closed_tickets_count()
            all_tickets_count = get_all_tickets_count()
        except Exception:
            open_tickets_count = 0
            closed_tickets_count = 0
            all_tickets_count = 0
        return {
            "bot_status": bot_status,
            "all_settings_ok": all_settings_ok,
            "support_bot_status": support_bot_status,
            "support_settings_ok": support_settings_ok,
            "open_tickets_count": open_tickets_count,
            "closed_tickets_count": closed_tickets_count,
            "all_tickets_count": all_tickets_count,
            "brand_title": settings.get('panel_brand_title') or 'T‑Shift VPN',
        }

    @flask_app.route('/brand-title', methods=['POST'])
    @login_required
    def update_brand_title_route():
        title = (request.form.get('title') or '').strip()
        if not title:
            return jsonify({"ok": False, "error": "empty"}), 400
        try:
            update_setting('panel_brand_title', title)
            return jsonify({"ok": True, "title": title})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/')
    @login_required
    def index():
        return redirect(url_for('dashboard_page'))

    @flask_app.route('/dashboard')
    @login_required
    def dashboard_page():
        stats = {
            "user_count": get_user_count(),
            "total_keys": get_total_keys_count(),
            "total_spent": get_total_spent_sum(),
            "host_count": len(get_all_hosts())
        }
        
        page = request.args.get('page', 1, type=int)
        per_page = 8
        
        transactions, total_transactions = get_paginated_transactions(page=page, per_page=per_page)
        total_pages = ceil(total_transactions / per_page)
        
        chart_data = get_daily_stats_for_charts(days=30)
        common_data = get_common_template_data()
        
        return render_template(
            'dashboard.html',
            stats=stats,
            chart_data=chart_data,
            transactions=transactions,
            current_page=page,
            total_pages=total_pages,
            **common_data
        )

    # Partials for dashboard fragments (auto-update without reload)
    @flask_app.route('/dashboard/stats.partial')
    @login_required
    def dashboard_stats_partial():
        stats = {
            "user_count": get_user_count(),
            "total_keys": get_total_keys_count(),
            "total_spent": get_total_spent_sum(),
            "host_count": len(get_all_hosts())
        }
        common_data = get_common_template_data()
        return render_template('partials/dashboard_stats.html', stats=stats, **common_data)

    @flask_app.route('/dashboard/transactions.partial')
    @login_required
    def dashboard_transactions_partial():
        page = request.args.get('page', 1, type=int)
        per_page = 8
        transactions, total_transactions = get_paginated_transactions(page=page, per_page=per_page)
        return render_template('partials/dashboard_transactions.html', transactions=transactions)

    @flask_app.route('/dashboard/charts.json')
    @login_required
    def dashboard_charts_json():
        data = get_daily_stats_for_charts(days=30)
        return jsonify(data)

    # --- Host agent settings & per-host speedtest ---
    @flask_app.route('/admin/hosts/agent/save', methods=['POST'])
    @login_required
    def save_host_agent_settings_route():
        host_name = (request.form.get('host_name') or '').strip()
        agent_url = (request.form.get('agent_url') or '').strip()
        agent_token = (request.form.get('agent_token') or '').strip()
        if not host_name:
            flash('Не указан хост', 'danger')
            return redirect(url_for('settings_page'))
        ok = update_host_agent_info(host_name, agent_url or None, agent_token or None)
        flash('Настройки агента сохранены.' if ok else 'Не удалось сохранить настройки агента.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page'))

    @flask_app.route('/admin/hosts/<host_name>/speedtest', methods=['POST'])
    @login_required
    def run_host_speedtest_route(host_name: str):
        """Запускает speedtest на указанном хосте через лёгкий агент."""
        # Получаем данные хоста, чтобы взять agent_url/token
        host = None
        for h in get_all_hosts():
            if (h.get('host_name') or '').strip() == (host_name or '').strip():
                host = h
                break
        if not host:
            return jsonify({"ok": False, "error": "host_not_found"}), 404
        agent_url = (host.get('agent_url') or '').strip()
        agent_token = (host.get('agent_token') or '').strip()
        if not agent_url:
            return jsonify({"ok": False, "error": "agent_not_configured"}), 400
        # Формируем запрос к агенту: POST /speedtest { force:true }
        try:
            url = agent_url.rstrip('/') + '/speedtest'
            payload = json.dumps({"force": True}).encode('utf-8')
            req = urllib.request.Request(url, data=payload, method='POST')
            req.add_header('Content-Type', 'application/json')
            if agent_token:
                req.add_header('Authorization', f'Bearer {agent_token}')
            with urllib.request.urlopen(req, timeout=240) as resp:
                body = resp.read().decode('utf-8')
                data = json.loads(body)
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode('utf-8')
                err = json.loads(body)
            except Exception:
                err = {"error": e.reason, "code": e.code}
            return jsonify({"ok": False, "error": err}), 502
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 502

        # Ожидаем от агента поля: download_speed, upload_speed, ping, isp, external_ip, server_name, server_country, timestamp
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "invalid_agent_response"}), 502
        return jsonify({"ok": True, "data": data})

    # --- Speedtest routes ---
    @flask_app.route('/speedtest/run', methods=['POST'])
    @login_required
    def run_speedtest():
        """Запуск теста скорости"""
        force = request.json.get('force', False) if request.is_json else False
        
        try:
            result = speedtest_manager.run_speed_test(force=force)
            if result:
                return jsonify({
                    "success": True,
                    "data": result
                })
            else:
                return jsonify({
                    "success": False,
                    "error": "Не удалось выполнить тест скорости"
                }), 500
        except Exception as e:
            logger.error(f"Ошибка при выполнении speedtest: {e}")
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500

    @flask_app.route('/speedtest/status')
    @login_required
    def speedtest_status():
        """Получение статуса и результатов speedtest"""
        try:
            status = speedtest_manager.get_test_status()
            results = speedtest_manager.get_cached_results()
            
            return jsonify({
                "success": True,
                "status": status,
                "results": results
            })
        except Exception as e:
            logger.error(f"Ошибка при получении статуса speedtest: {e}")
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500

    @flask_app.route('/speedtest/widget.partial')
    @login_required
    def speedtest_widget_partial():
        """Partial для виджета speedtest"""
        try:
            status = speedtest_manager.get_test_status()
            results = speedtest_manager.get_cached_results()
            
            return render_template('partials/speedtest_widget.html', 
                                 status=status, results=results)
        except Exception as e:
            logger.error(f"Ошибка при рендеринге speedtest widget: {e}")
            return render_template('partials/speedtest_widget.html', 
                                 status=None, results=None, error=str(e))

    # --- Support partials ---
    @flask_app.route('/support/table.partial')
    @login_required
    def support_table_partial():
        status = request.args.get('status') or None
        page = request.args.get('page', 1, type=int)
        per_page = 12
        tickets, total = get_tickets_paginated(page=page, per_page=per_page, status=status)
        return render_template('partials/support_table.html', tickets=tickets)

    @flask_app.route('/support/open-count.partial')
    @login_required
    def support_open_count_partial():
        try:
            count = get_open_tickets_count() or 0
        except Exception:
            count = 0
        # Возвращаем готовый HTML-бейдж (или пустую строку)
        if count and count > 0:
            html = (
                '<span class="badge bg-green-lt" title="Открытые тикеты">'
                '<span class="status-dot status-dot-animated bg-green"></span>'
                f" {count}</span>"
            )
        else:
            html = ''
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}

    @flask_app.route('/users')
    @login_required
    def users_page():
        users = get_all_users()
        for user in users:
            uid = user['telegram_id']
            user['user_keys'] = get_user_keys(uid)
            try:
                user['balance'] = get_balance(uid)
                user['referrals'] = get_referrals_for_user(uid)
            except Exception:
                user['balance'] = 0.0
                user['referrals'] = []
        
        common_data = get_common_template_data()
        return render_template('users.html', users=users, **common_data)

    # Partial: users table tbody
    @flask_app.route('/users/table.partial')
    @login_required
    def users_table_partial():
        users = get_all_users()
        for user in users:
            uid = user['telegram_id']
            user['user_keys'] = get_user_keys(uid)
            try:
                user['balance'] = get_balance(uid)
                user['referrals'] = get_referrals_for_user(uid)
            except Exception:
                user['balance'] = 0.0
                user['referrals'] = []
        return render_template('partials/users_table.html', users=users)

    @flask_app.route('/users/<int:user_id>/balance/adjust', methods=['POST'])
    @login_required
    def adjust_balance_route(user_id: int):
        try:
            delta = float(request.form.get('delta', '0') or '0')
        except ValueError:
            # AJAX?
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                return jsonify({"ok": False, "error": "invalid_amount"}), 400
            flash('Некорректная сумма изменения баланса.', 'danger')
            return redirect(url_for('users_page'))

        ok = adjust_user_balance(user_id, delta)
        message = 'Баланс изменён.' if ok else 'Не удалось изменить баланс.'
        category = 'success' if ok else 'danger'
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": ok, "message": message})
        flash(message, category)
        # Telegram-уведомление пользователю (через запущенный цикл событий бота)
        try:
            if ok:
                bot = _bot_controller.get_bot_instance()
                if bot:
                    sign = '+' if delta >= 0 else ''
                    text = f"💳 Ваш баланс был изменён администратором: {sign}{delta:.2f} RUB\nТекущий баланс: {get_balance(user_id):.2f} RUB"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                        logger.info(f"Balance notify scheduled for user {user_id}")
                    else:
                        # fallback, если по какой-то причине нет общего цикла (не рекомендуется, но лучше чем молча не отправить)
                        logger.warning("EVENT_LOOP is not running; using fallback asyncio.run for balance notification")
                        asyncio.run(bot.send_message(chat_id=user_id, text=text))
                else:
                    logger.warning("Bot instance is None; cannot send balance notification")
        except Exception as e:
            logger.warning(f"Failed to send balance notification: {e}")
        return redirect(url_for('users_page'))

    @flask_app.route('/admin/keys')
    @login_required
    def admin_keys_page():
        keys = []
        try:
            keys = get_all_keys()
        except Exception:
            keys = []
        hosts = []
        try:
            hosts = get_all_hosts()
        except Exception:
            hosts = []
        users = []
        try:
            users = get_all_users()
        except Exception:
            users = []
        common_data = get_common_template_data()
        return render_template('admin_keys.html', keys=keys, hosts=hosts, users=users, **common_data)

    # Partial: admin keys table tbody
    @flask_app.route('/admin/keys/table.partial')
    @login_required
    def admin_keys_table_partial():
        keys = []
        try:
            keys = get_all_keys()
        except Exception:
            keys = []
        return render_template('partials/admin_keys_table.html', keys=keys)

    @flask_app.route('/admin/hosts/<host_name>/plans')
    @login_required
    def admin_get_plans_for_host_json(host_name: str):
        try:
            plans = get_plans_for_host(host_name)
            data = [
                {
                    "plan_id": p.get('plan_id'),
                    "plan_name": p.get('plan_name'),
                    "months": p.get('months'),
                    "price": p.get('price'),
                } for p in plans
            ]
            return jsonify({"ok": True, "items": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/create', methods=['POST'])
    @login_required
    def create_key_route():
        try:
            user_id = int(request.form.get('user_id'))
            host_name = (request.form.get('host_name') or '').strip()
            xui_uuid = (request.form.get('xui_client_uuid') or '').strip()
            key_email = (request.form.get('key_email') or '').strip()
            expiry = request.form.get('expiry_date') or ''
            # ожидаем datetime-local, конвертируем в ms
            from datetime import datetime
            expiry_ms = int(datetime.fromisoformat(expiry).timestamp() * 1000) if expiry else 0
        except Exception:
            flash('Проверьте поля ключа.', 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))
        # Если UUID не указан — генерируем автоматически, как при выдаче ключа в боте
        if not xui_uuid:
            xui_uuid = str(uuid.uuid4())
        # 1) Создать/обновить клиента на XUI-хосте
        result = None
        try:
            result = asyncio.run(xui_api.create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
        except Exception as e:
            logger.error(f"Failed to create/update key on host: {e}")
            result = None
        if not result:
            flash('Не удалось создать ключ на хосте. Проверьте доступность XUI.', 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))

        # Обновляем UUID и expiry на основании ответа панели
        try:
            xui_uuid = result.get('client_uuid') or xui_uuid
            expiry_ms = result.get('expiry_timestamp_ms') or expiry_ms
        except Exception:
            pass

        # 2) Сохранить в БД
        new_id = add_new_key(user_id, host_name, xui_uuid, key_email, expiry_ms or 0)
        flash(('Ключ добавлен.' if new_id else 'Ошибка при добавлении ключа.'), 'success' if new_id else 'danger')

        # 3) Уведомление пользователю в Telegram (без email, с пометкой, что ключ выдан администратором)
        try:
            bot = _bot_controller.get_bot_instance()
            if bot and new_id:
                text = (
                    '🔐 Ваш ключ готов!\n'
                    f'Сервер: {host_name}\n'
                    'Выдан администратором через панель.\n'
                )
                if result and result.get('connection_string'):
                    cs = html_escape.escape(result['connection_string'])
                    text += f"\nПодключение:\n<pre><code>{cs}</code></pre>"
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True),
                        loop
                    )
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
        except Exception as e:
            logger.warning(f"Failed to notify user about new key: {e}")
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/create-ajax', methods=['POST'])
    @login_required
    def create_key_ajax_route():
        try:
            user_id = int(request.form.get('user_id'))
            host_name = (request.form.get('host_name') or '').strip()
            xui_uuid = (request.form.get('xui_client_uuid') or '').strip()
            key_email = (request.form.get('key_email') or '').strip()
            expiry = request.form.get('expiry_date') or ''
            from datetime import datetime
            expiry_ms = int(datetime.fromisoformat(expiry).timestamp() * 1000) if expiry else 0
        except Exception as e:
            return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400

        if not xui_uuid:
            xui_uuid = str(uuid.uuid4())

        try:
            result = asyncio.run(xui_api.create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
        except Exception as e:
            result = None
            logger.error(f"create_key_ajax_route: host error: {e}")
        if not result:
            return jsonify({"ok": False, "error": "host_failed"}), 500

        # sync DB
        new_id = add_new_key(user_id, host_name, result.get('client_uuid') or xui_uuid, key_email, result.get('expiry_timestamp_ms') or expiry_ms or 0)

        # notify user (без email, с пометкой про администратора)
        try:
            bot = _bot_controller.get_bot_instance()
            if bot and new_id:
                text = (
                    '🔐 Ваш ключ готов!\n'
                    f'Сервер: {host_name}\n'
                    'Выдан администратором через панель.\n'
                )
                if result and result.get('connection_string'):
                    cs = html_escape.escape(result['connection_string'])
                    text += f"\nПодключение:\n<pre><code>{cs}</code></pre>"
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True),
                        loop
                    )
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
        except Exception as e:
            logger.warning(f"Failed to notify user (ajax): {e}")

        return jsonify({
            "ok": True,
            "key_id": new_id,
            "uuid": result.get('client_uuid'),
            "expiry_ms": result.get('expiry_timestamp_ms'),
            "connection": result.get('connection_string')
        })

    @flask_app.route('/admin/keys/generate-email')
    @login_required
    def generate_key_email_route():
        try:
            user_id = int(request.args.get('user_id'))
        except Exception:
            return jsonify({"ok": False, "error": "invalid user_id"}), 400
        try:
            user = get_user(user_id) or {}
            raw_username = (user.get('username') or f'user{user_id}').lower()
            import re
            username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
            base_local = f"{username_slug}"
            candidate_local = base_local
            attempt = 1
            while True:
                candidate_email = f"{candidate_local}@bot.local"
                if not get_key_by_email(candidate_email):
                    break
                attempt += 1
                candidate_local = f"{base_local}-{attempt}"
            return jsonify({"ok": True, "email": candidate_email})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/<int:key_id>/delete', methods=['POST'])
    @login_required
    def delete_key_route(key_id: int):
        # пытаемся удалить с сервера и из БД
        try:
            key = get_key_by_id(key_id)
            if key:
                try:
                    asyncio.run(xui_api.delete_client_on_host(key['host_name'], key['key_email']))
                except Exception:
                    pass
        except Exception:
            pass
        ok = delete_key_by_id(key_id)
        flash('Ключ удалён.' if ok else 'Не удалось удалить ключ.', 'success' if ok else 'danger')
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/<int:key_id>/adjust-expiry', methods=['POST'])
    @login_required
    def adjust_key_expiry_route(key_id: int):
        try:
            delta_days = int(request.form.get('delta_days', '0'))
        except Exception:
            return jsonify({"ok": False, "error": "invalid_delta"}), 400
        key = get_key_by_id(key_id)
        if not key:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            # Текущая дата истечения
            cur_expiry = key.get('expiry_date')
            from datetime import datetime, timedelta
            if isinstance(cur_expiry, str):
                try:
                    cur_dt = datetime.fromisoformat(cur_expiry)
                except Exception:
                    # fallback: если в БД дата как 'YYYY-MM-DD HH:MM:SS'
                    try:
                        cur_dt = datetime.strptime(cur_expiry, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        cur_dt = datetime.utcnow()
            else:
                cur_dt = cur_expiry or datetime.utcnow()
            new_dt = cur_dt + timedelta(days=delta_days)
            new_ms = int(new_dt.timestamp() * 1000)

            # 1) Применяем новый срок на 3xui (чтобы дата в панели совпадала с реальной)
            try:
                result = asyncio.run(xui_api.create_or_update_key_on_host(
                    host_name=key.get('host_name'),
                    email=key.get('key_email'),
                    expiry_timestamp_ms=new_ms
                ))
            except Exception as e:
                result = None
            if not result or not result.get('expiry_timestamp_ms'):
                return jsonify({"ok": False, "error": "xui_update_failed"}), 500

            # 2) Сохраняем в БД (обновляем UUID, если изменился, и дату истечения)
            client_uuid = result.get('client_uuid') or key.get('xui_client_uuid') or ''
            update_key_info(key_id, client_uuid, int(result.get('expiry_timestamp_ms')))

            # Уведомим пользователя о продлении/сокращении срока
            try:
                user_id = key.get('user_id')
                new_ms_final = int(result.get('expiry_timestamp_ms'))
                from datetime import datetime as _dt
                new_dt_local = _dt.fromtimestamp(new_ms_final/1000)
                text = (
                    "🗓️ Срок вашего VPN-ключа изменён администратором.\n"
                    f"Хост: {key.get('host_name')}\n"
                    f"Email ключа: {key.get('key_email')}\n"
                    f"Новая дата истечения: {new_dt_local.strftime('%Y-%m-%d %H:%M')}"
                )
                if user_id:
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                    elif bot:
                        asyncio.run(bot.send_message(chat_id=user_id, text=text))
            except Exception:
                pass

            return jsonify({"ok": True, "new_expiry_ms": int(result.get('expiry_timestamp_ms'))})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/sweep-expired', methods=['POST'])
    @login_required
    def sweep_expired_keys_route():
        from datetime import datetime
        removed = 0
        failed = 0
        now = datetime.utcnow()
        keys = get_all_keys()
        for k in keys:
            exp = k.get('expiry_date')
            exp_dt = None
            try:
                if isinstance(exp, str):
                    try:
                        from datetime import datetime as dt
                        exp_dt = dt.fromisoformat(exp)
                    except Exception:
                        try:
                            exp_dt = datetime.strptime(exp, '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            exp_dt = None
                else:
                    exp_dt = exp
            except Exception:
                exp_dt = None
            if not exp_dt or exp_dt > now:
                continue
            # Истёкший — пробуем удалить на сервере и в БД, уведомляем пользователя
            try:
                try:
                    asyncio.run(xui_api.delete_client_on_host(k.get('host_name'), k.get('key_email')))
                except Exception:
                    pass
                delete_key_by_id(k.get('key_id'))
                removed += 1
                # Уведомление пользователю о автоудалении
                try:
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    text = (
                        "Ваш ключ был автоматически удалён по истечении срока.\n"
                        f"Хост: {k.get('host_name')}\nEmail: {k.get('key_email')}\n"
                        "При необходимости вы можете оформить новый ключ."
                    )
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=k.get('user_id'), text=text), loop)
                    else:
                        asyncio.run(bot.send_message(chat_id=k.get('user_id'), text=text))
                except Exception:
                    pass
            except Exception:
                failed += 1
        flash(f"Удалено истёкших ключей: {removed}. Ошибок: {failed}.", 'success' if failed == 0 else 'warning')
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/<int:key_id>/comment', methods=['POST'])
    @login_required
    def update_key_comment_route(key_id: int):
        comment = (request.form.get('comment') or '').strip()
        ok = update_key_comment(key_id, comment)
        flash('Комментарий обновлён.' if ok else 'Не удалось обновить комментарий.', 'success' if ok else 'danger')
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/balance')
    @login_required
    def admin_balance_page():
        try:
            user_id = request.args.get('user_id', type=int)
        except Exception:
            user_id = None
        user = None
        balance = None
        referrals = []
        if user_id:
            try:
                user = get_user(user_id)
                balance = get_balance(user_id)
                referrals = get_referrals_for_user(user_id)
            except Exception:
                pass
        common_data = get_common_template_data()
        return render_template('admin_balance.html', user=user, balance=balance, referrals=referrals, **common_data)

    @flask_app.route('/support')
    @login_required
    def support_list_page():
        status = request.args.get('status')
        page = request.args.get('page', 1, type=int)
        per_page = 12
        tickets, total = get_tickets_paginated(page=page, per_page=per_page, status=status if status in ['open', 'closed'] else None)
        total_pages = ceil(total / per_page) if per_page else 1
        open_count = get_open_tickets_count()
        closed_count = get_closed_tickets_count()
        all_count = get_all_tickets_count()
        common_data = get_common_template_data()
        return render_template(
            'support.html',
            tickets=tickets,
            current_page=page,
            total_pages=total_pages,
            filter_status=status,
            open_count=open_count,
            closed_count=closed_count,
            all_count=all_count,
            **common_data
        )

    @flask_app.route('/support/<int:ticket_id>', methods=['GET', 'POST'])
    @login_required
    def support_ticket_page(ticket_id):
        ticket = get_ticket(ticket_id)
        if not ticket:
            flash('Тикет не найден.', 'danger')
            return redirect(url_for('support_list_page'))

        if request.method == 'POST':
            message = (request.form.get('message') or '').strip()
            action = request.form.get('action')
            if action == 'reply':
                if not message:
                    flash('Сообщение не может быть пустым.', 'warning')
                else:
                    add_support_message(ticket_id, sender='admin', content=message)
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = f"Ответ по тикету #{ticket_id}:\n\n{message}"
                            asyncio.run_coroutine_threadsafe(bot.send_message(user_chat_id, text), loop)
                        else:
                            logger.error("Support reply: support bot or event loop is not available; message not sent to user.")
                    except Exception as e:
                        logger.error(f"Support reply: failed to send Telegram message to user {ticket.get('user_id')} via support-bot: {e}", exc_info=True)
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            text = f"💬 Ответ админа из панели по тикету #{ticket_id}:\n\n{message}"
                            asyncio.run_coroutine_threadsafe(
                                bot.send_message(chat_id=int(forum_chat_id), text=text, message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Support reply: failed to mirror message to forum thread for ticket {ticket_id}: {e}")
                    flash('Ответ отправлен.', 'success')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
            elif action == 'close':
                if ticket.get('status') != 'closed' and set_ticket_status(ticket_id, 'closed'):
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            asyncio.run_coroutine_threadsafe(
                                bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Support close: failed to close forum topic for ticket {ticket_id}: {e}")
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = f"✅ Ваш тикет #{ticket_id} был закрыт администратором. Вы можете создать новое обращение при необходимости."
                            asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                    except Exception as e:
                        logger.warning(f"Support close: failed to notify user {ticket.get('user_id')} about closing ticket #{ticket_id}: {e}")
                    flash('Тикет закрыт.', 'success')
                else:
                    flash('Не удалось закрыть тикет.', 'danger')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
            elif action == 'open':
                if ticket.get('status') != 'open' and set_ticket_status(ticket_id, 'open'):
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            asyncio.run_coroutine_threadsafe(
                                bot.reopen_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Support open: failed to reopen forum topic for ticket {ticket_id}: {e}")
                    # Notify user
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = f"🔓 Ваш тикет #{ticket_id} снова открыт. Вы можете продолжить переписку."
                            asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                    except Exception as e:
                        logger.warning(f"Support open: failed to notify user {ticket.get('user_id')} about opening ticket #{ticket_id}: {e}")
                    flash('Тикет открыт.', 'success')
                else:
                    flash('Не удалось открыть тикет.', 'danger')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))

        messages = get_ticket_messages(ticket_id)
        common_data = get_common_template_data()
        return render_template('ticket.html', ticket=ticket, messages=messages, **common_data)

    @flask_app.route('/support/<int:ticket_id>/messages.json')
    @login_required
    def support_ticket_messages_api(ticket_id):
        ticket = get_ticket(ticket_id)
        if not ticket:
            return jsonify({"error": "not_found"}), 404
        messages = get_ticket_messages(ticket_id) or []
        items = [
            {
                "sender": m.get('sender'),
                "content": m.get('content'),
                "created_at": m.get('created_at')
            }
            for m in messages
        ]
        return jsonify({
            "ticket_id": ticket_id,
            "status": ticket.get('status'),
            "messages": items
        })

    @flask_app.route('/support/<int:ticket_id>/delete', methods=['POST'])
    @login_required
    def delete_support_ticket_route(ticket_id: int):
        ticket = get_ticket(ticket_id)
        if not ticket:
            flash('Тикет не найден.', 'danger')
            return redirect(url_for('support_list_page'))
        try:
            bot = _support_bot_controller.get_bot_instance()
            loop = current_app.config.get('EVENT_LOOP')
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        bot.delete_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                        loop
                    )
                    fut.result(timeout=5)
                except Exception as e:
                    logger.warning(f"Delete forum topic failed for ticket {ticket_id} (chat {forum_chat_id}, thread {thread_id}): {e}. Trying to close topic as fallback.")
                    try:
                        fut2 = asyncio.run_coroutine_threadsafe(
                            bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                            loop
                        )
                        fut2.result(timeout=5)
                    except Exception as e2:
                        logger.warning(f"Fallback close forum topic also failed for ticket {ticket_id}: {e2}")
            else:
                logger.error("Support delete: support bot or event loop unavailable, or missing forum ids; topic not deleted.")
        except Exception as e:
            logger.warning(f"Failed to process forum topic deletion for ticket {ticket_id} before deletion: {e}")
        if delete_ticket(ticket_id):
            flash(f"Тикет #{ticket_id} удалён.", 'success')
            return redirect(url_for('support_list_page'))
        else:
            flash(f"Не удалось удалить тикет #{ticket_id}.", 'danger')
            return redirect(url_for('support_ticket_page', ticket_id=ticket_id))

    @flask_app.route('/settings', methods=['GET', 'POST'])
    @login_required
    def settings_page():
        if request.method == 'POST':
            if 'panel_password' in request.form and request.form.get('panel_password'):
                update_setting('panel_password', request.form.get('panel_password'))

            for checkbox_key in ['force_subscription', 'sbp_enabled', 'trial_enabled', 'enable_referrals', 'enable_fixed_referral_bonus']:
                values = request.form.getlist(checkbox_key)
                value = values[-1] if values else 'false'
                update_setting(checkbox_key, 'true' if value == 'true' else 'false')

            for key in ALL_SETTINGS_KEYS:
                if key in ['panel_password', 'force_subscription', 'sbp_enabled', 'trial_enabled', 'enable_referrals', 'enable_fixed_referral_bonus']:
                    continue
                update_setting(key, request.form.get(key, ''))

            # Единый тост об успехе сохранения настроек
            flash('Настройки сохранены.', 'success')
            next_hash = (request.form.get('next_hash') or '').strip() or '#panel'
            next_tab = (next_hash[1:] if next_hash.startswith('#') else next_hash) or 'panel'
            return redirect(url_for('settings_page', tab=next_tab))

        current_settings = get_all_settings()
        hosts = get_all_hosts()
        for host in hosts:
            host['plans'] = get_plans_for_host(host['host_name'])
        
        common_data = get_common_template_data()
        return render_template('settings.html', settings=current_settings, hosts=hosts, **common_data)

    @flask_app.route('/update-host-subscription', methods=['POST'])
    @login_required
    def update_host_subscription_route():
        host_name = (request.form.get('host_name') or '').strip()
        sub_url = (request.form.get('host_subscription_url') or '').strip()
        if not host_name:
            flash('Не указан хост для обновления ссылки подписки.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_subscription_url(host_name, sub_url or None)
        if ok:
            flash('Ссылка подписки для хоста обновлена.', 'success')
        else:
            flash('Не удалось обновить ссылку подписки для хоста (возможно, хост не найден).', 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-host-url', methods=['POST'])
    @login_required
    def update_host_url_route():
        host_name = (request.form.get('host_name') or '').strip()
        new_url = (request.form.get('host_url') or '').strip()
        if not host_name or not new_url:
            flash('Укажите имя хоста и новый URL.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_url(host_name, new_url)
        flash('URL хоста обновлён.' if ok else 'Не удалось обновить URL хоста.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/rename-host', methods=['POST'])
    @login_required
    def rename_host_route():
        old_name = (request.form.get('old_host_name') or '').strip()
        new_name = (request.form.get('new_host_name') or '').strip()
        if not old_name or not new_name:
            flash('Введите старое и новое имя хоста.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_name(old_name, new_name)
        flash('Имя хоста обновлено.' if ok else 'Не удалось переименовать хост.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/start-support-bot', methods=['POST'])
    @login_required
    def start_support_bot_route():
        loop = current_app.config.get('EVENT_LOOP')
        if loop and loop.is_running():
            _support_bot_controller.set_loop(loop)
        result = _support_bot_controller.start()
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('settings_page'))

    def _wait_for_stop(controller, timeout: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            status = controller.get_status() or {}
            if not status.get('is_running'):
                return True
            time.sleep(0.1)
        return False

    @flask_app.route('/stop-support-bot', methods=['POST'])
    @login_required
    def stop_support_bot_route():
        result = _support_bot_controller.stop()
        _wait_for_stop(_support_bot_controller)
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/start-bot', methods=['POST'])
    @login_required
    def start_bot_route():
        result = _bot_controller.start()
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/stop-bot', methods=['POST'])
    @login_required
    def stop_bot_route():
        result = _bot_controller.stop()
        _wait_for_stop(_bot_controller)
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/stop-both-bots', methods=['POST'])
    @login_required
    def stop_both_bots_route():
        main_result = _bot_controller.stop()
        support_result = _support_bot_controller.stop()

        statuses = []
        categories = []
        for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
            if res.get('status') == 'success':
                statuses.append(f"{name}: остановлен")
                categories.append('success')
            else:
                statuses.append(f"{name}: ошибка — {res.get('message')}")
                categories.append('danger')
        _wait_for_stop(_bot_controller)
        _wait_for_stop(_support_bot_controller)
        category = 'danger' if 'danger' in categories else 'success'
        flash(' | '.join(statuses), category)
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/start-both-bots', methods=['POST'])
    @login_required
    def start_both_bots_route():
        main_result = _bot_controller.start()
        loop = current_app.config.get('EVENT_LOOP')
        if loop and loop.is_running():
            _support_bot_controller.set_loop(loop)
        support_result = _support_bot_controller.start()

        statuses = []
        categories = []
        for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
            if res.get('status') == 'success':
                statuses.append(f"{name}: запущен")
                categories.append('success')
            else:
                statuses.append(f"{name}: ошибка — {res.get('message')}")
                categories.append('danger')
        category = 'danger' if 'danger' in categories else 'success'
        flash(' | '.join(statuses), category)
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/users/ban/<int:user_id>', methods=['POST'])
    @login_required
    def ban_user_route(user_id):
        ban_user(user_id)
        flash(f'Пользователь {user_id} был заблокирован.', 'success')
        return redirect(url_for('users_page'))

    @flask_app.route('/users/unban/<int:user_id>', methods=['POST'])
    @login_required
    def unban_user_route(user_id):
        unban_user(user_id)
        flash(f'Пользователь {user_id} был разблокирован.', 'success')
        return redirect(url_for('users_page'))

    @flask_app.route('/users/revoke/<int:user_id>', methods=['POST'])
    @login_required
    def revoke_keys_route(user_id):
        keys_to_revoke = get_user_keys(user_id)
        success_count = 0
        total = len(keys_to_revoke)

        for key in keys_to_revoke:
            result = asyncio.run(xui_api.delete_client_on_host(key['host_name'], key['key_email']))
            if result:
                success_count += 1

        # удаляем из БД все ключи пользователя
        delete_user_keys(user_id)

        # уведомление пользователю в Telegram
        try:
            bot = _bot_controller.get_bot_instance()
            if bot:
                text = (
                    "❌ Ваши VPN‑ключи были отозваны администратором.\n"
                    f"Всего ключей: {total}\n"
                    f"Отозвано: {success_count}"
                )
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text))
        except Exception:
            pass

        message = (
            f"Все {total} ключей для пользователя {user_id} были успешно отозваны." if success_count == total
            else f"Удалось отозвать {success_count} из {total} ключей для пользователя {user_id}. Проверьте логи."
        )
        category = 'success' if success_count == total else 'warning'

        # Если это AJAX-запрос (из таблицы пользователей) — возвращаем JSON
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": success_count == total, "message": message, "revoked": success_count, "total": total}), 200

        flash(message, category)
        return redirect(url_for('users_page'))

    @flask_app.route('/add-host', methods=['POST'])
    @login_required
    def add_host_route():
        create_host(
            name=request.form['host_name'],
            url=request.form['host_url'],
            user=request.form['host_username'],
            passwd=request.form['host_pass'],
            inbound=int(request.form['host_inbound_id']),
            subscription_url=(request.form.get('host_subscription_url') or '').strip() or None
        )
        flash(f"Хост '{request.form['host_name']}' успешно добавлен.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/delete-host/<host_name>', methods=['POST'])
    @login_required
    def delete_host_route(host_name):
        delete_host(host_name)
        flash(f"Хост '{host_name}' и все его тарифы были удалены.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/add-plan', methods=['POST'])
    @login_required
    def add_plan_route():
        create_plan(
            host_name=request.form['host_name'],
            plan_name=request.form['plan_name'],
            months=int(request.form['months']),
            price=float(request.form['price'])
        )
        flash(f"Новый тариф для хоста '{request.form['host_name']}' добавлен.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/delete-plan/<int:plan_id>', methods=['POST'])
    @login_required
    def delete_plan_route(plan_id):
        delete_plan(plan_id)
        flash("Тариф успешно удален.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-plan/<int:plan_id>', methods=['POST'])
    @login_required
    def update_plan_route(plan_id):
        plan_name = (request.form.get('plan_name') or '').strip()
        months = request.form.get('months')
        price = request.form.get('price')
        try:
            months_int = int(months)
            price_float = float(price)
        except (TypeError, ValueError):
            flash('Некорректные значения для месяцев или цены.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))

        if not plan_name:
            flash('Название тарифа не может быть пустым.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))

        ok = update_plan(plan_id, plan_name, months_int, price_float)
        if ok:
            flash('Тариф обновлён.', 'success')
        else:
            flash('Не удалось обновить тариф (возможно, он не найден).', 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @csrf.exempt
    @flask_app.route('/yookassa-webhook', methods=['POST'])
    def yookassa_webhook_handler():
        try:
            event_json = request.json
            if event_json.get("event") == "payment.succeeded":
                metadata = event_json.get("object", {}).get("metadata", {})
                
                bot = _bot_controller.get_bot_instance()
                payment_processor = handlers.process_successful_payment

                if metadata and bot is not None and payment_processor is not None:
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                    else:
                        logger.error("YooKassa webhook: Event loop is not available!")
            return 'OK', 200
        except Exception as e:
            logger.error(f"Error in yookassa webhook handler: {e}", exc_info=True)
            return 'Error', 500
        
    @csrf.exempt
    @flask_app.route('/cryptobot-webhook', methods=['POST'])
    def cryptobot_webhook_handler():
        try:
            request_data = request.json
            
            if request_data and request_data.get('update_type') == 'invoice_paid':
                payload_data = request_data.get('payload', {})
                
                payload_string = payload_data.get('payload')
                
                if not payload_string:
                    logger.warning("CryptoBot Webhook: Received paid invoice but payload was empty.")
                    return 'OK', 200

                parts = payload_string.split(':')
                if len(parts) < 9:
                    logger.error(f"cryptobot Webhook: Invalid payload format received: {payload_string}")
                    return 'Error', 400

                metadata = {
                    "user_id": parts[0],
                    "months": parts[1],
                    "price": parts[2],
                    "action": parts[3],
                    "key_id": parts[4],
                    "host_name": parts[5],
                    "plan_id": parts[6],
                    "customer_email": parts[7] if parts[7] != 'None' else None,
                    "payment_method": parts[8]
                }
                
                bot = _bot_controller.get_bot_instance()
                loop = current_app.config.get('EVENT_LOOP')
                payment_processor = handlers.process_successful_payment

                if bot and loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                else:
                    logger.error("cryptobot Webhook: Could not process payment because bot or event loop is not running.")

            return 'OK', 200
            
        except Exception as e:
            logger.error(f"Error in cryptobot webhook handler: {e}", exc_info=True)
            return 'Error', 500
        
    @csrf.exempt
    @flask_app.route('/heleket-webhook', methods=['POST'])
    def heleket_webhook_handler():
        try:
            data = request.json
            logger.info(f"Received Heleket webhook: {data}")

            api_key = get_setting("heleket_api_key")
            if not api_key: return 'Error', 500

            sign = data.pop("sign", None)
            if not sign: return 'Error', 400
                
            sorted_data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
            
            base64_encoded = base64.b64encode(sorted_data_str.encode()).decode()
            raw_string = f"{base64_encoded}{api_key}"
            expected_sign = hashlib.md5(raw_string.encode()).hexdigest()

            if not compare_digest(expected_sign, sign):
                logger.warning("Heleket webhook: Invalid signature.")
                return 'Forbidden', 403

            if data.get('status') in ["paid", "paid_over"]:
                metadata_str = data.get('description')
                if not metadata_str: return 'Error', 400
                
                metadata = json.loads(metadata_str)
                
                bot = _bot_controller.get_bot_instance()
                loop = current_app.config.get('EVENT_LOOP')
                payment_processor = handlers.process_successful_payment

                if bot and loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
            
            return 'OK', 200
        except Exception as e:
            logger.error(f"Error in heleket webhook handler: {e}", exc_info=True)
            return 'Error', 500
        
    @csrf.exempt
    @flask_app.route('/ton-webhook', methods=['POST'])
    def ton_webhook_handler():
        try:
            data = request.json
            logger.info(f"Received TonAPI webhook: {data}")

            if 'tx_id' in data:
                account_id = data.get('account_id')
                for tx in data.get('in_progress_txs', []) + data.get('txs', []):
                    in_msg = tx.get('in_msg')
                    if in_msg and in_msg.get('decoded_comment'):
                        payment_id = in_msg['decoded_comment']
                        amount_nano = int(in_msg.get('value', 0))
                        amount_ton = float(amount_nano / 1_000_000_000)

                        metadata = find_and_complete_ton_transaction(payment_id, amount_ton)
                        
                        if metadata:
                            logger.info(f"TON Payment successful for payment_id: {payment_id}")
                            bot = _bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            payment_processor = handlers.process_successful_payment

                            if bot and loop and loop.is_running():
                                asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
            
            return 'OK', 200
        except Exception as e:
            logger.error(f"Error in ton webhook handler: {e}", exc_info=True)
            return 'Error', 500

    return flask_app