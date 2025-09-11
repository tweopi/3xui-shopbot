import os
import json
import subprocess
from flask import Flask, request, jsonify
import base64

app = Flask(__name__)

AGENT_TOKEN = os.getenv("AGENT_TOKEN", "").strip()
BASIC_USER = os.getenv("BASIC_USER", "").strip()
BASIC_PASS = os.getenv("BASIC_PASS", "").strip()

def _check_auth(req) -> bool:
    """Проверка авторизации. Разрешаем ЛИБО Bearer-токен, ЛИБО Basic Auth.
    Если ни один метод не настроен — доступ открыт (для простых тестов).
    """
    has_bearer = bool(AGENT_TOKEN)
    has_basic = bool(BASIC_USER and BASIC_PASS)
    # если оба механизма отключены — не требуем авторизацию
    if not (has_bearer or has_basic):
        return True
    auth = req.headers.get("Authorization", "").strip()
    if has_bearer and auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1]
        if token == AGENT_TOKEN:
            return True
    if has_basic and auth.startswith("Basic "):
        try:
            raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
            user, pwd = raw.split(":", 1)
            if user == BASIC_USER and pwd == BASIC_PASS:
                return True
        except Exception:
            pass
    return False

@app.route("/", methods=["GET"]) 
def index():
    return jsonify({
        "ok": True,
        "name": "3xui-shopbot speedtest agent",
        "version": "1.0.0",
        "endpoints": {
            "GET /health": "Проверка доступности",
            "POST /speedtest": "Запуск Ookla speedtest, JSON-ответ"
        },
        "auth": {
            "bearer": bool(AGENT_TOKEN),
            "basic": bool(BASIC_USER and BASIC_PASS)
        }
    })

@app.route("/health", methods=["GET"]) 
def health():
    return jsonify({"ok": True, "version": "1.0.0"})

@app.route("/speedtest", methods=["POST"]) 
def speedtest_route():
    # Авторизация: Bearer или Basic (если настроены)
    if not _check_auth(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        # Run Ookla speedtest with JSON output
        proc = subprocess.run([
            "speedtest",
            "--accept-license",
            "--accept-gdpr",
            "-f", "json"
        ], capture_output=True, text=True, timeout=240)
        if proc.returncode != 0:
            return jsonify({"ok": False, "error": proc.stderr.strip() or "speedtest_failed"}), 502
        data = json.loads(proc.stdout)
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "timeout"}), 504
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    # Extract normalized fields similar to panel expectations
    def _safe_int(x, default=0):
        try:
            return int(x)
        except Exception:
            return default
    def _safe_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    dl_bw = _safe_int((data.get('download') or {}).get('bandwidth'), 0)  # bytes/sec
    ul_bw = _safe_int((data.get('upload') or {}).get('bandwidth'), 0)    # bytes/sec
    ping_ms = _safe_float((data.get('ping') or {}).get('latency'), 0.0)
    isp = data.get('isp') or data.get('ispName') or 'Unknown'
    external_ip = (data.get('interface') or {}).get('externalIp') or 'Unknown'
    server = data.get('server') or {}
    server_name = server.get('name') or server.get('host') or 'Unknown'
    server_country = server.get('country') or server.get('location') or 'Unknown'
    timestamp = data.get('timestamp')

    payload = {
        "timestamp": timestamp,
        "download_speed": round(dl_bw * 8 / 1_000_000, 2),
        "upload_speed": round(ul_bw * 8 / 1_000_000, 2),
        "ping": round(ping_ms, 2),
        "server_name": server_name,
        "server_country": server_country,
        "isp": isp,
        "external_ip": external_ip
    }
    return jsonify(payload)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
