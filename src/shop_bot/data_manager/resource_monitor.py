import os
import json
import time
import logging
import shutil
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import paramiko

from shop_bot.data_manager import database

logger = logging.getLogger(__name__)


# -------- Local metrics (container/host running the panel) --------
def _read_proc_meminfo() -> Tuple[int | None, int | None]:
    total_kb = None
    avail_kb = None
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    parts = line.split()
                    total_kb = int(parts[1])
                elif line.startswith('MemAvailable:'):
                    parts = line.split()
                    avail_kb = int(parts[1])
                if total_kb is not None and avail_kb is not None:
                    break
    except Exception:
        pass
    return total_kb, avail_kb


def _get_uptime_seconds_fallback() -> float | None:
    try:
        with open('/proc/uptime', 'r') as f:
            txt = f.read().strip().split()
            return float(txt[0])
    except Exception:
        return None


def get_local_metrics() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        'ok': True,
        'source': 'local',
        'cpu_percent': None,
        'cpu_count': os.cpu_count() or 1,
        'loadavg': None,  # (1m, 5m, 15m)
        'mem_total': None,
        'mem_used': None,
        'mem_available': None,
        'mem_percent': None,
        'disk_total': None,
        'disk_used': None,
        'disk_free': None,
        'disk_percent': None,
        'uptime_seconds': None,
        'network_sent': None,
        'network_recv': None,
        'network_packets_sent': None,
        'network_packets_recv': None,
        'error': None,
    }

    # Load average
    try:
        if hasattr(os, 'getloadavg'):
            la = os.getloadavg()
            out['loadavg'] = {'1m': la[0], '5m': la[1], '15m': la[2]}
    except Exception:
        pass

    # Disk usage (root)
    try:
        disk_path = '/'
        if os.name == 'nt':
            disk_path = os.environ.get('SystemDrive', 'C:') + '\\'
        du = shutil.disk_usage(disk_path)
        out['disk_total'] = int(du.total)
        out['disk_used'] = int(du.used)
        out['disk_free'] = int(du.free)
        out['disk_percent'] = round((du.used / du.total) * 100.0, 2) if du.total else None
    except Exception:
        pass

    # Memory and CPU via psutil if available
    try:
        import psutil  # type: ignore

        out['cpu_percent'] = float(psutil.cpu_percent(interval=0.1))
        vm = psutil.virtual_memory()
        out['mem_total'] = int(vm.total)
        out['mem_used'] = int(vm.used)
        out['mem_available'] = int(vm.available)
        out['mem_percent'] = float(vm.percent)
        
        # Network data
        try:
            net_io = psutil.net_io_counters()
            out['network_sent'] = int(net_io.bytes_sent)
            out['network_recv'] = int(net_io.bytes_recv)
            out['network_packets_sent'] = int(net_io.packets_sent)
            out['network_packets_recv'] = int(net_io.packets_recv)
        except Exception:
            pass
            
        try:
            out['uptime_seconds'] = float(time.time() - psutil.boot_time())
        except Exception:
            out['uptime_seconds'] = _get_uptime_seconds_fallback()
    except Exception:
        # Fallbacks without psutil
        total_kb, avail_kb = _read_proc_meminfo()
        if total_kb is not None and avail_kb is not None:
            total = total_kb * 1024
            avail = avail_kb * 1024
            used = total - avail
            out['mem_total'] = total
            out['mem_available'] = avail
            out['mem_used'] = used
            out['mem_percent'] = round((used / total) * 100.0, 2) if total else None
        out['cpu_percent'] = None
        out['uptime_seconds'] = _get_uptime_seconds_fallback()
        if out['mem_total'] is None:
            out['ok'] = False
            out['error'] = 'psutil not installed and /proc parsing failed'

    return out


# -------- Remote host metrics (via SSH) --------
def _ssh_connect(host_row: dict) -> paramiko.SSHClient:
    ssh_host = (host_row.get('ssh_host') or '').strip()
    ssh_port = int(host_row.get('ssh_port') or 22)
    ssh_user = (host_row.get('ssh_user') or '').strip()
    ssh_password = host_row.get('ssh_password')
    ssh_key_path = (host_row.get('ssh_key_path') or '').strip() or None

    if not ssh_host or not ssh_user:
        raise RuntimeError('SSH settings are not configured for host')

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = None
    if ssh_key_path:
        for KeyClass in (paramiko.RSAKey, paramiko.Ed25519Key):
            try:
                pkey = KeyClass.from_private_key_file(ssh_key_path)
                break
            except Exception:
                pkey = None
    ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, pkey=pkey, timeout=20)
    return ssh


def _ssh_exec(ssh: paramiko.SSHClient, cmd: str, timeout: int = 20) -> Tuple[int, str, str]:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='ignore')
    err = stderr.read().decode('utf-8', errors='ignore')
    rc = stdout.channel.recv_exit_status() if hasattr(stdout, 'channel') else 0
    return rc, out, err


def get_host_metrics_via_ssh(host_row: dict) -> Dict[str, Any]:
    res: Dict[str, Any] = {
        'ok': False,
        'host_name': host_row.get('host_name'),
        'cpu_percent': None,
        'cpu_count': None,
        'loadavg': None,
        'mem_total': None,
        'mem_used': None,
        'mem_available': None,
        'mem_percent': None,
        'disk_total': None,
        'disk_used': None,
        'disk_free': None,
        'disk_percent': None,
        'uptime_seconds': None,
        'error': None,
    }
    try:
        ssh = _ssh_connect(host_row)
    except Exception as e:
        res['error'] = f'SSH connect failed: {e}'
        return res

    try:
        # CPU count
        rc, out, _ = _ssh_exec(ssh, 'nproc || getconf _NPROCESSORS_ONLN || echo 1')
        try:
            res['cpu_count'] = int((out or '1').strip().splitlines()[0])
        except Exception:
            res['cpu_count'] = 1

        # loadavg
        rc, out, _ = _ssh_exec(ssh, 'cat /proc/loadavg || uptime')
        la = None
        try:
            parts = (out or '').strip().split()
            la = {
                '1m': float(parts[0]),
                '5m': float(parts[1]),
                '15m': float(parts[2]),
            }
        except Exception:
            la = None
        res['loadavg'] = la

        if res['cpu_count']:
            try:
                cpu_pct = (la.get('1m') / float(res['cpu_count'])) * 100.0 if la and la.get('1m') is not None else None
            except Exception:
                cpu_pct = None
            if cpu_pct is not None:
                if cpu_pct < 0:
                    cpu_pct = 0.0
                res['cpu_percent'] = round(min(cpu_pct, 100.0), 2)

        # meminfo
        rc, out, _ = _ssh_exec(ssh, "grep -E 'MemTotal:|MemAvailable:' /proc/meminfo || cat /proc/meminfo")
        total_kb = None
        avail_kb = None
        try:
            for line in out.splitlines():
                if line.startswith('MemTotal:'):
                    total_kb = int(line.split()[1])
                elif line.startswith('MemAvailable:'):
                    avail_kb = int(line.split()[1])
        except Exception:
            pass
        if total_kb is not None and avail_kb is not None:
            total = total_kb * 1024
            avail = avail_kb * 1024
            used = total - avail
            res['mem_total'] = total
            res['mem_available'] = avail
            res['mem_used'] = used
            res['mem_percent'] = round((used / total) * 100.0, 2) if total else None

        # disk usage (root)
        rc, out, _ = _ssh_exec(ssh, "LC_ALL=C df -P -B1 / | tail -n 1")
        try:
            parts = out.strip().split()
            if len(parts) >= 5:
                total = int(parts[1])
                used = int(parts[2])
                avail = int(parts[3])
                res['disk_total'] = total
                res['disk_used'] = used
                res['disk_free'] = avail
                res['disk_percent'] = round((used / total) * 100.0, 2) if total else None
        except Exception:
            pass

        # uptime seconds
        rc, out, _ = _ssh_exec(ssh, 'cat /proc/uptime || uptime -s')
        up = None
        try:
            up = float(out.strip().split()[0])
        except Exception:
            # try parse boot time
            try:
                rc, out2, _ = _ssh_exec(ssh, 'uptime -s')
                boot_str = (out2 or '').strip()
                if boot_str:
                    try:
                        boot_dt = datetime.fromisoformat(boot_str)
                        up = (datetime.now() - boot_dt).total_seconds()
                    except Exception:
                        up = None
            except Exception:
                up = None
        res['uptime_seconds'] = up

        res['ok'] = True
    except Exception as e:
        res['ok'] = False
        res['error'] = str(e)
    finally:
        try:
            ssh.close()
        except Exception:
            pass
    return res


def collect_hosts_metrics() -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    try:
        hosts = database.get_all_hosts()
    except Exception as e:
        return {'ok': False, 'items': [], 'error': f'get_all_hosts failed: {e}'}

    for h in hosts:
        # Проверяем наличие SSH настроек
        if h.get('ssh_host') and h.get('ssh_user'):
            # Хост с SSH - получаем метрики
            try:
                m = get_host_metrics_via_ssh(h)
            except Exception as e:
                m = {'ok': False, 'host_name': h.get('host_name'), 'error': str(e)}
        else:
            # Хост без SSH - показываем базовую информацию
            m = {
                'ok': False,
                'host_name': h.get('host_name'),
                'host_url': h.get('host_url'),
                'error': 'SSH не настроен',
                'cpu_percent': None,
                'mem_percent': None,
                'disk_percent': None,
                'uptime_seconds': None
            }
        items.append(m)

    return {'ok': True, 'items': items}
