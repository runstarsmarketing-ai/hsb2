#!/usr/bin/env python3
"""
Enhanced AutoHost Telegram Bot with Flask Web Service
Features:
- Upload .py -> saved to UPLOAD_DIR and started on a free port (env PORT passed)
- Upload requirements.txt -> pip install -r (background)
- /list, /ports, /stop, /restart, /logs, /clearlogs, /status
- Auto-restart crashed scripts with cooldown/backoff
- Multiple admins with hosting limits
- Admin management system
- Resource monitoring and bot health checks
- Per-user bot limits
- Flask web service for Koyeb hosting
"""

import os
import time
import subprocess
import socket
import json
import threading
from typing import Dict, Tuple, Optional, List
from datetime import datetime, timedelta
import asyncio
from collections import deque
from flask import Flask, jsonify, request
import logging


try:
    import psutil
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8270301262:AAEldMmzcGK9sRbYk9WrFz6MqYE42EDnwlc")

# Where uploaded scripts and logs are stored
# Where uploaded scripts and logs are stored
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/tmp/uploaded_scripts")
LOG_DIR = os.path.join(UPLOAD_DIR, "logs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# Port pool
PORT_START = int(os.getenv("PORT_START", "5000"))
PORT_END = int(os.getenv("PORT_END", "6000"))

# Default limits
DEFAULT_BOT_LIMIT = int(os.getenv("DEFAULT_BOT_LIMIT", "5"))
MAX_BOT_LIMIT = int(os.getenv("MAX_BOT_LIMIT", "20"))

WEB_SERVICE_PORT = int(os.getenv("PORT", "8080"))  # Koyeb uses PORT env var

# runtime registries:
# running: name -> (Popen, port, log_path, last_start_ts, owner_id)
running: Dict[str, Tuple[subprocess.Popen, int, str, float, int]] = {}

# restart bookkeeping: name -> (attempt_count, last_attempt_ts)
_restart_info: Dict[str, Tuple[int, float]] = {}

# limits/timeouts
RESTART_MIN_DELAY = 3.0      # seconds minimum between restarts for same script
MAX_BACKOFF = 60.0           # max backoff between restarts
HEALTH_CHECK_INTERVAL = 30.0 # seconds between health checks
MONITOR_LOOP_INTERVAL = 20.0  # seconds between monitor loop runs

PYTHON_CMD = os.getenv("PYTHON_CMD", "python")  # python executable to run scripts

# Live logging configuration
LIVE_LOG_UPDATE_INTERVAL = 2.0  # seconds between live log updates
MAX_LIVE_LOG_LINES = 20  # maximum lines to show in live logs
LIVE_LOG_BUFFER_SIZE = 50  # maximum lines to keep in memory buffer

SCRIPT_LIVE_LOG_DURATION = 15.0  # seconds to show live logs for scripts
PIP_LIVE_LOG_FULL_DURATION = True  # pip logs stay live until completion

# Live logging tracking: message_id -> (chat_id, log_file_path, last_position, task, process, start_time, is_pip)
_live_logs: Dict[int, Dict] = {}

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ----------------------------------------
# Admin Management System
# ----------------------------------------

ADMIN_FILE = "admins.json"
SUPER_ADMIN_ID = 7665143902  # Your original admin ID - cannot be removed

class AdminManager:
    def __init__(self):
        self.admins = self.load_admins()
        # Ensure super admin exists
        if SUPER_ADMIN_ID not in self.admins:
            self.admins[SUPER_ADMIN_ID] = {
                "bot_limit": MAX_BOT_LIMIT,
                "added_by": SUPER_ADMIN_ID,
                "added_at": time.time(),
                "is_super": True
            }
            self.save_admins()

    def load_admins(self) -> Dict[int, Dict]:
        """Load admin data from file"""
        if os.path.exists(ADMIN_FILE):
            try:
                with open(ADMIN_FILE, "r") as f:
                    data = json.load(f)
                    # Convert string keys to int for backward compatibility
                    return {int(k): v for k, v in data.items()}
            except Exception:
                pass
        return {}

    def save_admins(self):
        """Save admin data to file"""
        try:
            with open(ADMIN_FILE, "w") as f:
                # Convert int keys to string for JSON
                data = {str(k): v for k, v in self.admins.items()}
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving admins: {e}")

    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return user_id in self.admins

    def is_super_admin(self, user_id: int) -> bool:
        """Check if user is super admin"""
        return user_id == SUPER_ADMIN_ID or self.admins.get(user_id, {}).get("is_super", False)

    def add_admin(self, user_id: int, added_by: int, bot_limit: int = None) -> bool:
        """Add new admin"""
        if user_id in self.admins:
            return False
        
        if bot_limit is None:
            bot_limit = DEFAULT_BOT_LIMIT
        
        bot_limit = min(bot_limit, MAX_BOT_LIMIT)
        
        self.admins[user_id] = {
            "bot_limit": bot_limit,
            "added_by": added_by,
            "added_at": time.time(),
            "is_super": False
        }
        self.save_admins()
        return True

    def remove_admin(self, user_id: int) -> bool:
        """Remove admin (cannot remove super admin)"""
        if user_id == SUPER_ADMIN_ID:
            return False
        if user_id not in self.admins:
            return False
        
        del self.admins[user_id]
        self.save_admins()
        return True

    def get_bot_limit(self, user_id: int) -> int:
        """Get bot limit for user"""
        return self.admins.get(user_id, {}).get("bot_limit", 0)

    def set_bot_limit(self, user_id: int, limit: int) -> bool:
        """Set bot limit for user"""
        if user_id not in self.admins:
            return False
        
        limit = min(limit, MAX_BOT_LIMIT)
        self.admins[user_id]["bot_limit"] = limit
        self.save_admins()
        return True

    def get_user_bot_count(self, user_id: int) -> int:
        """Count bots owned by user"""
        return sum(1 for (_, _, _, _, owner) in running.values() if owner == user_id)

    def can_host_more_bots(self, user_id: int) -> bool:
        """Check if user can host more bots"""
        current_count = self.get_user_bot_count(user_id)
        limit = self.get_bot_limit(user_id)
        return current_count < limit

    def list_admins(self) -> List[Tuple[int, Dict]]:
        """Get list of all admins"""
        return list(self.admins.items())

# Global admin manager
admin_manager = AdminManager()

@app.route('/')
def health_check():
    """Health check endpoint for Koyeb"""
    return jsonify({
        "status": "healthy",
        "service": "telegram-bot",
        "running_scripts": len(running),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health')
def detailed_health():
    """Detailed health check"""
    try:
        memory_info = {}
        if _HAS_PSUTIL:
            process = psutil.Process()
            memory_info = {
                "memory_percent": process.memory_percent(),
                "memory_mb": process.memory_info().rss / 1024 / 1024
            }
        
        return jsonify({
            "status": "healthy",
            "running_scripts": len(running),
            "active_live_logs": len(_live_logs),
            "admin_count": len(admin_manager.admins),
            "uptime": time.time(),
            **memory_info
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/stats')
def get_stats():
    """Get bot statistics"""
    return jsonify({
        "total_scripts": len(running),
        "scripts": [{"name": name, "port": port, "owner": owner} 
                   for name, (_, port, _, _, owner) in running.items()],
        "admins": len(admin_manager.admins),
        "live_logs": len(_live_logs)
    })

# --------- Helpers ----------
def is_admin(user_id: int) -> bool:
    return admin_manager.is_admin(user_id)

def safe_join_upload(filename: str) -> str:
    # prevent path traversal
    safe = filename.replace("..", "").replace("/", "_").replace("\\", "_")
    return os.path.join(UPLOAD_DIR, safe)

def log_path_for(name: str) -> str:
    safe = name.replace("/", "_")
    return os.path.join(LOG_DIR, f"{safe}.log")

def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.1)
        return s.connect_ex(("127.0.0.1", port)) == 0

def get_free_port() -> Optional[int]:
    used_ports = {p for (_, p, _, _, _) in running.values()}
    for p in range(PORT_START, PORT_END):
        if p in used_ports:
            continue
        if not port_in_use(p):
            return p
    return None

def start_process(file_path: str, script_name: str, port: Optional[int], owner_id: int) -> subprocess.Popen:
    env = os.environ.copy()
    if port is not None:
        env["PORT"] = str(port)

    lp = log_path_for(script_name)
    # ensure log file exists and append
    lf = open(lp, "a", buffering=1, encoding="utf-8", errors="replace")
    lf.write(f"\n--- START {time.strftime('%Y-%m-%d %H:%M:%S')} (PORT={env.get('PORT','-')}) (OWNER={owner_id}) ---\n")

    proc = subprocess.Popen(
        [PYTHON_CMD, file_path],
        stdout=lf,
        stderr=subprocess.STDOUT,
        env=env,
        cwd=os.path.dirname(file_path) or UPLOAD_DIR,
        start_new_session=True,
        close_fds=True
    )
    return proc

def backoff_delay(name: str) -> float:
    attempts, _ = _restart_info.get(name, (0, 0.0))
    delay = min(MAX_BACKOFF, (2 ** attempts))
    return max(RESTART_MIN_DELAY, delay)

def record_restart_attempt(name: str):
    attempts, _ = _restart_info.get(name, (0, 0.0))
    _restart_info[name] = (attempts + 1, time.time())

def reset_restart_info(name: str):
    if name in _restart_info:
        _restart_info.pop(name, None)

def get_process_stats(proc: subprocess.Popen) -> Dict:
    """Get process statistics if psutil is available"""
    if not _HAS_PSUTIL:
        return {}
    
    try:
        p = psutil.Process(proc.pid)
        return {
            "cpu_percent": p.cpu_percent(),
            "memory_mb": p.memory_info().rss / 1024 / 1024,
            "status": p.status(),
            "create_time": p.create_time()
        }
    except Exception:
        return {}

async def _send_chunked_message(bot, chat_id: int, text: str, parse_mode: str = None, max_length: int = 4000):
    """Send long messages in chunks to avoid Telegram limits"""
    if len(text) <= max_length:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        except Exception as e:
            # If parsing fails, try without parse_mode
            if parse_mode:
                try:
                    await bot.send_message(chat_id=chat_id, text=text)
                except Exception:
                    await bot.send_message(chat_id=chat_id, text=f"⚠️ Could not send message: {str(e)[:200]}")
            else:
                await bot.send_message(chat_id=chat_id, text=f"⚠️ Could not send message: {str(e)[:200]}")
        return
    
    # Split into chunks
    chunks = []
    current_chunk = ""
    lines = text.split('\n')
    
    for line in lines:
        if len(current_chunk) + len(line) + 1 <= max_length:
            current_chunk += line + '\n'
        else:
            if current_chunk:
                chunks.append(current_chunk.rstrip())
            current_chunk = line + '\n'
    
    if current_chunk:
        chunks.append(current_chunk.rstrip())
    
    # Send chunks
    for i, chunk in enumerate(chunks):
        try:
            prefix = f"📄 Part {i+1}/{len(chunks)}\n\n" if len(chunks) > 1 else ""
            await bot.send_message(chat_id=chat_id, text=prefix + chunk, parse_mode=parse_mode)
            await asyncio.sleep(0.1)  # Small delay between chunks
        except Exception as e:
            # If parsing fails, try without parse_mode
            if parse_mode:
                try:
                    await bot.send_message(chat_id=chat_id, text=prefix + chunk)
                except Exception:
                    await bot.send_message(chat_id=chat_id, text=f"⚠️ Could not send chunk {i+1}: {str(e)[:200]}")
            else:
                await bot.send_message(chat_id=chat_id, text=f"⚠️ Could not send chunk {i+1}: {str(e)[:200]}")

def _escape_markdown_chars(text: str) -> str:
    """Escape special characters that can break Markdown parsing"""
    # Replace problematic characters that can break Telegram's Markdown parser
    chars_to_escape = ['`', '*', '_', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in chars_to_escape:
        text = text.replace(char, f'\\{char}')
    return _escape_markdown_chars

# ---------- PTB Handlers ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text(
            "❌ You are *not authorized* to use this bot.\n"
            "📩 Contact admin for access. @Vxxwo",
            parse_mode="Markdown"
        )
        return

    bot_limit = admin_manager.get_bot_limit(user_id)
    current_bots = admin_manager.get_user_bot_count(user_id)
    is_super = admin_manager.is_super_admin(user_id)
    
    msg = (
        "👋 *@Vxxwo Hosting Bot*\n\n"
        f"📊 Your bot limit: *{current_bots}/{bot_limit}*\n\n"
        "Upload `.py` to host it (env `PORT` will be set), "
        "or upload `requirements.txt` to install libs.\n\n"
        "*Commands*\n"
        "• `/list` — running scripts\n"
        "• `/ports` — port mapping\n"
        "• `/stop <name>` — stop script\n"
        "• `/restart <name>` — restart script\n"
        "• `/logs <name>` — view logs\n"
        "• `/clearlogs <name>` — clear logs\n"
        "• `/status` — server status\n"
        "• `/mystats` — your bot statistics\n"
        "• `/health` — health check all bots\n\n"
        "*Admin Commands*\n"
        "• `/addadmin <user_id> [limit]` — add admin\n"
        "• `/removeadmin <user_id>` — remove admin\n"
        "• `/listadmins` — list all admins\n"
        "• `/setlimit <user_id> <limit>` — set bot limit\n"
    )
    
    if is_super:
        msg += (
            "\n*Super Admin Pip Commands*\n"
            "• `/pip install <package>` — install package\n"
            "• `/pip uninstall <package>` — uninstall package\n"
            "• `/pip list` — list installed packages\n"
            "• `/pip show <package>` — show package info\n"
            "• `/pip upgrade <package>` — upgrade package\n"
        )
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: `/addadmin <user_id> [bot_limit]`\n"
            f"Default limit: {DEFAULT_BOT_LIMIT}, Max limit: {MAX_BOT_LIMIT}",
            parse_mode="Markdown"
        )
        return

    try:
        new_admin = int(context.args[0])
        bot_limit = int(context.args[1]) if len(context.args) > 1 else DEFAULT_BOT_LIMIT
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id or bot_limit.")
        return

    if admin_manager.add_admin(new_admin, user_id, bot_limit):
        await update.message.reply_text(
            f"✅ Added `{new_admin}` as admin with bot limit `{bot_limit}`.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("⚠️ User is already an admin.")

async def cmd_removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not admin_manager.is_super_admin(user_id):
        await update.message.reply_text("❌ Only super admins can remove admins.")
        return

    if not context.args:
        await update.message.reply_text("Usage: `/removeadmin <user_id>`", parse_mode="Markdown")
        return

    try:
        rem_admin = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id.")
        return

    if admin_manager.remove_admin(rem_admin):
        # Stop all bots owned by removed admin
        to_stop = [name for name, (_, _, _, _, owner) in running.items() if owner == rem_admin]
        for name in to_stop:
            try:
                running[name][0].terminate()
                running.pop(name, None)
                reset_restart_info(name)
            except Exception:
                pass
        
        await update.message.reply_text(
            f"✅ Removed `{rem_admin}` from admins and stopped their {len(to_stop)} bots.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("⚠️ User is not an admin or cannot be removed.")

async def cmd_listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return

    admins = admin_manager.list_admins()
    if not admins:
        await update.message.reply_text("📭 No admins found.")
        return

    lines = ["👥 *Admin List:*"]
    for admin_id, data in admins:
        bot_count = admin_manager.get_user_bot_count(admin_id)
        bot_limit = data.get("bot_limit", 0)
        is_super = data.get("is_super", False)
        added_at = datetime.fromtimestamp(data.get("added_at", 0)).strftime("%Y-%m-%d")
        
        status = "🔱 Super" if is_super else "👤 Admin"
        lines.append(f"• `{admin_id}` — {status} — Bots: {bot_count}/{bot_limit} — Added: {added_at}")
    
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not admin_manager.is_super_admin(user_id):
        await update.message.reply_text("❌ Only super admins can set limits.")
        return

    if len(context.args) != 2:
        await update.message.reply_text(
            f"Usage: `/setlimit <user_id> <limit>`\nMax limit: {MAX_BOT_LIMIT}",
            parse_mode="Markdown"
        )
        return

    try:
        target_user = int(context.args[0])
        new_limit = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id or limit.")
        return

    if admin_manager.set_bot_limit(target_user, new_limit):
        await update.message.reply_text(
            f"✅ Set bot limit for `{target_user}` to `{new_limit}`.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ User is not an admin.")

async def cmd_mystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return

    bot_count = admin_manager.get_user_bot_count(user_id)
    bot_limit = admin_manager.get_bot_limit(user_id)
    
    # Get user's bots
    user_bots = [(name, proc, port) for name, (proc, port, _, _, owner) in running.items() if owner == user_id]
    
    lines = [f"📊 *Your Statistics:*"]
    lines.append(f"🤖 Bots: {bot_count}/{bot_limit}")
    lines.append(f"🔱 Super Admin: {'Yes' if admin_manager.is_super_admin(user_id) else 'No'}")
    
    if user_bots:
        lines.append("\n*Your Running Bots:*")
        for name, proc, port in user_bots:
            alive = proc.poll() is None
            status = "🟢 Running" if alive else "🔴 Stopped"
            lines.append(f"• `{name}` — Port `{port}` — {status}")
    
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    if not running:
        await update.message.reply_text("📭 No scripts running.")
        return

    lines = ["🏥 *Health Check:*"]
    healthy = 0
    unhealthy = 0
    
    for name, (proc, port, _, start_time, owner) in running.items():
        alive = proc.poll() is None
        if alive:
            healthy += 1
            status = "🟢"
            uptime = time.time() - start_time
            uptime_str = f"{uptime/3600:.1f}h" if uptime > 3600 else f"{uptime/60:.1f}m"
            
            # Get process stats if available
            stats = get_process_stats(proc)
            if stats:
                cpu = stats.get("cpu_percent", 0)
                mem = stats.get("memory_mb", 0)
                lines.append(f"{status} `{name}` — Up {uptime_str} — CPU {cpu:.1f}% — RAM {mem:.1f}MB")
            else:
                lines.append(f"{status} `{name}` — Up {uptime_str}")
        else:
            unhealthy += 1
            lines.append(f"🔴 `{name}` — Crashed")
    
    lines.insert(1, f"Summary: {healthy} healthy, {unhealthy} unhealthy")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Permission denied.")
        return

    doc = update.message.document
    if not doc:
        await update.message.reply_text("⚠️ No document found.")
        return
    
    name = doc.file_name
    saved_path = safe_join_upload(name)
    file_size = doc.file_size

    # download the file
    tgfile = await context.bot.get_file(doc.file_id)
    await tgfile.download_to_drive(saved_path)

    # Enhanced requirements handler
    if name.lower() == "requirements.txt" or name.lower().endswith("requirements.txt"):
        # Show requirements content first
        try:
            with open(saved_path, "r", encoding="utf-8", errors="replace") as f:
                requirements_content = f.read().strip()
            
            if requirements_content:
                lp = log_path_for("requirements")
                with open(lp, "a", encoding="utf-8", errors="replace") as lf:
                    lf.write(f"\n--- INSTALL REQUIREMENTS {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
                    lf.write(f"File: {name}\nContent:\n{requirements_content}\n")
                
                live_msg_id = await start_live_logging(
                    update.effective_chat.id, lp, context.bot,
                    is_pip=True
                )
                
                proc = subprocess.Popen(
                    ["pip", "install", "-r", saved_path],
                    stdout=open(lp, "a", buffering=1, encoding="utf-8", errors="replace"),
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True
                )
                
                # Enhanced wait function with live logging
                async def wait_install(p):
                    p.wait()
                    code = p.returncode
                    
                    # Stop live logging and send final message
                    if live_msg_id:
                        if code == 0:
                            final_msg = f"✅ **Requirements Installation Completed!**\n\n📋 Installed packages from:\n\`\`\`\n{requirements_content}\n\`\`\`"
                        else:
                            final_msg = f"❌ **Requirements Installation Failed** (exit code {code})\n\n💡 Check `/logs requirements` for full details"
                        
                        await _stop_live_logging(live_msg_id, final_msg, context.bot)
                
                context.application.create_task(wait_install(proc))
                return
            else:
                await update.message.reply_text("⚠️ Requirements file is empty.")
                return
        except Exception as e:
            await update.message.reply_text(f"❌ Error reading requirements file: {e}")
            return

    # only .py for hosting
    if not name.endswith(".py"):
        await update.message.reply_text("⚠️ Only `.py` files (or requirements.txt) supported.")
        return

    await update.message.reply_text(f"📁 Saved `{name}` ({file_size} bytes)")
    
    # Start the script with live logging
    lp = log_path_for(name)
    
    # Start live logging
    live_msg_id = await start_live_logging(
        update.effective_chat.id, lp, context.bot
    )
    
    # Start the script
    result_msg = await start_script(name, update.effective_user.id)
    
    # If script started successfully, the live logging will continue
    # If it failed, stop live logging and show error
    if "❌" in result_msg and live_msg_id:
        await _stop_live_logging(live_msg_id, f"❌ **Failed to start {name}**\n\n{result_msg}", context.bot)
    elif live_msg_id:
        # Update the live log message to show it's running
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=live_msg_id,
                text=f"✅ **Script Running: {name}**\n\n🔄 **Live Logs:**",
                parse_mode="Markdown"
            )
        except:
            pass

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not running:
        await update.message.reply_text("📭 No scripts running.")
        return
    
    lines = ["📜 *Running Scripts:*"]
    is_super = admin_manager.is_super_admin(user_id)
    
    for name, (proc, port, _, start_time, owner) in running.items():
        alive = proc.poll() is None
        uptime = time.time() - start_time
        uptime_str = f"{uptime/3600:.1f}h" if uptime > 3600 else f"{uptime/60:.1f}m"
        
        if is_super:
            lines.append(f"• `{name}` — PID {proc.pid} — Port `{port}` — Owner `{owner}` — Up {uptime_str} — Alive: {alive}")
        elif owner == user_id:
            lines.append(f"• `{name}` — PID {proc.pid} — Port `{port}` — Up {uptime_str} — Alive: {alive}")
    
    if len(lines) == 1:
        await update.message.reply_text("📭 No scripts running (that you can see).")
    else:
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_ports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not running:
        await update.message.reply_text("📭 No scripts running.")
        return
    
    msg = "🔌 *Port Mapping:*\n"
    is_super = admin_manager.is_super_admin(user_id)
    
    for name, (_, port, _, _, owner) in running.items():
        if is_super or owner == user_id:
            msg += f"• `{name}` → `{port}`\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /stop <script.py>")
        return
    
    name = context.args[0]
    if name not in running:
        await update.message.reply_text("❌ Script not found.", parse_mode="Markdown")
        return
    
    # Check ownership
    owner = running[name][4]
    if owner != user_id and not admin_manager.is_super_admin(user_id):
        await update.message.reply_text("❌ You don't own this bot.")
        return
    
    try:
        running[name][0].terminate()
    except Exception:
        pass
    running.pop(name, None)
    reset_restart_info(name)
    await update.message.reply_text(f"🛑 Stopped `{name}`", parse_mode="Markdown")

async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /restart <script.py>")
        return
    
    name = context.args[0]
    path = safe_join_upload(name)
    if not os.path.exists(path):
        await update.message.reply_text("❌ Script file not found.")
        return
    
    # Check ownership if bot is running
    if name in running:
        owner = running[name][4]
        if owner != user_id and not admin_manager.is_super_admin(user_id):
            await update.message.reply_text("❌ You don't own this bot.")
            return
        
        # stop if running
        try:
            running[name][0].terminate()
        except Exception:
            pass
        running.pop(name, None)
    
    # Check bot limit
    if not admin_manager.can_host_more_bots(user_id):
        current = admin_manager.get_user_bot_count(user_id)
        limit = admin_manager.get_bot_limit(user_id)
        await update.message.reply_text(f"❌ Bot limit reached ({current}/{limit}). Stop some bots first.")
        return
    
    # try to reuse old port if free else pick new
    port = get_free_port()
    if port is None:
        await update.message.reply_text("❌ No free ports available.")
        return
    
    try:
        proc = start_process(path, name, port, user_id)
        running[name] = (proc, port, log_path_for(name), time.time(), user_id)
        reset_restart_info(name)
        await update.message.reply_text(f"🔄 Restarted `{name}` on port `{port}`", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Restart failed: {e}")

async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /logs <script.py>")
        return
    
    name = context.args[0]
    
    # Check ownership
    if name in running:
        owner = running[name][4]
        if owner != user_id and not admin_manager.is_super_admin(user_id):
            await update.message.reply_text("❌ You don't own this bot.")
            return
    
    lp = log_path_for(name)
    if not os.path.exists(lp):
        await update.message.reply_text("❌ Log file not found.")
        return
    
    try:
        with open(lp, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        
        if not lines:
            await update.message.reply_text("📄 Log file is empty.")
            return
        
        last_lines = lines[-100:] if len(lines) > 100 else lines
        text = ''.join(last_lines)
            
        # Clean the text to avoid parsing issues
        clean_text = text.replace('\x00', '').replace('\r', '')
        
        total_lines = len(lines)
        showing_lines = len(last_lines)
        header = f"📋 **Logs for {name}** (showing last {showing_lines}/{total_lines} lines):\n\n"
        
        # Send with chunking to handle long logs
        log_message = f"{header}\`\`\`\n{clean_text}\n\`\`\`"
        await _send_chunked_message(context.bot, update.effective_chat.id, log_message, parse_mode="Markdown")
        
    except Exception as e:
        await update.message.reply_text(f"⚠️ Could not read logs: {str(e)}")

async def cmd_clearlogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /clearlogs <script.py>")
        return
    
    name = context.args[0]
    
    # Check ownership
    if name in running:
        owner = running[name][4]
        if owner != user_id and not admin_manager.is_super_admin(user_id):
            await update.message.reply_text("❌ You don't own this bot.")
            return
    
    lp = log_path_for(name)
    if os.path.exists(lp):
        open(lp, "w").close()
        await update.message.reply_text(f"🧹 Cleared logs for `{name}`", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Log file not found.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    lines = ["📊 **Server Status:**", ""]
    
    if _HAS_PSUTIL:
        try:
            cpu = psutil.cpu_percent(interval=0.5)
            ram = psutil.virtual_memory().percent
            disk = psutil.disk_usage("/").percent
            lines.append(f"🖥️ **System Resources:**")
            lines.append(f"CPU: {cpu:.1f}% | RAM: {ram:.1f}% | Disk: {disk:.1f}%")
            lines.append("")
        except Exception:
            pass
    
    total_bots = len(running)
    healthy_bots = sum(1 for (proc, _, _, _, _) in running.values() if proc.poll() is None)
    
    lines.append(f"🤖 **Bot Status:**")
    lines.append(f"Total Bots: {total_bots} | Healthy: {healthy_bots}")
    lines.append(f"Available Ports: {PORT_END - PORT_START}")
    lines.append("")
    
    if running:
        lines.append("📋 **Running Bots:**")
        for name, (proc, port, _, _, owner) in running.items():
            status = "✅ Running" if proc.poll() is None else "❌ Stopped"
            lines.append(f"• {name} (Port: {port}) - {status}")
        lines.append("")
    
    # Admin stats
    total_admins = len(admin_manager.admins)
    lines.append(f"👥 **Admin Stats:**")
    lines.append(f"Total Admins: {total_admins}")
    
    status_text = "\n".join(lines)
    await _send_chunked_message(context.bot, update.effective_chat.id, status_text, parse_mode="Markdown")

# Admin management commands
async def cmd_pip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not admin_manager.is_super_admin(user_id):
        await update.message.reply_text("❌ Only super admins can use pip commands.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "• `/pip install <package>` — install package\n"
            "• `/pip uninstall <package>` — uninstall package\n"
            "• `/pip list` — list installed packages\n"
            "• `/pip show <package>` — show package info\n"
            "• `/pip upgrade <package>` — upgrade package",
            parse_mode="Markdown"
        )
        return

    action = context.args[0].lower()
    
    if action == "install":
        if len(context.args) < 2:
            await update.message.reply_text("❌ Usage: `/pip install <package>`", parse_mode="Markdown")
            return
        
        package = context.args[1]
        await _run_pip_command(update, context, ["pip", "install", package], f"Installing {package}")
    
    elif action == "uninstall":
        if len(context.args) < 2:
            await update.message.reply_text("❌ Usage: `/pip uninstall <package>`", parse_mode="Markdown")
            return
        
        package = context.args[1]
        await _run_pip_command(update, context, ["pip", "uninstall", "-y", package], f"Uninstalling {package}")
    
    elif action == "upgrade":
        if len(context.args) < 2:
            await update.message.reply_text("❌ Usage: `/pip upgrade <package>`", parse_mode="Markdown")
            return
        
        package = context.args[1]
        await _run_pip_command(update, context, ["pip", "install", "--upgrade", package], f"Upgrading {package}")
    
    elif action == "list":
        await _run_pip_command(update, context, ["pip", "list"], "Listing installed packages", show_output=True)
    
    elif action == "show":
        if len(context.args) < 2:
            await update.message.reply_text("❌ Usage: `/pip show <package>`", parse_mode="Markdown")
            return
        
        package = context.args[1]
        await _run_pip_command(update, context, ["pip", "show", package], f"Showing info for {package}", show_output=True)
    
    else:
        await update.message.reply_text("❌ Unknown pip action. Use: install, uninstall, upgrade, list, show")

async def _run_pip_command(update: Update, context: ContextTypes.DEFAULT_TYPE, cmd: list, description: str, show_output: bool = False):
    """Helper function to run pip commands with live logging"""
    lp = log_path_for("pip_commands")
    
    # Log the command
    with open(lp, "a", encoding="utf-8", errors="replace") as lf:
        lf.write(f"\n--- {description.upper()} {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
        lf.write(f"Command: {' '.join(cmd)}\n")
    
    try:
        live_msg_id = await _start_live_logging(
            update, context, lp,
            f"⏳ **{description}**\n\n🔄 **Live Command Output:**\n\n💡 Send /cancel to stop this command"
        )
        
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            start_new_session=True
        )
        
        if live_msg_id and live_msg_id in _live_logs:
            chat_id, log_file_path, last_position, task, _ = _live_logs[live_msg_id]
            _live_logs[live_msg_id] = (chat_id, log_file_path, last_position, task, proc)
        
        async def stream_output():
            try:
                with open(lp, "a", encoding="utf-8", errors="replace") as log_file:
                    while True:
                        line = proc.stdout.readline()
                        if not line:
                            break
                        log_file.write(line)
                        log_file.flush()  # Ensure immediate write for live logging
                        await asyncio.sleep(0.01)  # Small delay to prevent blocking
            except Exception as e:
                print(f"Error streaming pip output: {e}")
        
        # Wait for completion in background
        async def wait_pip_command():
            # Start output streaming
            stream_task = context.application.create_task(stream_output())
            
            # Wait for process completion
            returncode = await asyncio.get_event_loop().run_in_executor(None, proc.wait)
            
            # Cancel streaming task
            stream_task.cancel()
            
            # Stop live logging and send final message
            if live_msg_id:
                if returncode == 0:
                    if show_output:
                        # For list/show commands, read final output
                        try:
                            with open(lp, "r", encoding="utf-8", errors="replace") as f:
                                content = f.read()
                                # Get the output from this command only
                                last_section = content.split(f"--- {description.upper()}")[-1]
                                output = last_section.split("Command:")[1].split("\n", 1)[1] if "Command:" in last_section else ""
                                
                                if output.strip() and len(output) <= 3000:
                                    final_msg = f"✅ **{description} Completed**\n\n\`\`\`\n{output.strip()}\n\`\`\`"
                                else:
                                    final_msg = f"✅ **{description} completed successfully!**"
                        except:
                            final_msg = f"✅ **{description} completed successfully!**"
                    else:
                        final_msg = f"✅ **{description} completed successfully!**"
                else:
                    final_msg = f"❌ **{description} failed** (exit code {returncode})\n\n💡 Check `/logs pip_commands` for details"
                
                await _stop_live_logging(live_msg_id, final_msg, context.bot)
        
        context.application.create_task(wait_pip_command())
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error running pip command: {e}")
        with open(lp, "a", encoding="utf-8", errors="replace") as lf:
            lf.write(f"Exception: {e}\n")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel running pip commands or script hosting"""
    user_id = update.effective_user.id
    
    # Check if user has permission
    ADMIN_IDS = list(admin_manager.admins.keys())
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ You don't have permission to cancel commands.")
        return
    
    canceled_count = 0
    
    # Cancel all running live logs (pip commands and hosting)
    for message_id, (chat_id, log_file_path, last_position, task, process) in list(_live_logs.items()):
        if chat_id == update.effective_chat.id:
            try:
                # Kill the process if it exists
                if process and process.poll() is None:
                    process.terminate()
                    await asyncio.sleep(1)
                    if process.poll() is None:
                        process.kill()
                
                # Cancel the monitoring task
                task.cancel()
                
                # Send cancellation message
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text="🛑 **Command Canceled**\n\nThe running command has been stopped.",
                        parse_mode="Markdown"
                    )
                except:
                    pass
                
                # Clean up
                del _live_logs[message_id]
                canceled_count += 1
                
            except Exception as e:
                print(f"Error canceling command: {e}")
    
    if canceled_count > 0:
        await update.message.reply_text(f"🛑 Canceled {canceled_count} running command(s).")
    else:
        await update.message.reply_text("ℹ️ No running commands to cancel.")

async def _start_live_logging(update: Update, context: ContextTypes.DEFAULT_TYPE, log_file_path: str, initial_message: str):
    """Start live logging for a process"""
    try:
        # Send initial message
        message = await update.message.reply_text(initial_message, parse_mode="Markdown")
        
        # Start live log monitoring task
        task = context.application.create_task(
            _monitor_live_logs(message.chat_id, message.message_id, log_file_path, context.bot, False)
        )
        
        _live_logs[message.message_id] = (message.chat_id, log_file_path, 0, task, None)
        
        return message.message_id
    except Exception as e:
        print(f"Error starting live logging: {e}")
        return None

async def start_live_logging(chat_id: int, log_file_path: str, bot, is_pip: bool = False):
    """Start live logging for a process - wrapper function"""
    try:
        # Determine initial message based on type
        if is_pip:
            initial_message = "🔄 **Installing packages...**\n\n📦 Live installation logs:"
        else:
            initial_message = "🚀 **Started script...**\n\n📋 Live logs:"
        
        # Send initial message
        message = await bot.send_message(chat_id, initial_message, parse_mode="Markdown")
        
        task = asyncio.create_task(
            _monitor_live_logs(message.chat_id, message.message_id, log_file_path, bot, is_pip)
        )
        
        # Store with proper structure: (chat_id, log_file_path, last_position, task, start_time, is_pip)
        _live_logs[message.message_id] = {
            'chat_id': message.chat_id,
            'log_file_path': log_file_path,
            'last_position': 0,
            'task': task,
            'start_time': time.time(),
            'is_pip': is_pip
        }
        
        return message.message_id
    except Exception as e:
        print(f"Error starting live logging: {e}")
        return None

async def _monitor_live_logs(chat_id: int, message_id: int, log_file_path: str, bot, is_pip: bool = False):
    """Monitor log file and update message with live logs"""
    last_position = 0
    start_time = time.time()
    update_count = 0
    
    try:
        while message_id in _live_logs:
            try:
                elapsed_time = time.time() - start_time
                if not is_pip and elapsed_time > SCRIPT_LIVE_LOG_DURATION:
                    # Stop live logging after 15 seconds for scripts
                    final_msg = f"📋 **Script logs** (Live logging stopped after {SCRIPT_LIVE_LOG_DURATION}s)\n\n💡 Use `/logs <script_name>` for full logs"
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=final_msg,
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        print(f"Error sending final message: {e}")
                    break
                
                # Read new log content
                if os.path.exists(log_file_path):
                    try:
                        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
                            f.seek(last_position)
                            new_content = f.read()
                            last_position = f.tell()
                    except Exception:
                        new_content = ""
                else:
                    new_content = ""
                
                if new_content.strip():
                    clean_content = new_content.replace('\x00', '').replace('\r', '').strip()
                    
                    # Keep only last 2000 characters for live display
                    if len(clean_content) > 2000:
                        clean_content = "...\n" + clean_content[-2000:]
                    
                    if is_pip:
                        live_msg = f"🔄 **Installing packages...**\n\n📦 Live installation logs:\n\`\`\`\n{clean_content}\n\`\`\`"
                    else:
                        remaining_time = max(0, SCRIPT_LIVE_LOG_DURATION - elapsed_time)
                        live_msg = f"🚀 **Starting script...** (Live: {remaining_time:.0f}s)\n\n📋 Live logs:\n\`\`\`\n{clean_content}\n\`\`\`"
                    
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=live_msg,
                            parse_mode="Markdown"
                        )
                        update_count += 1
                    except Exception as e:
                        # If Markdown fails, try plain text
                        try:
                            plain_msg = live_msg.replace('\`\`\`', '').replace('**', '').replace('*', '')
                            await bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=message_id,
                                text=plain_msg
                            )
                        except Exception:
                            print(f"Error updating live logs: {e}")
                
                await asyncio.sleep(2)  # Update every 2 seconds
                
            except Exception as e:
                print(f"Error in live log monitoring: {e}")
                await asyncio.sleep(2)
                
    except Exception as e:
        print(f"Error in live log monitor: {e}")
    finally:
        # Cleanup
        if message_id in _live_logs:
            del _live_logs[message_id]

async def _stop_live_logging(message_id: int, final_message: str = None, bot = None):
    """Stop live logging and send final message"""
    if message_id not in _live_logs:
        print(f"Warning: Live log entry {message_id} not found, already cleaned up")
        return
        
    try:
        chat_id, log_file_path, last_position, task, process = _live_logs[message_id]
        
        # Cancel the monitoring task safely
        if task and not task.cancelled():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # Expected when cancelling
            except Exception as e:
                print(f"Error cancelling live log task: {e}")
        
        if final_message and bot:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=final_message,
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"Error sending final live log message: {e}")
        
        # Clean up safely
        if message_id in _live_logs:
            del _live_logs[message_id]
            
    except Exception as e:
        print(f"Error in _stop_live_logging: {e}")
        # Ensure cleanup even if there's an error
        if message_id in _live_logs:
            del _live_logs[message_id]

# ---------- Monitor (job_queue) ----------
async def monitor_loop(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    try:
        to_restart = []
        current_time = time.time()
        
        # detect exited processes
        for name, (proc, port, _, last_ts, owner) in list(running.items()):
            ret = proc.poll()
            if ret is not None:
                to_restart.append((name, port, ret, owner))
        
        if not to_restart:
            return
            
        restart_messages = []  # Batch messages to send
        
        for name, port, exitcode, owner in to_restart:
            attempts, last = _restart_info.get(name, (0, 0.0))
            
            # cooldown check
            if current_time - last < RESTART_MIN_DELAY:
                continue
            
            # compute and apply backoff
            delay = min(MAX_BACKOFF, (2 ** attempts))
            if current_time - last < delay:
                continue
            
            # Check if owner can still host bots
            if not admin_manager.can_host_more_bots(owner):
                running.pop(name, None)
                restart_messages.append((owner, f"❌ `{name}` crashed but cannot restart - bot limit reached."))
                continue
            
            # record attempt
            _restart_info[name] = (attempts + 1, current_time)
            
            lp = log_path_for(name)
            asyncio.create_task(asyncio.get_event_loop().run_in_executor(
                None, lambda: _write_crash_log(lp, exitcode)
            ))
            
            # verify file exists
            path = safe_join_upload(name)
            if not os.path.exists(path):
                running.pop(name, None)
                continue
            
            chosen_port = port if (not port_in_use(port)) else get_free_port()
            if chosen_port is None:
                restart_messages.append((owner, f"❌ No free port to restart `{name}` (was {port})."))
                continue
            
            # try start
            try:
                new_proc = start_process(path, name, chosen_port, owner)
                running[name] = (new_proc, chosen_port, log_path_for(name), current_time, owner)
                restart_messages.append((owner, f"🔁 Auto-restarted `{name}` on port `{chosen_port}` (exit={exitcode})"))
            except Exception as e:
                asyncio.create_task(asyncio.get_event_loop().run_in_executor(
                    None, lambda: _write_restart_failure_log(lp, e)
                ))
                restart_messages.append((owner, f"❌ Failed to restart `{name}`: {e}"))
        
        for owner_id, message in restart_messages:
            asyncio.create_task(_send_message_safe(app.bot, owner_id, message))
            
    except Exception:
        # ensure watcher never crashes
        pass

async def _send_message_safe(bot, chat_id: int, text: str):
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        pass

def _write_crash_log(log_path: str, exitcode: int):
    """Write crash log entry"""
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as lf:
            lf.write(f"\n--- CRASH {time.strftime('%Y-%m-%d %H:%M:%S')} exit={exitcode} ---\n")
    except Exception:
        pass

def _write_restart_failure_log(log_path: str, error: Exception):
    """Write restart failure log entry"""
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as lf:
            lf.write(f"--- RESTART FAILED {time.strftime('%Y-%m-%d %H:%M:%S')} error={error} ---\n")
    except Exception:
        pass

# ---------- Live Logging ----------
#async def _monitor_live_logs(chat_id: int, message_id: int, log_file_path: str, bot):
#    """Monitor log file and update message with live logs"""
#    last_position = 0
#    log_buffer = deque(maxlen=LIVE_LOG_BUFFER_SIZE)  # Memory-efficient circular buffer
#
#    try:
#        while message_id in _live_logs:
#            try:
#                # Check if log file exists
#                if not os.path.exists(log_file_path):
#                    await asyncio.sleep(LIVE_LOG_UPDATE_INTERVAL)
#                    continue
#
#                # Read new log content
#                with open(log_file_path, "r", encoding="utf-8", errors="replace") as f:
#                    f.seek(last_position)
#                    new_content = f.read()
#                    last_position = f.tell()
#
#                if new_content.strip():
#                    # Add new lines to buffer (memory efficient)
#                    new_lines = new_content.strip().split('\n')
#                    for line in new_lines:
#                        if line.strip():  # Only add non-empty lines
#                            log_buffer.append(line.strip())
#
#                    # Prepare display content (last N lines only)
#                    display_lines = list(log_buffer)[-MAX_LIVE_LOG_LINES:]
#                    display_content = '\n'.join(display_lines)
#
#                    # Truncate if too long for Telegram
#                    if len(display_content) > 3500:
#                        display_content = display_content[-3500:]
#                        display_content = "...\n" + display_content[display_content.find('\n')+1:]
#
#                    # Update message
#                    updated_text = f"🔄 **Live Logs** (Last {len(display_lines)} lines)\n\n\`\`\`\n{display_content}\n\`\`\`"
#
#                    try:
#                        await bot.edit_message_text(
#                            chat_id=chat_id,
#                            message_id=message_id,
#                            text=updated_text,
#                            parse_mode="Markdown"
#                        )
#                    except Exception as edit_error:
#                        # If edit fails, message might be too old or deleted
#                        if "message is not modified" not in str(edit_error).lower():
#                            print(f"Failed to edit live log message: {edit_error}")
#                            break
#
#                await asyncio.sleep(LIVE_LOG_UPDATE_INTERVAL)
#
#            except Exception as e:
#                print(f"Error in live log monitoring: {e}")
#                await asyncio.sleep(LIVE_LOG_UPDATE_INTERVAL)
#
#    except Exception as e:
#        print(f"Live log monitor crashed: {e}")
#    finally:
#        # Clean up
#        if message_id in _live_logs:
#            del _live_logs[message_id]

#async def _stop_live_logging(message_id: int, final_message: str = None, bot = None):
#    """Stop live logging and send final message"""
#    if message_id not in _live_logs:
#        print(f"Warning: Live log entry {message_id} not found, already cleaned up")
#        return
#
#    try:
#        chat_id, log_file_path, last_position, task, process = _live_logs[message_id]
#
#        # Cancel the monitoring task safely
#        if task and not task.cancelled():
#            task.cancel()
#            try:
#                await task
#            except asyncio.CancelledError:
#                pass  # Expected when cancelling
#            except Exception as e:
#                print(f"Error cancelling live log task: {e}")
#
#        if final_message and bot:
#            try:
#                await bot.edit_message_text(
#                    chat_id=chat_id,
#                    message_id=message_id,
#                    text=final_message,
#                    parse_mode="Markdown"
#                )
#            except Exception as e:
#                print(f"Error sending final live log message: {e}")
#
#        # Clean up safely
#        if message_id in _live_logs:
#            del _live_logs[message_id]
#
#    except Exception as e:
#        print(f"Error in _stop_live_logging: {e}")
#        # Ensure cleanup even if there's an error
#        if message_id in _live_logs:
#            del _live_logs[message_id]

# ---------- App bootstrap ----------
async def start_script(name: str, user_id: int) -> str:
    path = safe_join_upload(name)
    if not os.path.exists(path):
        return "❌ Script file not found."
    
    # Check bot limit
    if not admin_manager.can_host_more_bots(user_id):
        current = admin_manager.get_user_bot_count(user_id)
        limit = admin_manager.get_bot_limit(user_id)
        return f"❌ Bot limit reached ({current}/{limit}). Stop some bots first."
    
    # allocate port
    port = get_free_port()
    if port is None:
        return "❌ No free ports available in pool."
    
    # start
    try:
        proc = start_process(path, name, port, user_id)
    except Exception as e:
        return f"❌ Failed to start `{name}`: {e}"

    running[name] = (proc, port, log_path_for(name), time.time(), user_id)
    reset_restart_info(name)
    return f"🚀 Started `{name}` on port `{port}`\nLogs: `{os.path.basename(log_path_for(name))}`"

def main():
    if BOT_TOKEN == "PUT_YOUR_TOKEN_HERE" or not BOT_TOKEN:
        print("ERROR: Set BOT_TOKEN environment variable or update the BOT_TOKEN in the file.")
        return

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Basic commands
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("list", cmd_list))
    application.add_handler(CommandHandler("ports", cmd_ports))
    application.add_handler(CommandHandler("stop", cmd_stop))
    application.add_handler(CommandHandler("restart", cmd_restart))
    application.add_handler(CommandHandler("logs", cmd_logs))
    application.add_handler(CommandHandler("clearlogs", cmd_clearlogs))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("mystats", cmd_mystats))
    application.add_handler(CommandHandler("health", cmd_health))
    
    # Admin management commands
    application.add_handler(CommandHandler("addadmin", cmd_addadmin))
    application.add_handler(CommandHandler("removeadmin", cmd_removeadmin))
    application.add_handler(CommandHandler("listadmins", cmd_listadmins))
    application.add_handler(CommandHandler("setlimit", cmd_setlimit))
    
    application.add_handler(CommandHandler("pip", cmd_pip))
    
    application.add_handler(CommandHandler("cancel", cancel_command))
    
    # File handler
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # schedule monitor every 10 seconds instead of 5 to prevent job timing issues
    application.job_queue.run_repeating(monitor_loop, interval=MONITOR_LOOP_INTERVAL, first=MONITOR_LOOP_INTERVAL)

    print("Enhanced Bot started. Upload scripts to:", UPLOAD_DIR)
    print(f"Super Admin ID: {SUPER_ADMIN_ID}")
    print(f"Default bot limit: {DEFAULT_BOT_LIMIT}")

    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=WEB_SERVICE_PORT, debug=False, use_reloader=False)
    ).start()
    application.run_polling()

if __name__ == "__main__":
    main()
