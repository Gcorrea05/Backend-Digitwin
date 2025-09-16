import asyncio
import json
import os
import signal
from datetime import datetime
from typing import Dict, Optional, Set
from html import escape
from collections import deque
import logging

import httpx
import websockets
from websockets.exceptions import ConnectionClosed
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    AIORateLimiter,
)

logging.basicConfig(level=logging.INFO)
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
BACKEND_BASE = os.getenv("BACKEND_BASE", "http://localhost:8000").rstrip("/")
ALERTS_WS = os.getenv("ALERTS_WS", f"{BACKEND_BASE.replace('http','ws')}/ws/alerts")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "8.0"))
STATUS_INTERVAL = int(os.getenv("STATUS_INTERVAL", "30"))

status_tasks: Dict[int, asyncio.Task] = {}
alerts_tasks: Dict[int, asyncio.Task] = {}
seen_alert_keys: Dict[int, Set[str]] = {}

# buffer para /clear
sent_messages: Dict[int, deque[int]] = {}
def _buf(chat_id: int) -> deque[int]:
    return sent_messages.setdefault(chat_id, deque(maxlen=500))

async def send_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str):
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    _buf(chat_id).append(msg.message_id)

async def send_plain(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str):
    msg = await context.bot.send_message(chat_id=chat_id, text=text)
    _buf(chat_id).append(msg.message_id)

def fmt_ts(ts: Optional[str]) -> str:
    if not ts:
        return "-"
    try:
        dt = datetime.fromisoformat(ts.replace("Z","").replace("+00:00",""))
        return dt.strftime("%H:%M:%S")
    except Exception:
        return ts

# --------- chamadas HTTP ----------
async def fetch_json(client, url):
    try:
        r = await client.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

async def fetch_health(client): 
    return await fetch_json(client, f"{BACKEND_BASE}/health")

async def fetch_actuators_state(client): 
    return await fetch_json(client, f"{BACKEND_BASE}/api/live/actuators/state")

async def fetch_actuator_cpm(client, aid: int):
    data = await fetch_json(client, f"{BACKEND_BASE}/api/live/cycles/rate?actuator_id={aid}")
    for k in ("cpm","rate","value"):
        v=data.get(k)
        if isinstance(v,(int,float)): return float(v)
    rates=data.get("rates")
    if isinstance(rates,dict):
        v=rates.get(str(aid)) or rates.get(aid)
        if isinstance(v,(int,float)): return float(v)
    return None

async def fetch_actuator_vibration(client, aid: int):
    data=await fetch_json(client, f"{BACKEND_BASE}/api/live/vibration?actuator_id={aid}")
    avg=data.get("avg")
    if isinstance(avg,(int,float)): return float(avg)
    series=data.get("series") or data.get("values") or []
    if isinstance(series,list):
        xs=[v for v in series if isinstance(v,(int,float))]
        if xs: return float(sum(xs)/len(xs))
    by=data.get("by_actuator")
    if isinstance(by,dict):
        v=by.get(str(aid)) or by.get(aid)
        if isinstance(v,(int,float)): return float(v)
    return None

# ---------- status helpers ----------
def health_to_online(redis_ok, db_ok): 
    return "ONLINE" if redis_ok and db_ok else "WARNING"

def actuator_state_to_ok_warning(state):
    s=(state or "").lower()
    if s in ("erro","error","fault","alarm","unknown","desconhecido",""): 
        return "WARNING"
    return "OK"

def vib_to_ok_warning(vib, limit=5.0):
    if vib is None: return "ND"
    return "OK" if vib<limit else "WARNING"

async def compose_status_message(context):
    async with httpx.AsyncClient() as client:
        health=await fetch_health(client)
        acts=await fetch_actuators_state(client)
        redis_ok=bool(health.get("redis_ok") or health.get("ok_redis") or health.get("redis"))
        db_ok=health.get("db_time") is not None or health.get("db_ok") or health.get("mysql_ok")
        system_status=health_to_online(redis_ok,db_ok)
        actuators=acts.get("actuators") if isinstance(acts,dict) else None
        if not isinstance(actuators,list):
            actuators=[{"actuator_id":1},{"actuator_id":2}]
        lines=[f"🩺 <b>Sistema</b>: {escape(system_status)}","","🔧 <b>Atuadores</b>:"]
        for a in actuators:
            aid=a.get("actuator_id") or a.get("id")
            try: aid=int(str(aid))
            except: continue
            state=a.get("state")
            status_txt=actuator_state_to_ok_warning(state)
            emoji="✅" if status_txt=="OK" else "⚠️"
            async with httpx.AsyncClient() as client2:
                cpm=await fetch_actuator_cpm(client2,aid)
                vib=await fetch_actuator_vibration(client2,aid)
            cpm_str=f"{cpm:.2f}" if isinstance(cpm,(int,float)) else "—"
            vib_str=f"{vib:.2f} g" if isinstance(vib,(int,float)) else "—"
            vib_status=vib_to_ok_warning(vib)
            lines.append(
                f"• A{aid} → {emoji}{escape(status_txt)} | ⚙️ CPM: {escape(cpm_str)} | 📈 Vib: {escape(vib_status)} ({escape(vib_str)})"
            )
        return "\n".join(lines)
# ---------- loops ----------
async def status_loop(chat_id, context):
    while True:
        try:
            text=await compose_status_message(context)
            await send_html(context,chat_id,text)
        except Exception as e:
            await send_plain(context,chat_id,f"⚠️ Erro no status: {escape(str(e))}")
        await asyncio.sleep(STATUS_INTERVAL)

async def alerts_loop(chat_id, context):
    seen=seen_alert_keys.setdefault(chat_id,set())
    uri=ALERTS_WS
    if uri.startswith("http://"): uri="ws://"+uri[len("http://"):]
    if uri.startswith("https://"): uri="wss://"+uri[len("https://"):]
    while True:
        try:
            async with websockets.connect(uri,ping_interval=20,ping_timeout=20) as ws:
                async for msg in ws:
                    try: data=json.loads(msg)
                    except: continue
                    for it in data.get("items") or []:
                        key=f"{it.get('ts')}|{it.get('actuator_id')}|{it.get('type')}|{it.get('message')}"
                        if key in seen: continue
                        seen.add(key)
                        sev=(it.get("severity") or "").lower()
                        badge={"critical":"🛑","high":"🚨","medium":"⚠️","low":"ℹ️"}.get(sev,"🚨")
                        ts=fmt_ts(it.get("ts"))
                        text=(f"{badge} <b>ALERTA</b> [{escape(sev or 'n/d')}]\n"
                              f"• <b>Atuador:</b> {escape(str(it.get('actuator_id')))}\n"
                              f"• <b>Tipo:</b> {escape(str(it.get('type') or ''))}\n"
                              f"• <b>Msg:</b> {escape(str(it.get('message') or ''))}\n"
                              f"• <b>Quando:</b> {escape(ts)}")
                        await send_html(context,chat_id,text)
        except (ConnectionClosed,OSError) as e:
            await send_plain(context,chat_id,f"🔁 Reconectando alertas: {escape(str(e))}")
            await asyncio.sleep(5)
        except Exception as e:
            await send_plain(context,chat_id,f"⚠️ Erro nos alertas: {escape(str(e))}")
            await asyncio.sleep(10)

# ---------- comandos ----------
async def start_cmd(update, context):
    await update.message.reply_text(
        "👋 <b>Bem-vindo ao DigitwinBOT da IoTech!</b>\n"
        "• <code>/status</code> – status a cada 30s\n"
        "• <code>/alerts</code> – alertas em tempo real\n"
        "• <code>/subscribe</code> – ligar status+alertas\n"
        "• <code>/unsubscribe</code> – desligar tudo\n"
        "• <code>/clear</code> – limpar mensagens enviadas pelo bot",
        parse_mode=ParseMode.HTML)

async def status_cmd(update,context):
    chat_id=update.effective_chat.id
    if chat_id in status_tasks: status_tasks[chat_id].cancel()
    status_tasks[chat_id]=asyncio.create_task(status_loop(chat_id,context))
    await update.message.reply_text(f"✅ Status a cada {STATUS_INTERVAL}s. Use /stop para parar.")

async def stop_cmd(update,context):
    chat_id=update.effective_chat.id
    t=status_tasks.pop(chat_id,None)
    if t: t.cancel(); await update.message.reply_text("🛑 Status parado.")
    else: await update.message.reply_text("ℹ️ Nenhum status em execução.")

async def alerts_cmd(update,context):
    chat_id=update.effective_chat.id
    if chat_id in alerts_tasks: alerts_tasks[chat_id].cancel()
    alerts_tasks[chat_id]=asyncio.create_task(alerts_loop(chat_id,context))
    await update.message.reply_text("✅ Alertas habilitados.")

async def stopalerts_cmd(update,context):
    chat_id=update.effective_chat.id
    t=alerts_tasks.pop(chat_id,None)
    if t: t.cancel(); await update.message.reply_text("🛑 Alertas parados.")
    else: await update.message.reply_text("ℹ️ Nenhum canal de alertas.")

# subscribe/unsubscribe
async def subscribe_cmd(update,context):
    await status_cmd(update,context)
    await alerts_cmd(update,context)

async def unsubscribe_cmd(update,context):
    await stop_cmd(update,context)
    await stopalerts_cmd(update,context)

# limpar mensagens do bot
async def clear_cmd(update,context):
    chat_id=update.effective_chat.id
    ids=list(_buf(chat_id))
    for mid in ids:
        try: await context.bot.delete_message(chat_id,mid)
        except: pass
    _buf(chat_id).clear()
    await update.message.reply_text("🧹 Chat limpo!")

def _install_signal_handlers(app):
    loop=asyncio.get_event_loop()
    def _graceful(*_):
        for t in list(status_tasks.values())+list(alerts_tasks.values()):
            t.cancel()
        loop.create_task(app.stop())
    for sig in (signal.SIGINT,signal.SIGTERM):
        try: loop.add_signal_handler(sig,_graceful)
        except NotImplementedError: pass

def main():
    if not TELEGRAM_TOKEN: raise RuntimeError("Missing TELEGRAM_TOKEN")
    app=Application.builder().token(TELEGRAM_TOKEN).rate_limiter(AIORateLimiter()).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("alerts", alerts_cmd))
    app.add_handler(CommandHandler("stopalerts", stopalerts_cmd))
    app.add_handler(CommandHandler("subscribe", subscribe_cmd))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_cmd))
    app.add_handler(CommandHandler("clear", clear_cmd))
    _install_signal_handlers(app)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    main()
