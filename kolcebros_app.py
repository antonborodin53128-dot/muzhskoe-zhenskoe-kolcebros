"""Кольцеброс — конкурс «Мужское / Женское».

Участнику даётся 10 колец, по 2 на каждый из 5 цветов. Нужно набросить кольцо
нужного цвета на нужную палку. Ведущий сам считает попадания и нажимает на
пульте, сколько баллов добавить: 10, 15, 20, 25 или 30.

  /         — пульт ведущего (телефон)
  /screen   — экран для гостей (проектор)

Обновления идут мгновенно через Socket.IO. Все ссылки относительные, поэтому
этот же файл работает и отдельно, и внутри общего сборника (/women/kolcebros/...).
"""
import json
import os
import secrets
import tempfile
import time
from threading import RLock

from flask import Flask, redirect, render_template_string, request
from flask_socketio import SocketIO, emit

POINTS = (10, 15, 20, 25, 30)   # сколько баллов ведущий может добавить за раз
MAX_PARTICIPANTS = 30
MAX_ADDS = 200                  # страховка от бесконечного роста истории начислений
# Состояние игры переживает перезапуск процесса: ведущий не теряет конкурс из-за сбоя или перезагрузки сервера.
STATE_FILE = os.environ.get("KOLCEBROS_STATE_FILE", os.path.join(tempfile.gettempdir(), "kolcebros_state.json"))
STATE_TTL = 6 * 3600
BOOT = secrets.token_hex(4)   # меняется при каждом запуске сервера: клиенты понимают, что номера состояний начались заново

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", ping_interval=15, ping_timeout=25)

# ВАЖНО. socketio.emit() может прямо внутри себя закрыть «протухшее» соединение (например, свёрнутое окно)
# и тут же вызвать on_disconnect в том же потоке. Если emit() вызван под этой блокировкой, а on_disconnect
# тоже берёт её, поток зависает сам на себе, и весь сервер перестаёт отвечать.
# Поэтому: под блокировкой только меняем состояние и собираем снимок, а рассылаем всегда ПОСЛЕ неё.
# RLock — страховка на случай повторного входа.
lock = RLock()

state = {
    "participants": [],   # [{"name": str, "adds": [int, ...], "done": bool}]; adds — начисления по порядку (для отмены)
    "current": -1,
    "finished": False,
    "bump": 0,            # счётчик изменений: по нему экран понимает, что анимировать
    "last": None,         # последнее действие ведущего {"kind": "add"|"undo", "p": int, "n": bump}
    "rev": 0,             # номер снимка: клиент игнорирует снимок, который пришёл позже более нового
}


def score_of(player):
    return sum(player["adds"])


def snapshot_locked():
    state["rev"] += 1
    return {
        "participants": [
            {"name": p["name"], "score": score_of(p), "done": p["done"]}
            for p in state["participants"]
        ],
        "current": state["current"],
        "finished": state["finished"],
        "last": state["last"],
        "points": list(POINTS),
        "server_now": time.time(),
        "rev": state["rev"],
        "boot": BOOT,
    }


def publish(snap):
    """Разослать снимок всем. Вызывать только когда блокировка уже отпущена."""
    socketio.emit("state", snap)


def current_player_locked():
    i = state["current"]
    if 0 <= i < len(state["participants"]):
        return state["participants"][i]
    return None


def save_locked():
    data = {k: state[k] for k in ("participants", "current", "finished", "bump")}
    data["saved_at"] = time.time()
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except OSError:
        pass   # диск недоступен — игра всё равно идёт, просто без сохранения


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if time.time() - float(data["saved_at"]) > STATE_TTL:
            return
        parts = []
        for p in data["participants"][:MAX_PARTICIPANTS]:
            adds = [int(x) for x in p["adds"]]
            if len(adds) > MAX_ADDS or any(x not in POINTS for x in adds):
                return
            parts.append({"name": str(p["name"])[:32], "adds": adds, "done": bool(p["done"])})
        if not parts:
            return
        cur, finished = int(data["current"]), bool(data["finished"])
        if not finished and not 0 <= cur < len(parts):
            return
        state.update(participants=parts, current=cur, finished=finished, last=None,
                     bump=int(data.get("bump", 0)) + 1)
    except (OSError, ValueError, KeyError, TypeError):
        pass


load_state()


# ---------- события ----------

@socketio.on("connect")
def on_connect(data=None):
    with lock:
        snap = snapshot_locked()
    emit("state", snap)


@socketio.on("sync")
def on_sync(data=None):
    with lock:
        snap = snapshot_locked()
    emit("state", snap)


@socketio.on("setup")
def on_setup(data=None):
    try:
        count = int((data or {}).get("count", 4))
    except (TypeError, ValueError):
        count = 4
    count = max(1, min(MAX_PARTICIPANTS, count))
    with lock:
        state.update(
            participants=[{"name": f"Участник {i + 1}", "adds": [], "done": False} for i in range(count)],
            current=0, finished=False, last=None,
        )
        state["bump"] += 1
        save_locked()
        snap = snapshot_locked()
    publish(snap)


@socketio.on("add")
def on_add(data=None):
    """Добавить текущему участнику баллы: 10, 15, 20, 25 или 30."""
    try:
        points = int((data or {}).get("points", -1))
    except (TypeError, ValueError):
        return
    if points not in POINTS:
        return
    with lock:
        player = current_player_locked()
        if not player or state["finished"] or len(player["adds"]) >= MAX_ADDS:
            return
        player["adds"].append(points)
        state["bump"] += 1
        state["last"] = {"kind": "add", "p": points, "n": state["bump"]}
        save_locked()
        snap = snapshot_locked()
    publish(snap)


@socketio.on("undo")
def on_undo(data=None):
    """Отменить последнее начисление текущего участника."""
    with lock:
        player = current_player_locked()
        if not player or state["finished"] or not player["adds"]:
            return
        value = player["adds"].pop()
        state["bump"] += 1
        state["last"] = {"kind": "undo", "p": value, "n": state["bump"]}
        save_locked()
        snap = snapshot_locked()
    publish(snap)


@socketio.on("next")
def on_next(data=None):
    with lock:
        player = current_player_locked()
        if not player or state["finished"]:
            return
        player["done"] = True
        if state["current"] < len(state["participants"]) - 1:
            state["current"] += 1
        else:
            state["finished"] = True
        state["bump"] += 1
        state["last"] = None
        save_locked()
        snap = snapshot_locked()
    publish(snap)


@socketio.on("rename")
def on_rename(data=None):
    data = data or {}
    try:
        i = int(data.get("index", -1))
    except (TypeError, ValueError):
        return
    name = " ".join(str(data.get("name", "")).split())[:32]
    with lock:
        if not 0 <= i < len(state["participants"]):
            return
        state["participants"][i]["name"] = name or f"Участник {i + 1}"
        save_locked()
        snap = snapshot_locked()
    publish(snap)


@socketio.on("reset")
def on_reset(data=None):
    with lock:
        state.update(participants=[], current=-1, finished=False, last=None)
        state["bump"] += 1
        save_locked()
        snap = snapshot_locked()
    publish(snap)


# ---------- страницы ----------

def base_path():
    # "/" отдельно или "/women/kolcebros/" внутри сборника
    return (request.script_root or "") + "/"


@app.after_request
def no_cache(resp):
    if request.path in ("/", "/screen"):
        resp.headers["Cache-Control"] = "no-store"
    return resp


def page(template):
    return render_template_string(
        template, base=base_path(), points=POINTS,
        theme_css=THEME_CSS, client_js=CLIENT_JS, bg_js=BG_JS)


@app.get("/")
def control():
    return page(CONTROL_HTML)


@app.get("/healthz")
def healthz():
    # Пинг от открытых страниц: не даёт бесплатному хостингу «заснуть» и позволяет странице понять, жив ли сервер
    return "ok", 200, {"Cache-Control": "no-store", "Content-Type": "text/plain"}


@app.get("/control")
def control_old():
    return redirect(base_path(), code=302)


@app.get("/screen")
def screen():
    return page(SCREEN_HTML)


# ======================================================================
# Общий стиль конкурсов «Мужское / Женское».
# Тема задаётся атрибутом data-theme="men" | "women" на <html>.
# ======================================================================
THEME_CSS = r"""
:root,[data-theme=men]{
  --ink:#06140e; --ink-2:#0a1f16; --surface:#0d261b; --line:#1d4734;
  --signal:#2bf08a; --signal-ink:#02140a; --signal-soft:rgba(43,240,138,.14);
  --chalk:#f1f5ee; --mist:#8da698; --danger:#ff8e9c; --danger-bg:#2d1419;
  --warm:#ffd23f;
}
[data-theme=women]{
  --ink:#140710; --ink-2:#1d0b17; --surface:#26101f; --line:#4e1d3b;
  --signal:#ff4fa8; --signal-ink:#22000f; --signal-soft:rgba(255,79,168,.14);
  --chalk:#f8eff4; --mist:#b394a6; --danger:#ffb08e; --danger-bg:#2d1714;
}
:root{
  --display:"Unbounded",system-ui,sans-serif;
  --ui:"Onest",system-ui,sans-serif;
  --r-s:12px; --r-m:18px; --r-l:28px;
}
*{box-sizing:border-box}
html,body{margin:0;background:var(--ink);color:var(--chalk);font-family:var(--ui);-webkit-font-smoothing:antialiased}
body{min-height:100vh;min-height:100dvh}
button{font:inherit;color:inherit;cursor:pointer;-webkit-tap-highlight-color:transparent}
button:focus-visible,a:focus-visible,input:focus-visible{outline:3px solid var(--signal);outline-offset:3px}
.num{font-family:var(--digits);font-weight:var(--digits-w);font-variant-numeric:tabular-nums;font-feature-settings:"tnum"}
/* Шрифт цифр — Nunito: скруглённые края */
:root{--digits:"Nunito",var(--display);--digits-w:900}
/* Табло: каждая цифра прокручивается в своём окошке */
.roll{display:inline-flex;align-items:flex-start;line-height:1;--cell:1.08em;height:var(--cell);vertical-align:top;
  -webkit-mask-image:linear-gradient(transparent,#000 14%,#000 86%,transparent);mask-image:linear-gradient(transparent,#000 14%,#000 86%,transparent)}
.roll .d{display:inline-block;height:var(--cell);overflow:hidden}
.roll .s{display:flex;flex-direction:column;transition:transform var(--roll-ms,560ms) cubic-bezier(.22,1.18,.36,1)}
.roll .s>span{height:var(--cell);line-height:var(--cell);text-align:center}
.roll .sep{height:var(--cell);line-height:var(--cell);padding:0 .02em}
.roll .d.in{animation:digitIn .45s cubic-bezier(.2,.9,.3,1.2)}
@keyframes digitIn{from{transform:translateY(-.4em);opacity:0}}
/* Фон экрана для гостей */
#bg{position:fixed;inset:0;width:100%;height:100%;z-index:0;pointer-events:none}
.screen{position:relative;z-index:1}
/* Вместо точки в логотипе — кольцо */
.wordmark{display:inline-flex;align-items:center;gap:.5em;font-family:var(--display);font-weight:800;letter-spacing:.02em}
.wordmark i{width:.7em;height:.7em;border-radius:50%;border:.15em solid var(--signal);box-shadow:0 0 14px var(--signal-soft),inset 0 0 8px var(--signal-soft)}
.offline{position:fixed;left:0;right:0;top:0;z-index:100;padding:10px 16px;text-align:center;font-weight:600;background:var(--danger-bg);color:var(--danger);transform:translateY(-100%);transition:transform .25s}
.offline.on{transform:none}
.offbtn{margin-left:12px;border:1px solid currentColor;background:none;color:inherit;border-radius:999px;padding:4px 14px;font:inherit;font-weight:700;cursor:pointer}
[hidden]{display:none!important}
@media (prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.01ms!important;transition-duration:.01ms!important}}
"""

# Общая логика клиента: подключение, переподключение, табло, мелкие помощники.
CLIENT_JS = r"""
const BASE = document.documentElement.dataset.base || '/';
// tryAllTransports: если WebSocket не поднялся (прокси, сеть), соединение пойдёт обычными запросами, а не оборвётся.
const socket = io({path: BASE + 'socket.io', transports: ['websocket', 'polling'], tryAllTransports: true,
                   reconnectionDelay: 400, reconnectionDelayMax: 2500, timeout: 8000});
let S = null, bootId = null;
const offlineBar = document.getElementById('offline');
if (offlineBar) {
  const b = document.createElement('button'); b.className = 'offbtn'; b.textContent = 'Обновить страницу';
  b.onclick = () => location.reload(); offlineBar.appendChild(b);
}
let downSince = 0;
const setOffline = on => { if (offlineBar) offlineBar.classList.toggle('on', on); };
socket.on('connect', () => { downSince = 0; setOffline(false); });
socket.on('disconnect', () => { if (!downSince) downSince = performance.now(); setOffline(true); });
socket.on('connect_error', () => { if (!downSince) downSince = performance.now(); setOffline(true); });
socket.on('state', s => {
  if (s.boot !== bootId) { bootId = s.boot; S = null; }       // сервер перезапускался: номера снимков пошли заново
  else if (S && s.rev < S.rev) return;                         // более старый снимок обогнал новый — не откатываемся
  S = s; window.onState && window.onState(s);
});
const wake = () => { if (document.hidden) return; if (socket.connected) socket.emit('sync'); else { try { socket.connect(); } catch (e) {} } };
document.addEventListener('visibilitychange', wake);
addEventListener('online', wake);
addEventListener('focus', wake);
addEventListener('pageshow', wake);
// Пинг сервера раз в 4 минуты: хостинг не засыпает, пока открыта хотя бы одна страница.
setInterval(() => { fetch(BASE + 'healthz', {cache: 'no-store'}).catch(() => {}); }, 240000);
// Связи нет дольше 20 секунд, а сервер по обычному запросу отвечает: сокет «залип». Свежая страница вернёт связь,
// состояние конкурса хранится на сервере и никуда не денется.
setInterval(async () => {
  if (socket.connected || document.hidden) return;
  if (!downSince) downSince = performance.now();
  if (performance.now() - downSince < 20000) return;
  downSince = performance.now();
  try { const r = await fetch(BASE + 'healthz', {cache: 'no-store'}); if (r.ok && !socket.connected) location.reload(); } catch (e) {}
}, 5000);

function phaseOf(s){
  if (!s || !s.participants.length) return 'idle';
  return s.finished ? 'finished' : 'play';
}
function esc(v){ return String(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function ranking(s){ return s.participants.map((p, i) => ({...p, i})).filter(p => p.done).sort((a, b) => b.score - a.score || a.i - b.i); }
function ptsWord(n){ const a = n % 10, b = n % 100; if (a === 1 && b !== 11) return 'балл'; if (a >= 2 && a <= 4 && (b < 12 || b > 14)) return 'балла'; return 'баллов'; }
function setText(el, v){ v = String(v); if (el._v !== v) { el._v = v; el.textContent = v; } }   // трогаем DOM только при изменении
function setHtml(el, h){ if (el._h !== h) { el._h = h; el.innerHTML = h; } }
/* ---------- Табло ----------
   setRoll(el, '120') — каждая цифра крутится к новому значению.
   Направление берётся из значения: больше — крутим вверх, меньше — вниз. */
function setRoll(el, text, opts){
  text = String(text);
  if (el._text === text) return;
  const old = el._text, chars = [...text];
  const pattern = chars.map(c => /\d/.test(c) ? 'd' : c).join('');
  const up = (opts && 'up' in opts) ? opts.up : (old == null || parseFloat(text.replace(':', '.')) >= parseFloat(String(old).replace(':', '.')));
  if (el._pattern !== pattern) {
    // число разрядов поменялось — пересобираем, старые цифры выравниваем по правому краю
    const oldDigits = old ? [...old].filter(c => /\d/.test(c)).map(Number) : [];
    const nd = chars.filter(c => /\d/.test(c)).length;
    el.classList.add('roll'); el.innerHTML = ''; el._cells = [];
    let di = 0;
    chars.forEach(c => {
      if (!/\d/.test(c)) { const sp = document.createElement('span'); sp.className = 'sep'; sp.textContent = c; el.appendChild(sp); return; }
      const d = document.createElement('span'); d.className = 'd';
      const st = document.createElement('span'); st.className = 's';
      st.innerHTML = '0123456789'.split('').concat('0').map(n => `<span>${n}</span>`).join('');
      d.appendChild(st); el.appendChild(d);
      const fromOld = oldDigits[oldDigits.length - (nd - di)];
      const start = fromOld != null ? fromOld : (opts && opts.from != null ? opts.from : 0);
      if (old != null && fromOld == null && !(opts && opts.from != null)) d.classList.add('in');
      st.style.transition = 'none'; st.style.transform = `translateY(calc(${-start} * var(--cell)))`;
      el._cells.push({st, cur: start}); di++;
    });
    el._pattern = pattern;
    void el.offsetWidth;
  }
  el._text = text;
  let di = 0;
  chars.forEach(c => { if (/\d/.test(c)) rollCell(el._cells[di++], +c, up, opts && opts.delay); });
}
function rollCell(cell, to, up, delay){
  const st = cell.st, from = cell.cur;
  if (to === from) return;
  st.style.transitionDelay = delay ? delay + 'ms' : '';
  const go = idx => { st.style.transition = ''; st.style.transform = `translateY(calc(${-idx} * var(--cell)))`; };
  const snap = idx => { st.style.transition = 'none'; st.style.transform = `translateY(calc(${-idx} * var(--cell)))`; void st.offsetWidth; };
  if (up && to < from && to === 0) {            // 9 → 0 вверх: докручиваем до нижнего «0» и тихо возвращаемся
    go(10);
    clearTimeout(cell.t); cell.t = setTimeout(() => { if (cell.cur === 0) snap(0); }, 700 + (delay || 0));
  } else if (!up && to > from && from === 0) {   // 0 → 9 вниз: встаём на нижний «0» и крутим вниз
    snap(10); go(to);
  } else go(to);
  cell.cur = to;
}
"""

# Фон экрана для гостей: небольшое число колец медленно всплывает и кувыркается.
# На каждый бросок кольца вспыхивают ярче.
BG_JS = r"""
const Bg = (() => {
  const html = document.documentElement;
  const cv = document.getElementById('bg');
  if (!cv) return {pulse(){}, recolor(){}};
  const ctx = cv.getContext('2d');
  const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;
  let W = 0, H = 0, dpr = 1, flash = 0, flashTarget = 0, t0 = performance.now();
  let rgb = [255, 79, 168];
  function readColor(){
    const c = getComputedStyle(html).getPropertyValue('--signal').trim();
    const m = c.match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
    if (m) rgb = m.slice(1).map(h => parseInt(h, 16));
  }
  const rgba = a => `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${a})`;
  function size(){ dpr = Math.min(2, devicePixelRatio || 1); W = innerWidth; H = innerHeight; cv.width = W * dpr; cv.height = H * dpr; ctx.setTransform(dpr, 0, 0, dpr, 0, 0); }
  addEventListener('resize', size); size(); readColor();

  // «Кольца»: их немного (9), каждое — тонкий овал, наклонённый как брошенное кольцо
  const rings = Array.from({length: 9}, () => ({
    x: Math.random(), y: Math.random(), r: 22 + Math.random() * 38, v: .012 + Math.random() * .022,
    a: .16 + Math.random() * .2, w: Math.random() * 6, tilt: Math.random() * Math.PI, spin: (Math.random() - .5) * .25,
    sq: .45 + Math.random() * .35,
  }));
  function draw(t){
    ctx.globalCompositeOperation = 'lighter';
    rings.forEach(d => {
      const y = ((d.y - t * d.v) % 1 + 1) % 1, x = d.x + Math.sin(t * .3 + d.w) * .015;
      const px = x * W, py = y * H * 1.1 - H * .05, rr = d.r * (1 + flash * .25);
      const sq = d.sq + .18 * Math.sin(t * .5 + d.w);
      ctx.save();
      ctx.translate(px, py); ctx.rotate(d.tilt + t * d.spin);
      ctx.lineWidth = rr * .2;
      ctx.strokeStyle = rgba(d.a + flash * .3);
      ctx.shadowColor = rgba(.8); ctx.shadowBlur = rr * .55;
      ctx.beginPath(); ctx.ellipse(0, 0, rr, rr * sq, 0, 0, Math.PI * 2); ctx.stroke();
      ctx.restore();
    });
    ctx.globalCompositeOperation = 'source-over';
  }
  function loop(now){
    const t = reduce ? 0 : (now - t0) / 1000;
    ctx.clearRect(0, 0, W, H);
    draw(t);
    flash += (flashTarget - flash) * .07; flashTarget *= .975;    // ~0,5 с разгорается, ~2 с гаснет
    requestAnimationFrame(loop);
  }
  requestAnimationFrame(loop);
  return {pulse(){ flashTarget = 1; }, recolor: readColor};
})();
"""

FONTS = """<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Onest:wght@400;600;800&family=Unbounded:wght@600;800;900&family=Nunito:wght@800;900&display=swap" rel="stylesheet">
<script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>"""

# ======================================================================
# Пульт ведущего
# ======================================================================
CONTROL_HTML = r"""<!doctype html>
<html lang="ru" data-theme="women" data-base="{{ base }}">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#140710"><title>Кольцеброс · пульт</title>
""" + FONTS + r"""
<style>{{ theme_css|safe }}
body{background:radial-gradient(120% 60% at 0 0,var(--ink-2),var(--ink) 60%)}
.app{max-width:520px;margin:0 auto;padding:16px 16px calc(24px + env(safe-area-inset-bottom));display:flex;flex-direction:column;gap:14px;min-height:100dvh}
.top{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.top .wordmark{font-size:20px}
.link{color:var(--mist);font-weight:600;font-size:14px;text-decoration:underline;text-underline-offset:3px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--r-l);padding:20px}
h2{margin:0 0 14px;font-size:17px;font-weight:600;color:var(--mist)}
.stepper{display:grid;grid-template-columns:72px 1fr 72px;align-items:center;gap:10px;margin-bottom:16px}
.stepper button{height:72px;border-radius:var(--r-m);border:1px solid var(--line);background:var(--ink-2);font-size:30px;font-weight:600}
.stepper .num{text-align:center;font-size:52px;}
.btn{display:block;width:100%;border:0;border-radius:var(--r-m);padding:18px;font-size:19px;font-weight:800}
.btn.primary{background:var(--signal);color:var(--signal-ink)}
.btn.quiet{background:transparent;border:1px solid var(--line);color:var(--chalk);font-weight:600}
.btn.danger{background:transparent;border:1px solid var(--danger-bg);color:var(--danger);font-weight:600;font-size:15px;padding:14px}
.btn:disabled{opacity:.4;cursor:default}
.btn:active:not(:disabled){transform:scale(.98)}
.who{display:flex;justify-content:space-between;align-items:baseline;gap:10px}
.who .name{font-family:var(--display);font-size:26px;font-weight:800;overflow-wrap:anywhere}
.who .of{color:var(--mist);font-weight:600;white-space:nowrap}
.score{text-align:center;padding:14px 0 10px}
.score .num{font-size:96px;line-height:1;color:var(--signal)}
.score .unit{color:var(--mist);font-weight:600;margin-top:4px}
.pts{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-bottom:10px}
.pt{height:84px;padding:0;border:0;border-radius:var(--r-m);background:var(--signal);color:var(--signal-ink);font-family:var(--digits);font-weight:var(--digits-w);font-size:clamp(18px,5.4vw,27px);touch-action:manipulation}
.pt:disabled{opacity:.3;cursor:default}
.pt:active:not(:disabled){transform:scale(.95);filter:brightness(1.1)}
.stack{display:flex;flex-direction:column;gap:10px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.two .btn{padding:15px 8px;font-size:16px}
.rows{display:flex;flex-direction:column}
.row{display:flex;justify-content:space-between;padding:12px 2px;border-bottom:1px solid var(--line)}
.row:last-child{border-bottom:0}
.row b{font-family:var(--digits);font-weight:var(--digits-w);color:var(--signal)}
.row.first b{font-size:20px}
.hint{color:var(--mist);font-size:14px;text-align:center}
.spacer{flex:1}
.namelink{align-self:center;background:none;border:0;color:var(--mist);font-size:14px;font-weight:600;text-decoration:underline;text-underline-offset:3px;padding:8px}
.fields{display:flex;flex-direction:column;gap:8px}
.fields label{display:grid;grid-template-columns:2em 1fr;align-items:center;gap:8px;color:var(--mist);font-weight:600}
.fields input{width:100%;min-width:0;background:var(--ink-2);border:1px solid var(--line);border-radius:var(--r-s);color:var(--chalk);font:inherit;font-weight:600;padding:12px 14px}
.fields input:focus{outline:2px solid var(--signal);outline-offset:1px}
.resetzone{margin-top:30px;padding-top:20px;border-top:1px dashed var(--line);text-align:center}
.reset-open{background:none;border:0;color:var(--mist);opacity:.75;font-size:14px;font-weight:600;padding:10px 14px;text-decoration:underline;text-underline-offset:3px}
.reset-ask{text-align:left}
.reset-ask p{margin:0 0 14px;color:var(--chalk);line-height:1.4}
.reset-ask p span{display:block;color:var(--mist);font-size:14px;margin-top:4px}
.reset-ask .two .btn{padding:15px 8px;font-size:16px}
@media (max-width:340px){.pts{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style></head>
<body>
<div class="offline" id="offline">Нет связи с сервером — переподключаюсь…</div>
<main class="app">
  <div class="top"><span class="wordmark"><i></i>Кольцеброс</span><a class="link" href="{{ base }}screen" target="_blank" rel="noopener">Экран для гостей</a></div>

  <section class="card" id="setup">
    <h2>Сколько участников</h2>
    <div class="stepper"><button id="minusCount" aria-label="Меньше">−</button><div class="num" id="count">4</div><button id="plusCount" aria-label="Больше">+</button></div>
    <button class="btn primary" id="begin">Начать конкурс</button>
  </section>

  <section class="card" id="game" hidden>
    <div class="who"><span class="name" id="name"></span><span class="of" id="of"></span></div>
    <div class="score"><div class="num" id="score">0</div><div class="unit" id="unit">баллов</div></div>
    <div class="pts" id="pts">{% for p in points %}<button class="pt" data-p="{{ p }}" aria-label="Плюс {{ p }} баллов">+{{ p }}</button>{% endfor %}</div>
    <div class="stack">
      <button class="btn quiet" id="undo">Отменить последнее</button>
      <button class="btn primary" id="next"></button>
    </div>
  </section>

  <section class="card" id="final" hidden>
    <div class="who"><span class="name">Конкурс завершён</span></div>
    <p class="hint" style="text-align:left;margin:8px 0 0">Итоги уже на экране для гостей.</p>
  </section>

  <section class="card" id="results" hidden><h2 id="resultsTitle">Уже сыграли</h2><div class="rows" id="rows"></div></section>

  <section class="card" id="names" hidden>
    <h2>Имена участников</h2>
    <div class="fields" id="nameFields"></div>
    <p class="hint" style="text-align:left;margin:10px 0 0">Пустое поле — снова «Участник N». Имя сохраняется сразу.</p>
  </section>

  <div class="spacer"></div>
  <button class="namelink" id="namesToggle" hidden>Имена участников</button>
  <div class="resetzone" id="resetZone" hidden>
    <button class="reset-open" id="resetOpen"></button>
    <div class="reset-ask" id="resetAsk" hidden>
      <p><b id="resetQ"></b><span id="resetSub"></span></p>
      <div class="two"><button class="btn quiet" id="resetNo">Отмена</button><button class="btn danger" id="resetYes" disabled></button></div>
    </div>
  </div>
</main>
<script>{{ client_js|safe }}</script>
<script>
const $ = id => document.getElementById(id);
let count = 4;

/* ---------- связь ----------
   Все команды уходят через send(): если связи нет, команда не копится в очереди
   (иначе баллы начислятся позже, когда никто не ждёт), а сверху видна красная полоса.
   После каждой команды просим свежее состояние; если ответа нет — тихо переподключаемся. */
let pendingAt = 0, lastState = performance.now();
socket.on('state', () => { pendingAt = 0; lastState = performance.now(); });
function reconnect(){
  pendingAt = 0; lastState = performance.now();
  offlineBar && offlineBar.classList.add('on');
  try { socket.disconnect(); socket.connect(); } catch (e) {}
}
function send(name, data){
  if (!socket.connected) { offlineBar && offlineBar.classList.add('on'); return false; }
  socket.emit(name, data || {});
  socket.emit('sync');
  if (!pendingAt) pendingAt = performance.now();
  return true;
}
setInterval(() => {
  const now = performance.now();
  if (pendingAt && now - pendingAt > 3500) return reconnect();     // команда ушла, а ответа нет
  if (document.hidden) return;
  if (socket.connected) socket.emit('sync');                          // живой пульс: страница никогда не «застывает» молча
  if (now - lastState > 12000) reconnect();
}, 2000);

$('minusCount').onclick = () => { count = Math.max(1, count - 1); setText($('count'), count); };
$('plusCount').onclick = () => { count = Math.min(30, count + 1); setText($('count'), count); };
$('begin').onclick = () => send('setup', {count});

let namesOpen = false, namesKey = '';
$('namesToggle').onclick = () => { namesOpen = !namesOpen; namesKey = ''; $('names').hidden = !namesOpen; $('namesToggle').textContent = namesOpen ? 'Скрыть имена' : 'Имена участников'; if (namesOpen) $('names').scrollIntoView({behavior: 'smooth', block: 'start'}); };
function renderNames(){
  if (!namesOpen || !S) return;
  const key = S.participants.length + ':' + S.current;
  const box = $('nameFields');
  if (key !== namesKey) {   // поля пересобираются только при смене состава, чтобы не мешать вводу
    namesKey = key;
    box.innerHTML = S.participants.map((p, i) => `<label><span class="num">${i + 1}</span><input data-i="${i}" maxlength="32" placeholder="Участник ${i + 1}" value="${esc(/^Участник \d+$/.test(p.name) ? '' : p.name)}"></label>`).join('');
  }
  box.querySelectorAll('input').forEach(inp => {
    if (document.activeElement === inp) return;
    const p = S.participants[+inp.dataset.i]; if (!p) return;
    const v = /^Участник \d+$/.test(p.name) ? '' : p.name;
    if (inp.value !== v) inp.value = v;
  });
}
$('nameFields').addEventListener('change', e => { const inp = e.target.closest('input'); if (inp) send('rename', {index: +inp.dataset.i, name: inp.value}); });
$('nameFields').addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); e.target.blur(); } });

/* ---------- сброс игры: внизу, в два шага ---------- */
let resetT = 0, resetUnlockT = 0;
function closeReset(){ clearTimeout(resetT); clearTimeout(resetUnlockT); $('resetAsk').hidden = true; $('resetOpen').hidden = false; $('resetYes').disabled = true; }
$('resetOpen').onclick = () => {
  $('resetOpen').hidden = true; $('resetAsk').hidden = false; $('resetYes').disabled = true;
  resetUnlockT = setTimeout(() => { $('resetYes').disabled = false; }, 700);   // защита от случайного двойного нажатия
  resetT = setTimeout(closeReset, 10000);
  $('resetAsk').scrollIntoView({behavior: 'smooth', block: 'nearest'});
};
$('resetNo').onclick = closeReset;
$('resetYes').onclick = () => { if ($('resetYes').disabled) return; closeReset(); send('reset'); };

/* ---------- баллы ---------- */
function addPoints(points){
  if (!S || S.finished || !S.participants[S.current]) return;
  if (navigator.vibrate) navigator.vibrate(18);
  send('add', {points});
}
$('pts').addEventListener('click', e => { const b = e.target.closest('button'); if (b && !b.disabled) addPoints(+b.dataset.p); });
$('undo').onclick = () => { if (S && !S.finished) send('undo'); };
$('next').onclick = () => { if (S && !S.finished) send('next'); };
// Горячие клавиши для компьютера: 1–5 — баллы, Backspace — отмена
document.addEventListener('keydown', e => {
  if (e.repeat || !S || e.target.closest('input')) return;
  if (e.key >= '1' && e.key <= '5') { e.preventDefault(); addPoints(S.points[+e.key - 1]); }
  else if (e.key === 'Backspace') { e.preventDefault(); $('undo').click(); }
});

function frame(){
  if (!S) return;
  const ph = phaseOf(S), p = S.participants[S.current];
  const show = (id, on) => { const el = $(id); if (el.hidden === on) el.hidden = !on; };
  show('setup', ph === 'idle');
  show('game', !!(p && !S.finished));
  show('final', ph === 'finished');
  show('resetZone', ph !== 'idle');
  show('namesToggle', ph !== 'idle');
  if (ph === 'idle') { namesOpen = false; $('names').hidden = true; setText($('namesToggle'), 'Имена участников'); closeReset(); }
  const fin = ph === 'finished';
  setText($('resetOpen'), fin ? 'Начать новый конкурс' : 'Сбросить игру');
  setText($('resetQ'), fin ? 'Начать новый конкурс?' : 'Сбросить игру?');
  setText($('resetSub'), fin ? 'Итоги исчезнут, участников придётся задать заново.' : 'Все участники и результаты удалятся.');
  setText($('resetYes'), fin ? 'Да, начать заново' : 'Да, сбросить');
  renderNames();
  if (p && !S.finished) {
    const isLast = S.current === S.participants.length - 1;
    setText($('name'), p.name);
    setText($('of'), (S.current + 1) + ' из ' + S.participants.length);
    setRoll($('score'), p.score);
    setText($('unit'), ptsWord(p.score));
    const noScore = p.score === 0;
    if ($('undo').disabled !== noScore) $('undo').disabled = noScore;
    setText($('next'), isLast ? 'Показать итоги' : 'Следующий участник');
  }
  const done = ranking(S);
  show('results', done.length > 0);
  setText($('resultsTitle'), fin ? 'Итоги' : 'Уже сыграли');
  setHtml($('rows'), done.map((q, i) => `<div class="row${i === 0 ? ' first' : ''}"><span>${esc(q.name)}</span><b>${q.score}</b></div>`).join(''));
}
// Одна ошибка в отрисовке не должна навсегда останавливать страницу
function tick(){ try { frame(); } catch (e) { console.error(e); } requestAnimationFrame(tick); }
requestAnimationFrame(tick);
</script></body></html>"""

# ======================================================================
# Экран для гостей
# ======================================================================
SCREEN_HTML = r"""<!doctype html>
<html lang="ru" data-theme="women" data-base="{{ base }}">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Кольцеброс</title>
""" + FONTS + r"""
<style>{{ theme_css|safe }}
html,body{height:100%;overflow:hidden}
body{background:radial-gradient(60% 70% at 50% 55%,var(--signal-soft),transparent 70%),radial-gradient(90% 80% at 0 0,var(--ink-2),var(--ink) 65%)}
.screen{height:100vh;display:grid;grid-template-rows:auto 1fr;padding:3.2vh 4.5vw 4vh}
.head{display:flex;justify-content:space-between;align-items:center}
.head .wordmark{font-size:clamp(20px,2vw,34px)}
.head .round{color:var(--mist);font-weight:600;font-size:clamp(16px,1.4vw,24px)}
/* --- игра: имя и огромный счёт --- */
.play{position:relative;display:flex;min-height:0;--side:clamp(220px,21vw,380px)}
/* счёт строго по центру: слева и справа одинаковые поля шириной с рейтинг */
.play>.game{flex:1;min-width:0;padding:0 calc(var(--side) + 2vw)}
.play .who{max-width:100%;overflow-wrap:anywhere}
.game{position:relative;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;min-height:0}
/* результаты справа: сыгравшие + текущий участник вживую */
.side{position:absolute;right:0;top:50%;transform:translateY(-50%);width:var(--side);display:flex;flex-direction:column}
.side-h{color:var(--mist);font-weight:600;font-size:clamp(15px,1.3vw,24px);margin:0 0 1.4vh .2em}
.side-list{position:relative;--rh:clamp(38px,6.6vh,64px)}
.srow{position:absolute;left:0;right:0;top:0;height:var(--rh);display:grid;grid-template-columns:1.6em minmax(0,1fr) auto;align-items:center;gap:.7em;padding:0 .9em;border-radius:var(--r-m);
  background:rgba(38,16,31,.82);border:1px solid var(--line);font-size:clamp(14px,calc(var(--rh) * .38),26px);
  transition:transform .7s cubic-bezier(.2,.8,.2,1),opacity .4s,border-color .3s,background .3s;backdrop-filter:blur(2px)}
.srow .pl{color:var(--mist)}
.srow .pn{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:flex;align-items:center;gap:.5em}
.srow .sc{color:var(--chalk)}
.srow.lead .sc{color:var(--signal)}
.srow.now{border-color:var(--signal);background:rgba(255,79,168,.1)}
.srow.now .pn:before{content:"";flex:none;width:.5em;height:.5em;border-radius:50%;border:.13em solid var(--signal);box-shadow:0 0 10px var(--signal);animation:live 1.4s ease-in-out infinite}
@keyframes live{50%{opacity:.35}}
.srow.gone{opacity:0}
/* кнопка звука */
.sound{position:fixed;right:20px;bottom:20px;z-index:50;border:1px solid var(--line);background:rgba(38,16,31,.9);color:var(--chalk);border-radius:999px;padding:12px 20px;font-weight:600;font-size:16px;cursor:pointer;transition:opacity .4s}
.sound:hover{border-color:var(--signal)}
.sound.off{opacity:0;pointer-events:none}
.who{font-family:var(--display);font-weight:800;font-size:clamp(32px,4.2vw,80px);line-height:1}
.count{font-size:min(27vw,42vh);line-height:1;color:var(--signal);margin:1.5vh 0 0;filter:drop-shadow(0 0 40px var(--signal-soft))}
.unit{color:var(--mist);font-weight:600;font-size:clamp(20px,2vw,38px);margin-top:1vh}
/* вспышка «+20» над счётом */
.pop{position:absolute;left:50%;top:44%;z-index:3;pointer-events:none;opacity:0;font-family:var(--digits);font-weight:var(--digits-w);
  font-size:clamp(60px,9vw,150px);line-height:1;color:var(--signal);text-shadow:0 0 30px var(--signal),0 0 70px var(--signal-soft);transform:translate(-50%,-50%);white-space:nowrap}
.pop.go{animation:popUp 1.1s cubic-bezier(.2,.8,.2,1) both}
@keyframes popUp{0%{opacity:0;transform:translate(-50%,-30%) scale(.6)}18%{opacity:1;transform:translate(-50%,-70%) scale(1.12)}60%{opacity:1}100%{opacity:0;transform:translate(-50%,-150%) scale(1)}}
/* --- полноэкранные состояния --- */
.full{display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;min-height:0}
.full .big{font-family:var(--display);font-weight:900;font-size:clamp(70px,10vw,190px);line-height:.95;letter-spacing:-.02em}
.full .sub{color:var(--mist);font-weight:600;font-size:clamp(20px,2vw,36px);margin-top:3vh}
/* --- итоги: таблица, подстраивается под число участников --- */
.final{display:flex;flex-direction:column;min-height:0}
.final h1{font-family:var(--display);font-weight:900;font-size:clamp(40px,4.6vw,88px);margin:1.5vh 0 2.5vh;line-height:1}
.board{flex:1;min-height:0;display:grid;grid-auto-flow:column;grid-template-rows:repeat(var(--rows),auto);grid-template-columns:repeat(var(--cols),minmax(0,1fr));column-gap:3vw;align-content:start}
.board{--rh:calc((100vh - 3.2vh*2 - 3vw - 14vh) / var(--rows))}
.trow{display:grid;grid-template-columns:2.2em minmax(0,1fr) auto;align-items:center;gap:.8em;padding:0 .9em;height:min(var(--rh) - 6px,14vh);margin-bottom:6px;border-radius:var(--r-m);background:var(--surface);border:1px solid var(--line);font-size:clamp(14px,calc(var(--rh) * .38),46px);opacity:0;animation:rise .45s cubic-bezier(.2,.8,.2,1) forwards}
@keyframes rise{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.trow .pl{color:var(--mist)}
.trow .pn{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.trow .sc{--roll-ms:900ms}
.trow.win{background:var(--signal);border-color:var(--signal);color:var(--signal-ink)}
.trow.win .pl{color:var(--signal-ink)}
</style></head>
<body>
<canvas id="bg" aria-hidden="true"></canvas>
<div class="offline" id="offline">Нет связи с сервером — переподключаюсь…</div>
<div class="screen">
  <div class="head"><span class="wordmark"><i></i>Кольцеброс</span><span class="round" id="round"></span></div>

  <div class="full" id="idle"><div class="big">Кольцеброс</div><div class="sub">Скоро начнём</div></div>

  <div class="play" id="game" hidden>
    <div class="game">
      <div class="who" id="who"></div>
      <div class="count num" id="count"></div>
      <div class="unit" id="unit">баллов</div>
      <div class="pop" id="pop"></div>
    </div>
    <aside class="side"><div class="side-h">Результаты</div><div class="side-list" id="sideList"></div></aside>
  </div>

  <div class="final" id="final" hidden><h1>Итоги</h1><div class="board" id="board"></div></div>
</div>
<button class="sound" id="soundBtn">Включить звук</button>
<script>{{ client_js|safe }}</script>
<script>{{ bg_js|safe }}</script>
<script>
const $ = id => document.getElementById(id);
let shownScore = null, shownPlayer = null, lastView = '', lastFinalKey = '', lastPh = null, seenN = null;

/* ---------- Звуки: синтез в браузере, файлы не нужны ---------- */
const Snd = (() => {
  let ctx = null, master = null;
  function ensure(){
    if (!ctx) { ctx = new (window.AudioContext || window.webkitAudioContext)(); master = ctx.createGain(); master.gain.value = .8; master.connect(ctx.destination); }
    return ctx;
  }
  function tone(freq, dur, {type = 'sine', gain = .3, to = null, at = 0, attack = .005} = {}){
    if (!ctx || ctx.state !== 'running') return;
    const t = ctx.currentTime + at, o = ctx.createOscillator(), g = ctx.createGain();
    o.type = type; o.frequency.setValueAtTime(freq, t);
    if (to) o.frequency.exponentialRampToValueAtTime(to, t + dur);
    g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(gain, t + attack);
    g.gain.exponentialRampToValueAtTime(.0001, t + dur);
    o.connect(g); g.connect(master); o.start(t); o.stop(t + dur + .05);
  }
  // короткий сухой щелчок: всплеск шума через полосовой фильтр
  let noise = null;
  function click(at, gain = .6, freq = 3000){
    if (!ctx || ctx.state !== 'running') return;
    if (!noise) { noise = ctx.createBuffer(1, Math.floor(ctx.sampleRate * .05), ctx.sampleRate); const ch = noise.getChannelData(0); for (let i = 0; i < ch.length; i++) ch[i] = Math.random() * 2 - 1; }
    const t = ctx.currentTime + at, src = ctx.createBufferSource(), bp = ctx.createBiquadFilter(), g = ctx.createGain();
    src.buffer = noise; bp.type = 'bandpass'; bp.frequency.value = freq; bp.Q.value = 1.4;
    g.gain.setValueAtTime(gain, t); g.gain.exponentialRampToValueAtTime(.001, t + .028);
    src.connect(bp); bp.connect(g); g.connect(master); src.start(t); src.stop(t + .05);
  }
  return {
    get on(){ return !!ctx && ctx.state === 'running'; },
    async enable(){ ensure(); try { await ctx.resume(); } catch (e) {} return this.on; },
    tryAuto(){ ensure(); ctx.resume().catch(() => {}); return this.on; },
    // чем больше баллов за бросок, тем выше и «богаче» звон кольца
    plus(p = 10, d = 0){ const f = 520 + (p - 10) * 14; click(d); tone(f, .24, {type: 'triangle', gain: .3, at: d}); tone(f * 1.5, .34, {gain: .12, at: d + .04}); if (p >= 25) tone(f * 2, .4, {gain: .08, at: d + .08}); },
    minus(d = 0){ tone(440, .18, {type: 'triangle', gain: .18, to: 300, at: d}); },                  // отмена начисления
    fanfare(d = 0){ [523, 659, 784, 1047].forEach((f, i) => { tone(f, .55, {type: 'triangle', gain: .28, at: d + i * .14}); tone(f * 2, .45, {gain: .07, at: d + i * .14}); }); },
  };
})();
const soundBtn = $('soundBtn');
soundBtn.onclick = async () => { if (await Snd.enable()) { soundBtn.classList.add('off'); Snd.plus(20); } };
setTimeout(() => { if (Snd.tryAuto()) soundBtn.classList.add('off'); }, 300);

/* Прокрутка цифры: новая цифра встаёт на место примерно на 40% длительности
   (кривая с лёгким «перелётом»). Звук ставим ровно на этот момент. */
const LAND = .4;
function landDelay(el){ const ms = parseFloat(getComputedStyle(el).getPropertyValue('--roll-ms')) || 560; return ms / 1000 * LAND; }

/* ---------- Результаты справа ---------- */
const sideRows = new Map();
function renderSide(){
  const list = $('sideList');
  const rowH = list.querySelector('.srow') ? list.querySelector('.srow').offsetHeight : Math.max(38, Math.min(innerHeight * .066, 64));
  const step = rowH + 8, maxRows = Math.max(3, Math.floor((innerHeight * .68) / step));
  const ranked = S.participants.map((p, i) => ({...p, i})).filter(p => p.done || p.i === S.current)
    .sort((a, b) => b.score - a.score || (b.done - a.done) || a.i - b.i);
  let place = 0, prev = null;
  ranked.forEach((p, k) => { if (p.score !== prev) { place = k + 1; prev = p.score; } p.place = place; });
  let shown = ranked;
  if (ranked.length > maxRows) {
    shown = ranked.slice(0, maxRows - 1);
    const curP = ranked.find(p => p.i === S.current);
    shown.push(curP && !shown.includes(curP) ? curP : ranked[maxRows - 1]);
  }
  const visible = new Set(shown.map(p => p.i));
  shown.forEach((p, k) => {
    let el = sideRows.get(p.i);
    if (!el) {
      el = document.createElement('div'); el.className = 'srow gone';
      el.innerHTML = `<span class="pl num"></span><span class="pn"></span><span class="sc num"></span>`;
      el.style.transform = `translateY(${k * step}px)`;
      list.appendChild(el); sideRows.set(p.i, el); void el.offsetWidth;
    }
    el.classList.remove('gone');
    el.classList.toggle('now', p.i === S.current && !p.done);
    el.classList.toggle('lead', p.place === 1 && p.score > 0);
    el.style.transform = `translateY(${k * step}px)`; el.style.zIndex = p.i === S.current ? 2 : 1;
    el.querySelector('.pl').textContent = p.place;
    el.querySelector('.pn').textContent = p.name;
    setRoll(el.querySelector('.sc'), p.score);
  });
  sideRows.forEach((el, i) => { if (!visible.has(i)) el.classList.add('gone'); });
  list.style.height = (shown.length * step) + 'px';
}
/* длинное имя уменьшаем, чтобы оно влезло в колонку не больше чем в две строки */
function fitWho(){
  const el = $('who'); el.style.fontSize = '';
  let size = parseFloat(getComputedStyle(el).fontSize), lh = size * 1.05;
  for (let k = 0; k < 30 && (el.scrollWidth > el.clientWidth + 1 || el.offsetHeight > lh * 2.2); k++) {
    size *= .92; el.style.fontSize = size + 'px'; lh = size * 1.05;
  }
}
addEventListener('resize', () => { if (!$('game').hidden) fitWho(); });
function clearSide(){ sideRows.forEach(el => el.remove()); sideRows.clear(); }

function setView(v){
  if (v === lastView) return; lastView = v;
  ['idle','game','final'].forEach(id => $(id).hidden = id !== v);
}

/* «+20» над счётом на каждое начисление */
function firePop(last){
  const pop = $('pop');
  pop.className = 'pop';
  pop.textContent = '+' + last.p;
  void pop.offsetWidth; pop.classList.add('go');
}

function renderFinal(){
  const list = S.participants.map((p, i) => ({...p, i})).sort((a, b) => b.score - a.score || a.i - b.i);
  const key = JSON.stringify(list.map(p => [p.i, p.score]));
  if (key === lastFinalKey) return; lastFinalKey = key;
  const n = list.length, cols = n <= 8 ? 1 : n <= 18 ? 2 : 3, rows = Math.ceil(n / cols);
  const b = $('board'); b.style.setProperty('--cols', cols); b.style.setProperty('--rows', rows);
  const top = n ? list[0].score : 0;
  let place = 0, prev = null;
  b.innerHTML = list.map((p, k) => {
    if (p.score !== prev) { place = k + 1; prev = p.score; }
    const win = place === 1 && top > 0;
    return `<div class="trow${win ? ' win' : ''}" style="animation-delay:${Math.min(k, 20) * .06}s">
      <span class="pl num">${place}</span><span class="pn">${esc(p.name)}</span><span class="sc num" data-v="${p.score}"></span></div>`;
  }).join('');
  b.querySelectorAll('.sc').forEach((el, k) => { setRoll(el, 0, {up: true}); setTimeout(() => setRoll(el, el.dataset.v, {up: true}), 250 + Math.min(k, 20) * 60); });
}

function frame(){
  if (!S) { seenN = null; return; }
  const ph = phaseOf(S), p = S.participants[S.current];
  $('round').textContent = p && !S.finished ? `${S.current + 1} из ${S.participants.length}` : '';
  // что сейчас сделал ведущий: вспышка и звук. Первое состояние после загрузки только запоминаем, без анимации.
  const n = S.last ? S.last.n : -1;
  const fresh = seenN !== null && n !== seenN && S.last;
  seenN = n;
  if (ph === 'finished' && lastPh && lastPh !== 'finished') Snd.fanfare();
  lastPh = ph;
  if (ph === 'idle') { setView('idle'); shownScore = shownPlayer = null; lastFinalKey = ''; clearSide(); }
  else if (ph === 'finished') { setView('final'); renderFinal(); shownScore = shownPlayer = null; clearSide(); }
  else {
    setView('game');
    lastFinalKey = '';
    if ($('who').textContent !== p.name) { $('who').textContent = p.name; fitWho(); }
    if (shownPlayer !== S.current || shownScore !== p.score) {
      setRoll($('count'), p.score);
      $('unit').textContent = ptsWord(p.score);
      shownScore = p.score; shownPlayer = S.current;
    }
    if (fresh) {
      if (S.last.kind === 'add') { Bg.pulse(); firePop(S.last); Snd.plus(S.last.p, landDelay($('count'))); }
      else Snd.minus();
    }
    renderSide();
  }
}
// Одна ошибка в отрисовке не должна навсегда останавливать экран
function tick(){ try { frame(); } catch (e) { console.error(e); } requestAnimationFrame(tick); }
requestAnimationFrame(tick);
</script></body></html>"""

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), allow_unsafe_werkzeug=True)
