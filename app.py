from dotenv import load_dotenv
load_dotenv()
"""
MedicNEET Telegram Mini App - Cash Prize Quiz
Backend: FastAPI + SQLite + Daily Email Export
"""
import os, io, csv, json, time, hashlib, hmac, sqlite3, asyncio, logging, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from urllib.parse import parse_qsl
import httpx
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ─── CONFIG ────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@your_channel")
QUESTION_INTERVAL_HOURS = int(os.getenv("QUESTION_INTERVAL_HOURS", "4"))
PRIZE_WINDOW_MINUTES = int(os.getenv("PRIZE_WINDOW_MINUTES", "1"))  # Prize only for first X minutes
CASH_PRIZE = int(os.getenv("CASH_PRIZE", "50"))
DB_PATH = os.getenv("DB_PATH", "medicneet.db")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://yourdomain.com")
APP_STATUS = os.getenv("APP_STATUS", "launching_soon")  # "launching_soon" or "live"
PLAYSTORE_LINK = os.getenv("PLAYSTORE_LINK", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "medicneet.team@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "YOUR_GMAIL_APP_PASSWORD")
EXPORT_TO_EMAIL = os.getenv("EXPORT_TO_EMAIL", "medicneet.team@gmail.com")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for sql in [
        """CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, question TEXT NOT NULL,
            option_a TEXT NOT NULL, option_b TEXT NOT NULL, option_c TEXT NOT NULL, option_d TEXT NOT NULL,
            correct_answer TEXT NOT NULL, explanation TEXT, chapter TEXT, difficulty TEXT, sheet_row INTEGER UNIQUE)""",
        """CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT, question_id INTEGER NOT NULL,
            started_at TEXT NOT NULL, ends_at TEXT NOT NULL, prize_ends_at TEXT,
            winner_user_id TEXT, winner_name TEXT, winner_time_ms INTEGER,
            winner_photo_path TEXT, winner_upi_id TEXT, announced INTEGER DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, round_id INTEGER NOT NULL,
            user_id TEXT NOT NULL, user_name TEXT, selected_answer TEXT NOT NULL,
            is_correct INTEGER NOT NULL, time_ms INTEGER NOT NULL,
            attempted_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(round_id, user_id))""",
        """CREATE TABLE IF NOT EXISTS winners (
            id INTEGER PRIMARY KEY AUTOINCREMENT, round_id INTEGER NOT NULL,
            user_id TEXT NOT NULL, user_name TEXT, photo_path TEXT, upi_id TEXT,
            time_ms INTEGER, prize_amount INTEGER DEFAULT 50, paid INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS notify_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE,
            user_id TEXT, user_name TEXT, source TEXT DEFAULT 'miniapp',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS email_export_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, exported_at TEXT NOT NULL,
            email_count INTEGER, status TEXT)"""
    ]:
        c.execute(sql)
    conn.commit(); conn.close()
    logger.info("Database initialized")

def get_db():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; return conn

def sync_questions_from_sheet():
    if not GOOGLE_SHEET_ID: return 0
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly","https://www.googleapis.com/auth/drive.readonly"])
        sheet = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID).sheet1
        rows = sheet.get_all_records(); conn = get_db(); c = conn.cursor(); count = 0
        for i, row in enumerate(rows, start=2):
            try:
                c.execute("INSERT OR REPLACE INTO questions (question,option_a,option_b,option_c,option_d,correct_answer,explanation,chapter,difficulty,sheet_row) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (str(row.get("Question","")),str(row.get("Option A","")),str(row.get("Option B","")),str(row.get("Option C","")),str(row.get("Option D","")),
                     str(row.get("Correct Answer","")).upper().strip(),str(row.get("Explanation","")),str(row.get("Chapter","")),str(row.get("Difficulty","")),i))
                count += 1
            except Exception as e: logger.error(f"Row {i}: {e}")
        conn.commit(); conn.close(); logger.info(f"Synced {count} questions"); return count
    except Exception as e: logger.error(f"Sheet sync failed: {e}"); return 0

def validate_telegram_data(init_data):
    try:
        parsed = dict(parse_qsl(init_data)); check_hash = parsed.pop("hash","")
        dcs = "\n".join(f"{k}={v}" for k,v in sorted(parsed.items()))
        sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest() == check_hash:
            return json.loads(parsed.get("user","{}"))
    except: pass
    return None

def is_valid_photo(fp):
    try:
        with open(fp,'rb') as f: h = f.read(4)
        ok = h.startswith(b'\xff\xd8\xff') or h.startswith(b'\x89PNG')
        return ok and 10*1024 < os.path.getsize(fp) < 5*1024*1024
    except: return False

def get_or_create_current_round(return_is_new=False):
    conn = get_db(); c = conn.cursor(); now = datetime.utcnow().isoformat()
    c.execute("SELECT * FROM rounds WHERE ends_at > ? ORDER BY started_at DESC LIMIT 1", (now,))
    rnd = c.fetchone()
    if rnd: 
        conn.close()
        return (dict(rnd), False) if return_is_new else dict(rnd)
    c.execute("SELECT id FROM questions WHERE id NOT IN (SELECT question_id FROM rounds ORDER BY started_at DESC LIMIT 50) ORDER BY RANDOM() LIMIT 1")
    q = c.fetchone()
    if not q: c.execute("SELECT id FROM questions ORDER BY RANDOM() LIMIT 1"); q = c.fetchone()
    if not q: 
        conn.close()
        return (None, False) if return_is_new else None
    started = datetime.utcnow()
    prize_ends = started + timedelta(minutes=PRIZE_WINDOW_MINUTES)
    ends = started + timedelta(hours=QUESTION_INTERVAL_HOURS)
    c.execute("INSERT INTO rounds (question_id, started_at, ends_at, prize_ends_at) VALUES (?,?,?,?)", (q["id"], started.isoformat(), ends.isoformat(), prize_ends.isoformat()))
    rid = c.lastrowid; conn.commit()
    c.execute("SELECT * FROM rounds WHERE id = ?", (rid,)); r = dict(c.fetchone()); conn.close()
    return (r, True) if return_is_new else r

async def send_winner_to_channel(round_id, winner_name, time_ms, photo_path=None):
    ts = time_ms/1000
    app_line = f"📱 <a href='{PLAYSTORE_LINK}'>⬇️ Download MedicNEET</a>" if APP_STATUS=="live" and PLAYSTORE_LINK else "📱 <b>MedicNEET App — Launching Soon!</b> 🔔"
    text = f"🏆 <b>WINNER!</b>\n\n👤 {winner_name}\n⚡ Solved in {ts:.1f}s\n💰 Wins ₹{CASH_PRIZE}!\n\n🔥 Next question in {QUESTION_INTERVAL_HOURS}h!\n\n{app_line}"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}"
    async with httpx.AsyncClient() as client:
        if photo_path and os.path.exists(photo_path):
            with open(photo_path,"rb") as p: await client.post(f"{url}/sendPhoto", data={"chat_id":CHANNEL_ID,"caption":text,"parse_mode":"HTML"}, files={"photo":p})
        else: await client.post(f"{url}/sendMessage", data={"chat_id":CHANNEL_ID,"text":text,"parse_mode":"HTML"})

async def send_new_round_to_channel():
    """Post new question alert with quiz button to channel"""
    text = f"""🚨 <b>NEW QUESTION LIVE!</b>

💰 ₹{CASH_PRIZE} for the fastest correct answer
⏱ Prize window: {PRIZE_WINDOW_MINUTES} minute only!
🏆 Winners announced with payment proof

👇 Answer now!"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}"
    button = {"inline_keyboard": [[{"text": "🧠 Play Quiz - Win ₹50!", "url": "https://quiz.medicneet.com/"}]]}
    async with httpx.AsyncClient() as client:
        await client.post(f"{url}/sendMessage", json={"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML", "reply_markup": button})

def export_emails_csv():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT email, user_name, source, created_at FROM notify_emails ORDER BY created_at DESC")
    rows = c.fetchall(); conn.close()
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["Email","Name","Source","Signed Up At"])
    for r in rows: w.writerow([r["email"], r["user_name"] or "", r["source"] or "miniapp", r["created_at"]])
    return out.getvalue().encode("utf-8"), len(rows)

def send_daily_email_export():
    try:
        csv_bytes, count = export_emails_csv()
        if count == 0: logger.info("No emails to export"); return
        today = datetime.utcnow().strftime("%Y-%m-%d")
        msg = MIMEMultipart(); msg["From"]=SMTP_USER; msg["To"]=EXPORT_TO_EMAIL
        msg["Subject"] = f"MedicNEET Emails Export ({today}) — {count} subscribers"
        msg.attach(MIMEText(f"Daily email export from MedicNEET Mini App.\n\nTotal: {count} emails\nDate: {today}\n\nCSV attached.\n\n— MedicNEET Bot","plain"))
        att = MIMEBase("application","octet-stream"); att.set_payload(csv_bytes)
        encoders.encode_base64(att); att.add_header("Content-Disposition", f"attachment; filename=medicneet_emails_{today}.csv")
        msg.attach(att)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s: s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        conn = get_db(); c = conn.cursor()
        c.execute("INSERT INTO email_export_log (exported_at, email_count, status) VALUES (?,?,?)", (datetime.utcnow().isoformat(), count, "success"))
        conn.commit(); conn.close(); logger.info(f"✅ Exported {count} emails to {EXPORT_TO_EMAIL}")
    except Exception as e:
        logger.error(f"❌ Email export failed: {e}")
        try:
            conn = get_db(); c = conn.cursor()
            c.execute("INSERT INTO email_export_log (exported_at, email_count, status) VALUES (?,?,?)", (datetime.utcnow().isoformat(), 0, f"failed: {str(e)[:200]}"))
            conn.commit(); conn.close()
        except: pass

def send_winner_notification_email(round_id, winner_name, upi_id, time_ms, user_id):
    """Instant email to Shahul when a winner claims their prize"""
    try:
        time_sec = time_ms / 1000 if time_ms else 0
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        msg = MIMEText(
            f"🏆 NEW WINNER — Pay ₹{CASH_PRIZE} Now!\n\n"
            f"Round: #{round_id}\n"
            f"Winner: {winner_name}\n"
            f"Telegram ID: {user_id}\n"
            f"UPI ID: {upi_id}\n"
            f"Solve Time: {time_sec:.1f} seconds\n"
            f"Prize: ₹{CASH_PRIZE}\n"
            f"Time: {now}\n\n"
            f"— MedicNEET Bot", "plain"
        )
        msg["From"] = SMTP_USER
        msg["To"] = EXPORT_TO_EMAIL
        msg["Subject"] = f"💰 Pay ₹{CASH_PRIZE} → {winner_name} — UPI: {upi_id}"
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        logger.info(f"✅ Winner email sent: {winner_name} / {upi_id}")
    except Exception as e:
        logger.error(f"❌ Winner email failed: {e}")

async def round_manager():
    last_export_date = None
    while True:
        try:
            conn = get_db(); c = conn.cursor(); now = datetime.utcnow(); now_str = now.isoformat()
            c.execute("SELECT r.id, r.winner_name, r.winner_time_ms, r.winner_photo_path FROM rounds r WHERE r.ends_at <= ? AND r.announced = 0 AND r.winner_user_id IS NOT NULL", (now_str,))
            for rnd in c.fetchall():
                await send_winner_to_channel(rnd["id"], rnd["winner_name"], rnd["winner_time_ms"], rnd["winner_photo_path"])
                c.execute("UPDATE rounds SET announced = 1 WHERE id = ?", (rnd["id"],))
            c.execute("UPDATE rounds SET announced = 1 WHERE ends_at <= ? AND announced = 0 AND winner_user_id IS NULL", (now_str,))
            conn.commit(); conn.close()
            # Check for new round and announce if created
            rnd, is_new = get_or_create_current_round(return_is_new=True)
            if is_new and rnd:
                await send_new_round_to_channel()
                logger.info(f"📢 New round announced: Round #{rnd['id']}")
            today_str = now.strftime("%Y-%m-%d")
            if now.hour == 2 and now.minute >= 30 and last_export_date != today_str:
                send_daily_email_export(); last_export_date = today_str
        except Exception as e: logger.error(f"Round manager: {e}")
        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(); sync_questions_from_sheet(); get_or_create_current_round()
    task = asyncio.create_task(round_manager()); yield; task.cancel()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
os.makedirs("static/uploads", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request":request,"playstore_link":PLAYSTORE_LINK,"app_status":APP_STATUS,"cash_prize":CASH_PRIZE})

@app.get("/api/current-round")
async def api_current_round():
    rnd = get_or_create_current_round()
    if not rnd: return JSONResponse({"error":"No questions"}, status_code=404)
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM questions WHERE id = ?", (rnd["question_id"],)); q = c.fetchone()
    c.execute("SELECT COUNT(*) as cnt FROM attempts WHERE round_id = ?", (rnd["id"],)); ac = c.fetchone()["cnt"]
    c.execute("SELECT user_name, time_ms FROM attempts WHERE round_id = ? AND is_correct = 1 ORDER BY time_ms ASC LIMIT 1", (rnd["id"],))
    f = c.fetchone(); conn.close()
    return {"round_id":rnd["id"],"ends_at":rnd["ends_at"],"prize_ends_at":rnd.get("prize_ends_at"),"question":{"text":q["question"],"option_a":q["option_a"],"option_b":q["option_b"],"option_c":q["option_c"],"option_d":q["option_d"],"chapter":q["chapter"]},"stats":{"attempts":ac,"fastest_name":f["user_name"] if f else None,"fastest_time_ms":f["time_ms"] if f else None}}

@app.post("/api/submit")
async def api_submit(request: Request):
    data = await request.json(); rid=data.get("round_id"); uid=str(data.get("user_id","")); un=data.get("user_name","Anon"); sel=data.get("answer","").upper().strip(); tms=int(data.get("time_ms",0))
    if not all([rid,uid,sel,tms]): raise HTTPException(400,"Missing fields")
    conn = get_db(); c = conn.cursor(); now = datetime.utcnow().isoformat()
    c.execute("SELECT * FROM rounds WHERE id = ? AND ends_at > ?", (rid,now)); rnd = c.fetchone()
    if not rnd: conn.close(); raise HTTPException(400,"Round ended")
    c.execute("SELECT id FROM attempts WHERE round_id = ? AND user_id = ?", (rid,uid))
    if c.fetchone(): conn.close(); raise HTTPException(400,"Already attempted")
    c.execute("SELECT correct_answer, explanation FROM questions WHERE id = ?", (rnd["question_id"],)); qd = c.fetchone()
    correct = qd["correct_answer"]; exp = qd["explanation"] or ""; ic = 1 if sel==correct else 0
    c.execute("INSERT INTO attempts (round_id,user_id,user_name,selected_answer,is_correct,time_ms) VALUES (?,?,?,?,?,?)", (rid,uid,un,sel,ic,tms))
    iw = False
    # Check if still in prize window
    prize_ends_at = rnd["prize_ends_at"]
    in_prize_window = prize_ends_at and now <= prize_ends_at
    if ic and in_prize_window:
        c.execute("SELECT MIN(time_ms) as best FROM attempts WHERE round_id = ? AND is_correct = 1", (rid,))
        if c.fetchone()["best"] == tms:
            c.execute("UPDATE rounds SET winner_user_id=?,winner_name=?,winner_time_ms=? WHERE id=?", (uid,un,tms,rid))
            c.execute("DELETE FROM winners WHERE round_id = ?", (rid,))
            c.execute("INSERT INTO winners (round_id,user_id,user_name,time_ms) VALUES (?,?,?,?)", (rid,uid,un,tms)); iw = True
    conn.commit()
    c.execute("SELECT user_name, time_ms FROM attempts WHERE round_id = ? AND is_correct = 1 ORDER BY time_ms ASC LIMIT 10", (rid,))
    lb = [dict(r) for r in c.fetchall()]; conn.close()
    return {"correct":bool(ic),"correct_answer":correct,"explanation":exp,"your_time_ms":tms,"is_current_winner":iw,"leaderboard":lb,"prize_window_active":in_prize_window}

@app.post("/api/winner-photo")
async def api_winner_photo(round_id:int=Form(...),user_id:str=Form(...),upi_id:str=Form(...),photo:UploadFile=File(...)):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM winners WHERE round_id = ? AND user_id = ?", (round_id,user_id))
    if not c.fetchone(): conn.close(); raise HTTPException(403,"Not the winner")
    ts = int(time.time()); fp = f"static/uploads/winner_{round_id}_{ts}.jpg"
    with open(fp,"wb") as f: f.write(await photo.read())
    if not is_valid_photo(fp): os.remove(fp); conn.close(); raise HTTPException(400,"Invalid photo")
    c.execute("UPDATE winners SET photo_path=?,upi_id=? WHERE round_id=? AND user_id=?", (fp,upi_id,round_id,user_id))
    c.execute("UPDATE rounds SET winner_photo_path=?,winner_upi_id=? WHERE id=?", (fp,upi_id,round_id))
    # Get winner details for email
    c.execute("SELECT winner_name, winner_time_ms FROM rounds WHERE id=?", (round_id,))
    rnd_info = c.fetchone()
    conn.commit(); conn.close()
    # Send instant email notification
    send_winner_notification_email(round_id, rnd_info["winner_name"] if rnd_info else "Unknown", upi_id, rnd_info["winner_time_ms"] if rnd_info else 0, user_id)
    return {"success":True,"message":"Details submitted! Prize will be sent within 24 hours 🏆"}

@app.get("/api/leaderboard")
async def api_leaderboard():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT user_name, COUNT(*) as wins, MIN(time_ms) as best_time, SUM(prize_amount) as total_won FROM winners GROUP BY user_id ORDER BY wins DESC, best_time ASC LIMIT 20")
    lb = [dict(r) for r in c.fetchall()]; conn.close(); return {"leaderboard":lb}

@app.get("/api/history")
async def api_history():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT r.id, r.started_at, r.winner_name, r.winner_time_ms, q.question, q.chapter FROM rounds r JOIN questions q ON q.id = r.question_id WHERE r.announced = 1 ORDER BY r.started_at DESC LIMIT 10")
    h = [dict(r) for r in c.fetchall()]; conn.close(); return {"history":h}

@app.get("/api/app-status")
async def api_app_status():
    return {"status":APP_STATUS,"playstore_link":PLAYSTORE_LINK}

@app.post("/api/notify-email")
async def api_notify_email(request: Request):
    data = await request.json(); email = str(data.get("email","")).strip().lower()
    uid = str(data.get("user_id","")); un = data.get("user_name","")
    if not email or "@" not in email or "." not in email: raise HTTPException(400,"Invalid email")
    conn = get_db(); c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO notify_emails (email,user_id,user_name,source) VALUES (?,?,?,'miniapp')", (email,uid,un))
    conn.commit()
    c.execute("SELECT COUNT(*) as cnt FROM notify_emails"); total = c.fetchone()["cnt"]; conn.close()
    return {"success":True,"total_signups":total}

@app.get("/api/notify-count")
async def api_notify_count():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) as cnt FROM notify_emails"); t = c.fetchone()["cnt"]; conn.close()
    return {"count":t}

@app.post("/api/sync-sheet")
async def api_sync_sheet():
    return {"synced":sync_questions_from_sheet()}

@app.get("/api/export-emails")
async def api_export_emails():
    send_daily_email_export(); return {"status":"triggered"}
