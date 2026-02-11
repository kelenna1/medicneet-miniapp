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
PRIZE_WINDOW_MINUTES = int(os.getenv("PRIZE_WINDOW_MINUTES", "2"))  # Prize only for first X minutes
CASH_PRIZE = int(os.getenv("CASH_PRIZE", "5"))
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question_1_id INTEGER NOT NULL,
            question_2_id INTEGER NOT NULL,
            question_3_id INTEGER NOT NULL,
            question_4_id INTEGER NOT NULL,
            started_at TEXT NOT NULL, ends_at TEXT NOT NULL, prize_ends_at TEXT,
            winner_user_id TEXT, winner_name TEXT, winner_time_ms INTEGER,
            winner_photo_path TEXT, winner_upi_id TEXT, announced INTEGER DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, round_id INTEGER NOT NULL,
            user_id TEXT NOT NULL, user_name TEXT, selected_answers TEXT NOT NULL,
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
            email_count INTEGER, status TEXT)""",
        """CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            balance INTEGER DEFAULT 0,
            total_earned INTEGER DEFAULT 0,
            upi_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            type TEXT NOT NULL,
            round_id INTEGER,
            status TEXT DEFAULT 'completed',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT,
            amount INTEGER NOT NULL,
            upi_id TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)"""
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
    # Select 4 random questions - try to avoid recently used ones first
    c.execute("""SELECT id FROM questions WHERE id NOT IN (
        SELECT question_1_id FROM rounds ORDER BY started_at DESC LIMIT 10
        UNION SELECT question_2_id FROM rounds ORDER BY started_at DESC LIMIT 10
        UNION SELECT question_3_id FROM rounds ORDER BY started_at DESC LIMIT 10
        UNION SELECT question_4_id FROM rounds ORDER BY started_at DESC LIMIT 10
    ) ORDER BY RANDOM() LIMIT 4""")
    questions = c.fetchall()
    if len(questions) < 4:
        # Not enough unused questions, just get any 4 random questions
        c.execute("SELECT id FROM questions ORDER BY RANDOM() LIMIT 4")
        questions = c.fetchall()
    if len(questions) < 4:
        conn.close()
        return (None, False) if return_is_new else None
    q_ids = [q["id"] for q in questions]
    started = datetime.utcnow()
    prize_ends = started + timedelta(minutes=PRIZE_WINDOW_MINUTES)
    ends = started + timedelta(hours=QUESTION_INTERVAL_HOURS)
    c.execute("INSERT INTO rounds (question_1_id, question_2_id, question_3_id, question_4_id, started_at, ends_at, prize_ends_at) VALUES (?,?,?,?,?,?,?)",
              (q_ids[0], q_ids[1], q_ids[2], q_ids[3], started.isoformat(), ends.isoformat(), prize_ends.isoformat()))
    rid = c.lastrowid; conn.commit()
    c.execute("SELECT * FROM rounds WHERE id = ?", (rid,)); r = dict(c.fetchone()); conn.close()
    # Trigger channel announcement for new round (run in background)
    import threading
    def announce():
        import asyncio
        asyncio.run(send_new_round_to_channel())
    threading.Thread(target=announce, daemon=True).start()
    logger.info(f"📢 New round announced: Round #{rid}")
    return (r, True) if return_is_new else r

async def send_winner_to_channel(round_id):
    # Fetch all top 10 winners from database
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_name, time_ms, prize_amount FROM winners WHERE round_id = ? ORDER BY time_ms ASC LIMIT 10", (round_id,))
    winners = c.fetchall()

    # Get total participants count
    c.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM attempts WHERE round_id = ?", (round_id,))
    total_participants = c.fetchone()["cnt"]

    conn.close()

    url = f"https://api.telegram.org/bot{BOT_TOKEN}"
    button = {"inline_keyboard": [[{"text": "🧠 Play Next Round", "url": "https://t.me/Winners_neetbot/Medicneet"}]]}

    if not winners:
        # No winners - nobody got 4/4
        text = f"""🏆 <b>ROUND #{round_id} RESULTS</b>

No winners this round! 😢
Nobody scored 4/4 correct.

Better luck next time!
🔥 Next round in 30 mins!"""
    else:
        # Build winner list
        winner_lines = []
        for i, w in enumerate(winners, start=1):
            name = w["user_name"] or "Anonymous"
            time_sec = w["time_ms"] / 1000
            prize = w["prize_amount"]
            winner_lines.append(f"{i}. {name} — 4/4 in {time_sec:.1f}s — ₹{prize} ✅")

        winner_text = "\n".join(winner_lines)
        total_prize = sum(w["prize_amount"] for w in winners)

        text = f"""🏆 <b>ROUND #{round_id} RESULTS</b>

{winner_text}

💰 Total paid: ₹{total_prize}
👥 Total participants: {total_participants}

🔥 Next round in 30 mins!"""

    async with httpx.AsyncClient() as client:
        await client.post(f"{url}/sendMessage", json={"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML", "reply_markup": button})

async def send_new_round_to_channel():
    """Post new question alert with quiz button to channel"""
    text = f"""🚨 <b>NEET 2026 - 4 High Level Biology Questions Posted!</b>

💰 Top 10 winners get ₹{CASH_PRIZE} each (₹50 total prize pool)
⏱ Prize window: {PRIZE_WINDOW_MINUTES} minutes only!
🏆 Winners announced with payment proof

👇 Answer now!"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}"
    button = {"inline_keyboard": [[{"text": "🧠 Play Quiz - Win ₹5!", "url": "https://t.me/Winners_neetbot?start=quiz"}]]}
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

def send_withdrawal_request_email(user_id, user_name, amount, upi_id, balance, total_earned):
    """Send email when user requests withdrawal"""
    try:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        msg = MIMEText(
            f"💰 New Withdrawal Request!\n\n"
            f"User: {user_name}\n"
            f"User ID: {user_id}\n"
            f"Amount: ₹{amount}\n"
            f"UPI ID: {upi_id}\n"
            f"Current Balance: ₹{balance}\n"
            f"Total Earned: ₹{total_earned}\n\n"
            f"Requested at: {now}\n\n"
            f"— MedicNEET Bot", "plain"
        )
        msg["From"] = SMTP_USER
        msg["To"] = "shahulhameedp49@gmail.com"
        msg["Subject"] = f"💰 Withdrawal Request - {user_name}"
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        logger.info(f"✅ Withdrawal email sent: {user_name} / ₹{amount}")
    except Exception as e:
        logger.error(f"❌ Withdrawal email failed: {e}")

async def round_manager():
    last_export_date = None
    while True:
        try:
            conn = get_db(); c = conn.cursor(); now = datetime.utcnow(); now_str = now.isoformat()
            c.execute("SELECT r.id FROM rounds r WHERE r.prize_ends_at <= ? AND r.announced = 0", (now_str,))
            for rnd in c.fetchall():
                await send_winner_to_channel(rnd["id"])
                c.execute("UPDATE rounds SET announced = 1 WHERE id = ?", (rnd["id"],))
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
    # Fetch all 4 questions
    q_ids = [rnd["question_1_id"], rnd["question_2_id"], rnd["question_3_id"], rnd["question_4_id"]]
    c.execute("SELECT * FROM questions WHERE id IN (?,?,?,?)", q_ids)
    questions_raw = c.fetchall()
    # Maintain order of questions as they were stored
    questions_dict = {q["id"]: q for q in questions_raw}
    questions = [
        {
            "text": questions_dict[q_id]["question"],
            "option_a": questions_dict[q_id]["option_a"],
            "option_b": questions_dict[q_id]["option_b"],
            "option_c": questions_dict[q_id]["option_c"],
            "option_d": questions_dict[q_id]["option_d"],
            "chapter": questions_dict[q_id]["chapter"]
        }
        for q_id in q_ids if q_id in questions_dict
    ]
    c.execute("SELECT COUNT(*) as cnt FROM attempts WHERE round_id = ?", (rnd["id"],)); ac = c.fetchone()["cnt"]
    c.execute("SELECT user_name, time_ms FROM attempts WHERE round_id = ? AND is_correct = 1 ORDER BY time_ms ASC LIMIT 1", (rnd["id"],))
    f = c.fetchone(); conn.close()
    return {"round_id":rnd["id"],"ends_at":rnd["ends_at"],"prize_ends_at":rnd.get("prize_ends_at"),"questions":questions,"stats":{"attempts":ac,"fastest_name":f["user_name"] if f else None,"fastest_time_ms":f["time_ms"] if f else None}}

@app.post("/api/submit")
async def api_submit(request: Request):
    data = await request.json()
    rid = data.get("round_id")
    uid = str(data.get("user_id", ""))
    un = data.get("user_name", "Anon")
    answers = data.get("answers", [])  # Array of 4 answers
    tms = int(data.get("time_ms", 0))

    # Validate input
    if not all([rid, uid, tms]) or not isinstance(answers, list) or len(answers) != 4:
        raise HTTPException(400, "Missing fields or invalid answers format")

    # Normalize answers
    answers = [str(a).upper().strip() for a in answers]

    conn = get_db(); c = conn.cursor(); now = datetime.utcnow().isoformat()
    c.execute("SELECT * FROM rounds WHERE id = ? AND ends_at > ?", (rid, now))
    rnd = c.fetchone()
    if not rnd:
        conn.close()
        raise HTTPException(400, "Round ended")

    c.execute("SELECT id FROM attempts WHERE round_id = ? AND user_id = ?", (rid, uid))
    if c.fetchone():
        conn.close()
        raise HTTPException(400, "Already attempted")

    # Fetch all 4 questions and their correct answers
    q_ids = [rnd["question_1_id"], rnd["question_2_id"], rnd["question_3_id"], rnd["question_4_id"]]
    c.execute("SELECT id, correct_answer, explanation FROM questions WHERE id IN (?,?,?,?)", q_ids)
    questions_raw = c.fetchall()
    questions_dict = {q["id"]: q for q in questions_raw}

    # Check each answer and collect results
    correct_answers = []
    explanations = []
    results = []
    all_correct = True

    for i, q_id in enumerate(q_ids):
        if q_id in questions_dict:
            correct_ans = questions_dict[q_id]["correct_answer"]
            user_ans = answers[i] if i < len(answers) else ""
            is_correct = user_ans == correct_ans

            correct_answers.append(correct_ans)
            explanations.append(questions_dict[q_id]["explanation"] or "")
            results.append(is_correct)

            if not is_correct:
                all_correct = False
        else:
            correct_answers.append("?")
            explanations.append("")
            results.append(False)
            all_correct = False

    # Store attempt with all answers as JSON
    ic = 1 if all_correct else 0
    c.execute("INSERT INTO attempts (round_id,user_id,user_name,selected_answers,is_correct,time_ms) VALUES (?,?,?,?,?,?)",
              (rid, uid, un, json.dumps(answers), ic, tms))

    iw = False
    # Check if still in prize window
    prize_ends_at = rnd["prize_ends_at"]
    in_prize_window = prize_ends_at and now <= prize_ends_at

    if ic and in_prize_window:
        # Add to winners table with ₹5 prize
        c.execute("INSERT INTO winners (round_id,user_id,user_name,time_ms,prize_amount) VALUES (?,?,?,?,?)", (rid, uid, un, tms, 5))

        # Keep only top 10 winners for this round
        c.execute("DELETE FROM winners WHERE round_id = ? AND id NOT IN (SELECT id FROM winners WHERE round_id = ? ORDER BY time_ms ASC LIMIT 10)", (rid, rid))

        # Check if user is in top 10
        c.execute("SELECT COUNT(*) as cnt FROM winners WHERE round_id = ? AND user_id = ?", (rid, uid))
        if c.fetchone()["cnt"] > 0:
            iw = True

            # Add ₹5 to wallet balance
            c.execute("INSERT INTO wallets (user_id, user_name, balance, total_earned, created_at, updated_at) VALUES (?,?,5,5,?,?) ON CONFLICT(user_id) DO UPDATE SET balance = balance + 5, total_earned = total_earned + 5, updated_at = ?",
                     (uid, un, now, now, now))

            # Create transaction record
            c.execute("INSERT INTO transactions (user_id, amount, type, round_id, status, created_at) VALUES (?,?,?,?,?,?)",
                     (uid, 5, "win", rid, "completed", now))

        # Update rounds table with fastest (1st place) winner
        c.execute("SELECT user_id, user_name, time_ms FROM winners WHERE round_id = ? ORDER BY time_ms ASC LIMIT 1", (rid,))
        fastest = c.fetchone()
        if fastest:
            c.execute("UPDATE rounds SET winner_user_id=?,winner_name=?,winner_time_ms=? WHERE id=?",
                     (fastest["user_id"], fastest["user_name"], fastest["time_ms"], rid))

    conn.commit()
    c.execute("SELECT user_name, time_ms FROM attempts WHERE round_id = ? AND is_correct = 1 ORDER BY time_ms ASC LIMIT 10", (rid,))
    lb = [dict(r) for r in c.fetchall()]
    conn.close()

    return {
        "all_correct": all_correct,
        "results": results,  # Array of True/False for each question
        "correct_answers": correct_answers,
        "explanations": explanations,
        "your_time_ms": tms,
        "is_current_winner": iw,
        "leaderboard": lb,
        "prize_window_active": in_prize_window
    }

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

@app.get("/api/leaderboard/alltime")
async def api_leaderboard_alltime(user_id: str = None):
    """Get top 100 players by total earnings from wallets table"""
    conn = get_db(); c = conn.cursor()

    # Get top 100 players sorted by total_earned
    c.execute("""
        SELECT
            w.user_id,
            w.user_name,
            w.total_earned,
            MIN(winners.time_ms) as best_time
        FROM wallets w
        LEFT JOIN winners ON winners.user_id = w.user_id
        WHERE w.total_earned > 0
        GROUP BY w.user_id
        ORDER BY w.total_earned DESC, best_time ASC
        LIMIT 100
    """)

    leaderboard = []
    user_rank = None

    for idx, row in enumerate(c.fetchall(), start=1):
        entry = {
            "rank": idx,
            "user_id": row["user_id"],
            "user_name": row["user_name"] or "Anonymous",
            "total_earned": row["total_earned"],
            "best_time": row["best_time"]
        }
        leaderboard.append(entry)

        # Track if current user is in top 100
        if user_id and row["user_id"] == user_id:
            user_rank = idx

    conn.close()

    return {
        "leaderboard": leaderboard,
        "user_rank": user_rank
    }

@app.get("/api/history")
async def api_history():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT r.id, r.started_at, r.winner_name, r.winner_time_ms, q.question, q.chapter FROM rounds r JOIN questions q ON q.id = r.question_1_id WHERE r.announced = 1 ORDER BY r.started_at DESC LIMIT 10")
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

@app.get("/api/wallet")
async def api_wallet(user_id: str):
    """Get wallet balance and transactions for a user"""
    if not user_id:
        raise HTTPException(400, "user_id required")

    conn = get_db(); c = conn.cursor()

    # Get wallet info
    c.execute("SELECT balance, total_earned, upi_id FROM wallets WHERE user_id = ?", (user_id,))
    wallet = c.fetchone()

    if not wallet:
        # Create wallet if doesn't exist
        conn.close()
        return {"balance": 0, "total_earned": 0, "upi_id": None, "transactions": []}

    # Get transactions (wins only)
    c.execute("SELECT amount, type, round_id, created_at FROM transactions WHERE user_id = ? AND type = 'win' ORDER BY created_at DESC LIMIT 50", (user_id,))
    transactions = [dict(t) for t in c.fetchall()]

    conn.close()

    return {
        "balance": wallet["balance"],
        "total_earned": wallet["total_earned"],
        "upi_id": wallet["upi_id"],
        "transactions": transactions
    }

@app.post("/api/withdraw")
async def api_withdraw(request: Request):
    """Submit withdrawal request"""
    data = await request.json()
    user_id = str(data.get("user_id", ""))
    user_name = data.get("user_name", "Unknown")
    upi_id = str(data.get("upi_id", "")).strip()

    if not user_id or not upi_id:
        raise HTTPException(400, "user_id and upi_id required")

    if not upi_id or "@" not in upi_id:
        raise HTTPException(400, "Invalid UPI ID format")

    conn = get_db(); c = conn.cursor()

    # Get current wallet balance
    c.execute("SELECT balance, total_earned, user_name FROM wallets WHERE user_id = ?", (user_id,))
    wallet = c.fetchone()

    if not wallet or wallet["balance"] < 50:
        conn.close()
        raise HTTPException(400, "Insufficient balance. Minimum withdrawal is ₹50")

    balance = wallet["balance"]
    total_earned = wallet["total_earned"]
    actual_user_name = wallet["user_name"] or user_name

    # Deduct full balance from wallet
    c.execute("UPDATE wallets SET balance = 0, upi_id = ?, updated_at = ? WHERE user_id = ?",
             (upi_id, datetime.utcnow().isoformat(), user_id))

    # Create withdrawal request
    c.execute("INSERT INTO withdrawal_requests (user_id, user_name, amount, upi_id, status, created_at) VALUES (?,?,?,?,?,?)",
             (user_id, actual_user_name, balance, upi_id, "pending", datetime.utcnow().isoformat()))

    # Create transaction record
    c.execute("INSERT INTO transactions (user_id, amount, type, status, created_at) VALUES (?,?,?,?,?)",
             (user_id, balance, "withdraw", "pending", datetime.utcnow().isoformat()))

    conn.commit()
    conn.close()

    # Send email notification
    send_withdrawal_request_email(user_id, actual_user_name, balance, upi_id, 0, total_earned)

    return {
        "success": True,
        "message": f"Withdrawal requested! You'll receive ₹{balance} within 24 hours",
        "amount": balance
    }

@app.get("/api/stats")
async def api_stats(user_id: str):
    """Get user's personal stats including rank and performance metrics"""
    if not user_id:
        raise HTTPException(400, "user_id required")

    conn = get_db(); c = conn.cursor()

    # Get wallet info (balance and total_earned)
    c.execute("SELECT balance, total_earned FROM wallets WHERE user_id = ?", (user_id,))
    wallet = c.fetchone()

    current_balance = wallet["balance"] if wallet else 0
    total_earned = wallet["total_earned"] if wallet else 0

    # Get best time from winners table
    c.execute("SELECT MIN(time_ms) as best_time FROM winners WHERE user_id = ?", (user_id,))
    best_time_row = c.fetchone()
    best_time = best_time_row["best_time"] if best_time_row and best_time_row["best_time"] else None

    # Get rounds played (distinct rounds in attempts table)
    c.execute("SELECT COUNT(DISTINCT round_id) as rounds_played FROM attempts WHERE user_id = ?", (user_id,))
    rounds_played_row = c.fetchone()
    rounds_played = rounds_played_row["rounds_played"] if rounds_played_row else 0

    # Get rounds won (count of wins in winners table)
    c.execute("SELECT COUNT(*) as rounds_won FROM winners WHERE user_id = ?", (user_id,))
    rounds_won_row = c.fetchone()
    rounds_won = rounds_won_row["rounds_won"] if rounds_won_row else 0

    # Calculate win rate
    win_rate = round((rounds_won / rounds_played * 100) if rounds_played > 0 else 0, 1)

    # Calculate rank based on total_earned (same logic as leaderboard)
    c.execute("""
        SELECT COUNT(*) + 1 as rank
        FROM wallets w1
        WHERE w1.total_earned > (
            SELECT COALESCE(total_earned, 0)
            FROM wallets
            WHERE user_id = ?
        )
    """, (user_id,))
    rank_row = c.fetchone()
    rank = rank_row["rank"] if rank_row else None

    # Get total number of players with earnings
    c.execute("SELECT COUNT(*) as total_players FROM wallets WHERE total_earned > 0")
    total_players_row = c.fetchone()
    total_players = total_players_row["total_players"] if total_players_row else 0

    conn.close()

    return {
        "rank": rank,
        "total_players": total_players,
        "total_earned": total_earned,
        "current_balance": current_balance,
        "best_time": best_time,
        "rounds_played": rounds_played,
        "rounds_won": rounds_won,
        "win_rate": win_rate
    }
