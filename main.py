import logging
import asyncio
import pytz 
import html
from html import escape
from telegram.constants import ParseMode
from datetime import datetime, time
from telegram.error import Forbidden, BadRequest
from telegram import Update, Poll, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    PollAnswerHandler,
    MessageHandler,
    filters,
    Defaults
)
import database as db



# ---------------- CONFIG ----------------
# Replace with your actual Bot Token
BOT_TOKEN = "7904843747:AAG2qKwxxU14H7Mw5dO94oFh5S2IBM7mWxM"
OWNER_ID = 6435499094

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Professional Defaults (MarkdownV2 for cleaner text)
# Note: Using standard Markdown for simplicity to avoid escape errors
defaults = Defaults(parse_mode="Markdown")

# ---------------- HELPERS ----------------
MAX_MESSAGE_LENGTH = 4000  # Safe limit

def split_message(text, max_length=MAX_MESSAGE_LENGTH):
    return [
        text[i:i + max_length]
        for i in range(0, len(text), max_length)
    ]

def apply_footer(text: str) -> str:
    """Applies the professional divider and custom footer text."""
    with db.get_db() as conn:
        f_row = conn.execute("SELECT value FROM settings WHERE key='footer_text'").fetchone()
        f_en = conn.execute("SELECT value FROM settings WHERE key='footer_enabled'").fetchone()
    
    footer_text = f_row[0] if f_row else "NEETIQBot"
    enabled = f_en[0] if f_en else "1"
    
    if enabled == '1':
        return f"{text}\n\n━━━━━━━━━━━━━━━━━━━\n{footer_text}"
    return text

async def is_admin(user_id: int) -> bool:
    """Check if a user has admin privileges or is the owner."""
    if user_id == OWNER_ID:
        return True
    with db.get_db() as conn:
        res = conn.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)).fetchone()
        return res is not None

# ---------------- REGISTRATION ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    
    with db.get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, username, first_name, joined_at) VALUES (?,?,?,?)",
            (user.id, user.username, user.first_name, str(datetime.now()))
        )
        if chat.type != 'private':
            conn.execute(
                "INSERT OR IGNORE INTO chats (chat_id, type, title, added_at) VALUES (?,?,?,?)",
                (chat.id, chat.type, chat.title, str(datetime.now()))
            )

    if chat.type == 'private':
        welcome = (
            f"👋 *Welcome to NEETIQBot, {user.first_name}!*\n\n"
            "I am your dedicated NEET preparation assistant. "
            "I provide high-quality MCQs, track your streaks, and manage competitive leaderboards.\n\n"
            "📌 *Use* /help *to see all available commands.*"
        )
        btn = [[InlineKeyboardButton("➕ Add Me to Group", url=f"https://t.me/{context.bot.username}?startgroup=true")]]
        await update.message.reply_text(apply_footer(welcome), reply_markup=InlineKeyboardMarkup(btn))
    else:
        group_msg = f"🎉 *Group successfully registered with NEETIQBot!*\n\nPreparing {chat.title} for upcoming quizzes."
        await update.message.reply_text(apply_footer(group_msg))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 *NEETIQBot Command List*\n\n"
        "👋 *Basic Commands*\n"
        "/start - Register and start the bot\n"
        "/help - Display this help manual\n\n"
        "📘 *Quiz System*\n"
        "/randomquiz - Receive a random NEET MCQ\n"
        "/myscore - View your point summary\n"
        "/mystats - Detailed performance analysis\n\n"
        "🏆 *Leaderboards*\n"
        "/leaderboard - Global rankings (Top 25)\n"
        "/groupleaderboard - Group specific rankings"
    )
    await update.message.reply_text(apply_footer(help_text))

# ---------------- QUIZ SYSTEM ----------------

async def send_random_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a random NEET MCQ and permanently deletes it from the DB to prevent repetition."""
    chat_id = update.effective_chat.id
    
    with db.get_db() as conn:
        # 1. Fetch any random question currently in the database
        q = conn.execute("SELECT * FROM questions ORDER BY RANDOM() LIMIT 1").fetchone()

    if not q:
        # No need to reset sent_questions because we are deleting questions permanently
        return await update.message.reply_text(
            "📭 *Database Empty!* All questions have been used and deleted. "
            "Please upload new questions using /addquestion."
        )

    options = [q['a'], q['b'], q['c'], q['d']]
    
    # Map correct answer (supports 1-4 or A-D)
    correct_map = {'1': 0, '2': 1, '3': 2, '4': 3, 'A': 0, 'B': 1, 'C': 2, 'D': 3}
    c_idx = correct_map.get(str(q['correct']).upper(), 0)

    try:
        # 2. Send the Poll
        msg = await context.bot.send_poll(
            chat_id=chat_id,
            question=f"🧠 NEET MCQ:\n\n{q['question']}",
            options=options,
            type=Poll.QUIZ,
            correct_option_id=c_idx,
            explanation=f"📖 *Explanation:*\n{q['explanation']}",
            is_anonymous=False
        )
        
        # 3. Track poll for answers and IMMEDIATELY delete the question
        with db.get_db() as conn:
            # Save poll info so /myscore works
            conn.execute("INSERT INTO active_polls VALUES (?,?,?)", (msg.poll.id, chat_id, c_idx))
            
            # Delete the question by ID so it can never be sent again
            conn.execute("DELETE FROM questions WHERE id = ?", (q['id'],))
            
    except Exception as e:
        logger.error(f"Error in Quiz Flow: {e}")
        await update.message.reply_text("❌ Failed to process the quiz. Please check database logs.")


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes the user's answer and auto-corrects user info in the DB."""
    answer = update.poll_answer
    poll_id = answer.poll_id
    user = answer.user  # This contains the most recent username/first_name
    user_id = user.id

    # 1. AUTO-CORRECT: Refresh user info so Leaderboard replaces ID with Name
    with db.get_db() as conn:
        conn.execute("""
            INSERT INTO users (user_id, username, first_name) 
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
                username = excluded.username, 
                first_name = excluded.first_name
        """, (user_id, user.username, user.first_name))
        conn.commit()

    # 2. Retrieve Poll Data
    with db.get_db() as conn:
        poll_data = conn.execute("SELECT * FROM active_polls WHERE poll_id = ?", (poll_id,)).fetchone()

    if not poll_data:
        return

    chat_id = poll_data['chat_id']
    correct_option = poll_data['correct_option_id']
    is_correct = (answer.option_ids[0] == correct_option)

    # 3. Update Scoring
    db.update_user_stats(
        user_id,
        chat_id,
        is_correct,
        username=user.username,
        first_name=user.first_name
    )

    # 4. Compliment System (Existing logic)
    with db.get_db() as conn:
        setting = conn.execute("SELECT compliments_enabled FROM group_settings WHERE chat_id = ?", (chat_id,)).fetchone()
        if setting and setting[0] == 0:
            return

    c_type = "correct" if is_correct else "wrong"
    
    with db.get_db() as conn:
        comp = conn.execute(
            "SELECT text FROM group_compliments WHERE chat_id = ? AND type = ? ORDER BY RANDOM() LIMIT 1",
            (chat_id, c_type)
        ).fetchone()

        if not comp:
            comp = conn.execute(
                "SELECT text FROM compliments WHERE type = ? ORDER BY RANDOM() LIMIT 1",
                (c_type,)
            ).fetchone()

    if comp:
        compliment_text = comp[0]
        mention_name = f"@{user.username}" if user.username else f"*{user.first_name}*"
        final_text = compliment_text.replace("{user}", mention_name)

        if chat_id < 0:
            try:
                await context.bot.send_message(chat_id=chat_id, text=final_text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error sending compliment: {e}")


# ---------------- PERFORMANCE STATS ----------------

async def myscore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides a quick summary of the user's current score."""
    user = update.effective_user
    with db.get_db() as conn:
        s = conn.execute("SELECT * FROM stats WHERE user_id = ?", (user.id,)).fetchone()
    
    if not s:
        return await update.message.reply_text("❌ *No data found.* Participate in a quiz to generate stats!")

    text = (
        "📊 *Your Score Summary*\n\n"
        f"Total Attempted: `{s['attempted']}`\n"
        f"Correct Answers: `{s['correct']}`\n"
        f"Incorrect Answers: `{s['attempted'] - s['correct']}`\n"
        f"Current Score: `{s['score']}`"
    )
    await update.message.reply_text(apply_footer(text))



async def mystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id

    with db.get_db() as conn:
        s = conn.execute("SELECT * FROM stats WHERE user_id = ?", (user.id,)).fetchone()
        
        global_rank = "N/A"
        if s:
            g_rank_row = conn.execute("SELECT COUNT(*) + 1 FROM stats WHERE score > ?", (s['score'],)).fetchone()
            global_rank = g_rank_row[0]

        group_rank = "N/A"
        if update.effective_chat.type != 'private' and s:
            gr_row = conn.execute("SELECT COUNT(*) + 1 FROM group_stats WHERE chat_id = ? AND score > ?",
                                 (chat_id, s['score'])).fetchone()
            group_rank = gr_row[0]

    if not s:
        return await update.message.reply_text("❌ <b>No statistics available!</b>", parse_mode=ParseMode.HTML)

    # 1. Performance Calculations
    accuracy = (s['correct'] / s['attempted'] * 100) if s['attempted'] > 0 else 0

    # 2. NEET marking: +4 for Correct, -1 for Wrong
    wrong = s['attempted'] - s['correct']
    xp = (s['correct'] * 4) - (wrong * 1)
    xp = max(0, xp)  # Prevent negative XP

    # 3. Dynamic Rank Titles (Adjusted for the new XP scale)
    if xp > 500:
        rank_title = "Legendary Surgeon"
    elif xp > 300:
        rank_title = "Chief Resident"
    elif xp > 150:
        rank_title = "Gold Intern"
    elif xp > 50:
        rank_title = "Elite Aspirant"
    else:
        rank_title = "Medical Student"

    # Perfected Alignment (Standardized divider length)
    divider = "────────────────────"

    text = (
        f"🪪 <b>NEETIQ USER PROFILE</b>\n"
        f"{divider}\n"
        f"👤 <b>NAME</b> : <code>{html.escape(user.first_name)}</code>\n"
        f"🏅 <b>STATUS</b> : <code>{rank_title}</code>\n"
        f"{divider}\n"
        f"📊 <b>PERFORMANCE DATA:</b>\n"
        f"<code>╭────────────────────</code>\n"
        f"<code>│ 🏆 Global Rank : #{global_rank}</code>\n"
        f"<code>│ 👥 Group Rank  : #{group_rank}</code>\n"
        f"<code>│ 🧬 Current XP  : {xp:,}</code>\n"
        f"<code>╰────────────────────</code>\n"
        f"📈 <b>ACCURACY TRACKER:</b>\n"
        f"<code>┌── Attempted : {s['attempted']}</code>\n"
        f"<code>├── Correct   : {s['correct']}</code>\n"
        f"<code>└── Precision : {accuracy:.1f}%</code>\n"
        f"{divider}\n"
        f"🔥 <b>STREAK MONITOR:</b>\n"
        f"<code>┌── Current   : {s['current_streak']}</code>\n"
        f"<code>└── Best      : {s['max_streak']}</code>\n"
        f"{divider}"
    )

    final_output = apply_footer(text).strip()
    await update.message.reply_text(final_output, parse_mode=ParseMode.HTML)


# ---------------- LEADERBOARD helper ----------------

def get_badge(rank: int) -> str:
    """Returns medals for top 3, otherwise formatted hash."""
    badges = {1: "🥇", 2: "🥈", 3: "🥉"}
    return badges.get(rank, f"#{rank}")

# ---------------- LEADERBOARD FUNCTIONS ----------------

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = db.get_leaderboard_data(limit=10)
        if not rows:
            return await update.message.reply_text("📭 Leaderboard is empty.")

        text = "<b>🌍 Global Top 10 Performers</b>\n"
        text += "━━━━━━━━━━━━━━━━━━\n\n"

        for i, r in enumerate(rows, 1):
            rank_display = get_badge(i)
            name = html.escape(str(r[0]))
            # Using r[3] for the points as per your original logic
            text += f"{rank_display} {name} -  {r[3]} pts!\n"

        await update.message.reply_text(apply_footer(text), parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text("❌ Error generating leaderboard.")

async def groupleaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        rows = db.get_leaderboard_data(chat_id=chat_id, limit=10)
        title = html.escape(update.effective_chat.title)
        
        text = f"<b>👥 {title.upper()} Top 10</b>\n"
        text += "━━━━━━━━━━━━━━━━━━\n\n"

        if not rows:
            return await update.message.reply_text("📭 No participants yet.")

        for i, r in enumerate(rows, 1):
            rank_display = get_badge(i)
            name = html.escape(str(r[0]))
            text += f"{rank_display} {name} - {r[3]} pts!\n"

        await update.message.reply_text(apply_footer(text), parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text("❌ Error loading group leaderboard.")



# ---------------- ADMIN MANAGEMENT ----------------

async def adminlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists all authorized admins."""
    if not await is_admin(update.effective_user.id): return
    with db.get_db() as conn:
        admins = conn.execute("SELECT user_id FROM admins").fetchall()
    
    text = "👮 *Authorized Admins:*\n\n"
    text += f"• `{OWNER_ID}` (Owner)\n"
    for adm in admins:
        if adm[0] != OWNER_ID:
            text += f"• `{adm[0]}`\n"
    await update.message.reply_text(apply_footer(text))

async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    try:
        new_id = int(context.args[0])
        with db.get_db() as conn:
            conn.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?,?)", 
                         (new_id, str(datetime.now())))
        await update.message.reply_text(f"✅ User `{new_id}` is now an Admin.")
    except:
        await update.message.reply_text("❌ Usage: `/addadmin <user_id>`")

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    try:
        rem_id = int(context.args[0])
        with db.get_db() as conn:
            conn.execute("DELETE FROM admins WHERE user_id = ?", (rem_id,))
        await update.message.reply_text(f"✅ User `{rem_id}` removed from Admin list.")
    except:
        await update.message.reply_text("❌ Usage: `/removeadmin <user_id>`")

# ---------------- QUESTION MANAGEMENT ----------------

async def addquestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Improved version with better whitespace handling and validation."""
    if not await is_admin(update.effective_user.id): return
    
    content = ""
    if update.message.document and update.message.document.file_name.endswith('.txt'):
        file = await context.bot.get_file(update.message.document.file_id)
        content = (await file.download_as_bytearray()).decode('utf-8')
    elif update.message.text:
        content = update.message.text.replace("/addquestion", "").strip()

    if not content:
        return await update.message.reply_text("❌ Please provide text or a .txt file.")

    # Split by double newlines, then remove truly empty blocks
    raw_entries = [e.strip() for e in content.split('\n\n') if e.strip()]
    added_count = 0
    skipped_count = 0

    with db.get_db() as conn:
        for entry in raw_entries:
            # Filter out empty lines within a block (trailing spaces, etc.)
            lines = [l.strip() for l in entry.split('\n') if l.strip()]
            
            if len(lines) >= 7:
                try:
                    explanation = lines[-1]
                    # Normalize answer: trim spaces and make uppercase (e.g., 'a ' -> 'A')
                    correct = str(lines[-2]).strip().upper()
                    opt_d = lines[-3]
                    opt_c = lines[-4]
                    opt_b = lines[-5]
                    opt_a = lines[-6]
                    q_text = "\n".join(lines[:-6]) 

                    # Validation: Ensure 'correct' is a valid option identifier
                    if correct not in ['1', '2', '3', '4', 'A', 'B', 'C', 'D']:
                        skipped_count += 1
                        continue

                    conn.execute(
                        "INSERT INTO questions (question, a, b, c, d, correct, explanation) VALUES (?,?,?,?,?,?,?)",
                        (q_text, opt_a, opt_b, opt_c, opt_d, correct, explanation)
                    )
                    added_count += 1
                except Exception:
                    skipped_count += 1
            else:
                skipped_count += 1

    await update.message.reply_text(apply_footer(f"📊 *Import Summary:*\n✅ Added: `{added_count}`\n⚠️ Skipped: `{skipped_count}`"))

async def questions_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays total questions currently in the bank."""
    if not await is_admin(update.effective_user.id): return
    with db.get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    await update.message.reply_text(f"📘 *Total Questions in Database:* `{total}`")

async def del_all_questions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Unauthorized.")

    try:
        # Correct the name here to match the DB file exactly
        db.delete_all_questions() 
        
        await update.message.reply_text("🗑️ Questions deleted!")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


# ---------------- COMPLIMENT MANAGEMENT ----------------

async def addcompliment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    try:
        c_type = context.args[0].lower() # 'correct' or 'wrong'
        text = " ".join(context.args[1:])
        if c_type not in ['correct', 'wrong']: raise ValueError
        with db.get_db() as conn:
            conn.execute("INSERT INTO compliments (type, text) VALUES (?,?)", (c_type, text))
        await update.message.reply_text(f"✅ Added {c_type} compliment: \"{text}\"")
    except:
        await update.message.reply_text("❌ *Usage:* `/addcompliment correct Well done {user}!`")

async def listcompliments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return

    with db.get_db() as conn:
        rows = conn.execute("SELECT * FROM compliments").fetchall()

    if not rows:
        await update.message.reply_text("📭 No compliments added yet.")
        return

    text = "📝 Compliment List:\n\n"

    for r in rows:
        text += (
            f"ID: {r['id']} | Type: {r['type']}\n"
            f"Text: {r['text']}\n\n"
        )

    chunks = split_message(text)

    for chunk in chunks:
        await update.message.reply_text(chunk)

async def delcompliment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    try:
        cid = int(context.args[0])
        with db.get_db() as conn:
            conn.execute("DELETE FROM compliments WHERE id = ?", (cid,))
        await update.message.reply_text(f"✅ Compliment ID `{cid}` deleted.")
    except:
        await update.message.reply_text("❌ Usage: `/delcompliment <id>`")

async def delallcompliments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deletes all compliments. Restricted to Owner."""
    # Ensure OWNER_ID is defined in your script
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ *Unauthorized:* Only the owner can wipe compliments.")

    try:
        # Call it directly from the imported 'db' module
        db.delete_all_compliments() 
        
        await update.message.reply_text("🗑️ *Compliments Cleared:* The database table is now empty.")
    except Exception as e:
        await update.message.reply_text(f"❌ *Error:* {e}")


# ---------------- BROADCAST ----------------

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    msg_text = " ".join(context.args)
    if not msg_text: return await update.message.reply_text("❌ Usage: `/broadcast <message>`")

    with db.get_db() as conn:
        users = conn.execute("SELECT user_id FROM users").fetchall()
        groups = conn.execute("SELECT chat_id FROM chats").fetchall()

    u_ok, g_ok = 0, 0
    for u in users:
        try:
            await context.bot.send_message(u[0], f"📢 *NEETIQ ANNOUNCEMENT*\n\n{msg_text}")
            u_ok += 1
        except: pass
    for g in groups:
        try:
            await context.bot.send_message(g[0], f"📢 *NEETIQ ANNOUNCEMENT*\n\n{msg_text}")
            g_ok += 1
        except: pass

    await update.message.reply_text(f"✅ *Broadcast Complete:*\nUsers: `{u_ok}`\nGroups: `{g_ok}`")


# ---------------- SETTINGS (FOOTER & AUTOQUIZ) ----------------

async def footer_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manages the message footer status and text."""
    if not await is_admin(update.effective_user.id): return
    args = context.args
    
    if not args:
        return await update.message.reply_text(
            "⚙️ *Footer Settings Help:*\n"
            "• `/footer on` - Enable footer\n"
            "• `/footer off` - Disable footer\n"
            "• `/footer <text>` - Set custom footer text"
        )

    with db.get_db() as conn:
        if args[0].lower() == 'on':
            conn.execute("UPDATE settings SET value='1' WHERE key='footer_enabled'")
            await update.message.reply_text("✅ *Footer is now Enabled.*")
        elif args[0].lower() == 'off':
            conn.execute("UPDATE settings SET value='0' WHERE key='footer_enabled'")
            await update.message.reply_text("❌ *Footer is now Disabled.*")
        else:
            new_text = " ".join(args)
            conn.execute("UPDATE settings SET value=? WHERE key='footer_text'", (new_text,))
            await update.message.reply_text(f"✅ *Footer text updated to:* `{new_text}`")

async def autoquiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manages the automatic quiz scheduler."""
    if not await is_admin(update.effective_user.id): return
    args = context.args
    
    if not args:
        return await update.message.reply_text(
            "⚙️ *AutoQuiz Settings Help:*\n"
            "• `/autoquiz on` - Start automatic quizzes\n"
            "• `/autoquiz off` - Stop automatic quizzes\n"
            "• `/autoquiz interval <min>` - Set time interval (minutes)"
        )

    with db.get_db() as conn:
        if args[0].lower() == 'on':
            conn.execute("UPDATE settings SET value='1' WHERE key='autoquiz_enabled'")
            await update.message.reply_text("✅ *Auto Quiz mode is now ON.*")
        elif args[0].lower() == 'off':
            conn.execute("UPDATE settings SET value='0' WHERE key='autoquiz_enabled'")
            await update.message.reply_text("❌ *Auto Quiz mode is now OFF.*")
        elif args[0].lower() == 'interval' and len(args) > 1:
            try:
                minutes = int(args[1])
                conn.execute("UPDATE settings SET value=? WHERE key='autoquiz_interval'", (str(minutes),))
                await update.message.reply_text(f"✅ *Quiz interval set to {minutes} minutes.*")
            except ValueError:
                await update.message.reply_text("❌ Please provide a valid number for minutes.")

# ---------------- AUTOMATED JOBS ----------------

async def auto_quiz_job(context: ContextTypes.DEFAULT_TYPE):
    """Picks ONE question and sends it to ALL groups simultaneously."""
    with db.get_db() as conn:
        status = conn.execute("SELECT value FROM settings WHERE key='autoquiz_enabled'").fetchone()[0]
        if status == '0': return
        
        # Pick the shared question for this round
        q = conn.execute("SELECT * FROM questions ORDER BY RANDOM() LIMIT 1").fetchone()
        
        if not q:
            return # No questions left to send

        # Get all groups
        chats = conn.execute("SELECT chat_id FROM chats WHERE type != 'private'").fetchall()

    options = [q['a'], q['b'], q['c'], q['d']]
    correct_map = {'1': 0, '2': 1, '3': 2, '4': 3, 'A': 0, 'B': 1, 'C': 2, 'D': 3}
    c_idx = correct_map.get(str(q['correct']).upper(), 0)

    # Send the SAME question to all groups
    for c in chats:
        try:
            msg = await context.bot.send_poll(
                chat_id=c[0],
                question=f"🧠 *NEET MCQ (Global Quiz):*\n\n{q['question']}",
                options=options,
                type=Poll.QUIZ,
                correct_option_id=c_idx,
                explanation=f"📖 *Explanation:*\n{q['explanation']}",
                is_anonymous=False
            )
            # Track each poll so scoring works in every group
            with db.get_db() as conn:
                conn.execute("INSERT INTO active_polls VALUES (?,?,?)", (msg.poll.id, c[0], c_idx))
            
            await asyncio.sleep(0.2) # Small delay to prevent Telegram spam limits
        except Exception:
            continue

    # 3. AFTER sending to everyone, delete it from the database
    with db.get_db() as conn:
        conn.execute("DELETE FROM questions WHERE id = ?", (q['id'],))


async def nightly_leaderboard_job(context: ContextTypes.DEFAULT_TYPE):
    # 1. Global Section
    global_rows = db.get_leaderboard_data(limit=10)
    global_text = "🌍 Global Top 10 Performers\n"
    if not global_rows:
        global_text += "No global data recorded today.\n"
    else:
        for i, r in enumerate(global_rows, 1):
            badge = get_badge(i)
            name = html.escape(str(r[0]))
            global_text += f"{badge} {name} -  {r[3]} pts!\n"

    # 2. Get Groups
    with db.get_db() as conn:
        chats = conn.execute("SELECT chat_id, title FROM chats WHERE type != 'private'").fetchall()

    # 3. Send combined reports
    for c in chats:
        chat_id, chat_title = c[0], html.escape(c[1] if c[1] else "This Group")
        try:
            group_rows = db.get_leaderboard_data(chat_id=chat_id, limit=10)
            group_text = f"👥 {chat_title} Top 10\n"

            if not group_rows:
                group_text += "No participants in this group yet.\n"
            else:
                for i, r in enumerate(group_rows, 1):
                    badge = get_badge(i)
                    name = html.escape(str(r[0]))
                    group_text += f"{badge} {name} - {r[3]} pts!\n"

            final_message = (
                "🌙 <b>DAILY LEADERBOARD</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"<b>{global_text}</b>"
                "━━━━━━━━━━━━━━━━━━\n"
                f"<b>{group_text}</b>"
                "━━━━━━━━━━━━━━━━━━\n"
                "Great effort today, champs! 🚀 Keep the momentum going—more quizzes coming tomorrow!"
            )

            await context.bot.send_message(
                chat_id=chat_id,
                text=apply_footer(final_message),
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(0.5)
        except Exception:
            continue

async def bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides a high-level overview of the bot's reach and data."""
    if not await is_admin(update.effective_user.id): return
    
    with db.get_db() as conn:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_chats = conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0]
        total_questions = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
        total_attempts = conn.execute("SELECT SUM(attempted) FROM stats").fetchone()[0] or 0
        total_admins = conn.execute("SELECT COUNT(*) FROM admins").fetchone()[0] + 1 # +1 for Owner
	
    stats_text = (
        "🤖 *NEETIQ Master Bot Statistics*\n\n"
        f"👤 *Total Users:* `{total_users}`\n"
        f"👥 *Total Groups:* `{total_chats}`\n"
        f"👮 *Total Admins:* `{total_admins}`\n\n"
        "📊 *Database Growth*\n"
        f"❓ *Total Questions:* `{total_questions}`\n"
        f"📝 *Total Global Attempts:* `{total_attempts}`\n"
    )
    
    await update.message.reply_text(apply_footer(stats_text))

async def is_telegram_group_admin(update: Update):
    """Checks if the user is an actual admin of the Telegram Group."""
    member = await update.effective_chat.get_member(update.effective_user.id)
    return member.status in ['creator', 'administrator']

async def toggle_compliments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle compliments ON or OFF."""
    # Check if it's a group and if user is admin
    if update.effective_chat.type == "private":
        return await update.message.reply_text("❌ This command only works in groups.")
        
    if not await is_telegram_group_admin(update):
        return await update.message.reply_text("❌ Only group admins can do this.")

    status_input = context.args[0].lower() if context.args else ""
    if status_input not in ['on', 'off']:
        return await update.message.reply_text("📝 Usage: `/comp_toggle on` or `/comp_toggle off`")

    status = 1 if status_input == 'on' else 0
    chat_id = update.effective_chat.id

    with db.get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO group_settings (chat_id, compliments_enabled) VALUES (?, ?)", (chat_id, status))
    
    await update.message.reply_text(f"✅ Compliments are now {'ON' if status else 'OFF'}")

async def set_group_compliment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set custom compliment text."""
    if not await is_telegram_group_admin(update):
        return await update.message.reply_text("❌ Only group admins can do this.")

    if len(context.args) < 2:
        return await update.message.reply_text("📝 Usage: `/setcomp correct Great job {user}!`")

    c_type = context.args[0].lower()
    c_text = " ".join(context.args[1:])
    chat_id = update.effective_chat.id

    if c_type not in ['correct', 'wrong']:
        return await update.message.reply_text("❌ Type must be 'correct' or 'wrong'.")

    with db.get_db() as conn:
        # Delete old custom ones for this type in this group first to prevent spam
        conn.execute("DELETE FROM group_compliments WHERE chat_id = ? AND type = ?", (chat_id, c_type))
        conn.execute("INSERT INTO group_compliments VALUES (?, ?, ?)", (chat_id, c_type, c_text))
    
    await update.message.reply_text(f"✅ Custom {c_type} message saved!")


# REPLACE with your actual source group ID
SOURCE_GROUP_ID = -1003729584653 

async def mirror_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Enhanced Mirroring: Supports Photos, PDFs, Stickers, Polls, and Formatted Text.
    """
    # 1. Security Check
    if update.effective_chat.id != SOURCE_GROUP_ID:
        return

    # 2. Skip commands
    if update.message.text and update.message.text.startswith('/'):
        return

    # 3. Get targets from DB
    with db.get_db() as conn:
        user_rows = conn.execute("SELECT user_id FROM users").fetchall()
        group_rows = conn.execute("SELECT chat_id FROM chats WHERE chat_id != ?", (SOURCE_GROUP_ID,)).fetchall()
    
    # Track counts for the final message
    user_ids = [row[0] for row in user_rows]
    group_ids = [row[0] for row in group_rows]
    all_targets = set(user_ids + group_ids)

    print(f"📡 Mirroring content to {len(all_targets)} destinations...")

    user_success = 0
    group_success = 0
    removed = 0

    for target_id in all_targets:
        try:
            # copy_message handles PDFs, Images, and Inline buttons automatically
            await context.bot.copy_message(
                chat_id=target_id,
                from_chat_id=SOURCE_GROUP_ID,
                message_id=update.message.message_id
            )
            
            # Count success based on ID type (Groups usually have negative IDs)
            if str(target_id).startswith('-'):
                group_success += 1
            else:
                user_success += 1
                
            await asyncio.sleep(0.05) 

        except (Forbidden, BadRequest) as e:
            error_msg = str(e)
            if "Forbidden" in error_msg or "Chat not found" in error_msg:
                with db.get_db() as conn:
                    conn.execute("DELETE FROM users WHERE user_id = ?", (target_id,))
                    conn.execute("DELETE FROM chats WHERE chat_id = ?", (target_id,))
                removed += 1
        except Exception as e:
            print(f"❌ Mirror Error for {target_id}: {e}")

    # 4. Send the Congratulations Summary to YOU or the Master Group
    summary = (
        "Congratulations 🎉\n"
        f"Sent to : {group_success} grps and {user_success} users"
    )
    await context.bot.send_message(chat_id=SOURCE_GROUP_ID, text=summary)




if __name__ == '__main__':
    db.init_db()
    
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .defaults(defaults)
        .build()
    )


    # --- 2. MIRRORING HANDLER (Place this FIRST) ---
    # Captures: Text, Photos, PDFs/Documents, and Polls from Source Group
    app.add_handler(MessageHandler(
        filters.Chat(SOURCE_GROUP_ID) & 
        (~filters.COMMAND) & 
        (filters.TEXT | filters.PHOTO | filters.Document.ALL | filters.POLL), 
        mirror_messages
    ))

    # --- 3. COMMAND HANDLERS ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("randomquiz", send_random_quiz))
    app.add_handler(CommandHandler("myscore", myscore))
    app.add_handler(CommandHandler("mystats", mystats))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("botstats", bot_stats))
    app.add_handler(CommandHandler("setcomp", set_group_compliment))
    app.add_handler(CommandHandler("comp_toggle", toggle_compliments))
    app.add_handler(CommandHandler("groupleaderboard", groupleaderboard))

    # --- 4. ADMIN HANDLERS ---
    app.add_handler(CommandHandler("addadmin", add_admin))
    app.add_handler(CommandHandler("removeadmin", remove_admin))
    app.add_handler(CommandHandler("adminlist", adminlist))
    app.add_handler(CommandHandler("addquestion", addquestion))
    app.add_handler(CommandHandler("questions", questions_stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("addcompliment", addcompliment))
    app.add_handler(CommandHandler("listcompliments", listcompliments))
    app.add_handler(CommandHandler("delcompliment", delcompliment))
    app.add_handler(CommandHandler("footer", footer_cmd))
    app.add_handler(CommandHandler("autoquiz", autoquiz))
    app.add_handler(CommandHandler("delallquestions", del_all_questions))
    app.add_handler(CommandHandler("delallcompliments", delallcompliments))


 # --- 5. SPECIAL HANDLERS ---
    # This captures PDFs sent to the bot (not mirrored) for adding questions
    app.add_handler(MessageHandler(filters.Document.ALL & ~filters.Chat(SOURCE_GROUP_ID), addquestion))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    # ... Rest of Job Queue and run_polling ...

    # 6. JOB QUEUE SETUP
    jq = app.job_queue

    # Fetch interval from DB
    try:
        with db.get_db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key='autoquiz_interval'").fetchone()
            interval_min = int(row[0]) if row else 30
    except Exception:
        interval_min = 30 

    # Start repeating auto-quiz job
    jq.run_repeating(auto_quiz_job, interval=interval_min * 60, first=20)

    # --- NEW: Timezone-aware Daily Leaderboard Job ---
    # This ensures the job runs at exactly 9 PM IST (Asia/Kolkata)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    jq.run_daily(
        nightly_leaderboard_job,
        time=time(hour=21, minute=1, tzinfo=ist_timezone) 
    )

    print("🚀 NEETIQBot Master is Online!")
    app.run_polling(drop_pending_updates=True)

