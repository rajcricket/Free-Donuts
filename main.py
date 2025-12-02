import os
import logging
import asyncio
import base64
import io
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
import asyncpg

# ### CONFIGURATION ###
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
DB_URI = os.getenv("DB_URI")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID"))
FS_CHANNEL_ID = int(os.getenv("FS_CHANNEL_ID"))
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")

# ### LOGGING ###
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ### INITIALIZATION ###
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ### DATABASE FUNCTIONS (UPGRADED) ###
async def init_db():
    """Initializes DB and performs auto-migration for new features."""
    conn = await asyncpg.connect(DB_URI)
    
    # 1. Create Files Table (Existing)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            caption TEXT
        )
    ''')

    # 2. Migration: Add 'views' column if it doesn't exist
    try:
        await conn.execute('ALTER TABLE files ADD COLUMN views INTEGER DEFAULT 0')
    except asyncpg.exceptions.DuplicateColumnError:
        pass # Column already exists, safe to ignore

    # 3. Create Users Table (New)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY
        )
    ''')
    
    return conn

async def add_user(user_id):
    """Adds a user to the database. Ignores if already exists."""
    conn = await asyncpg.connect(DB_URI)
    try:
        await conn.execute('INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING', user_id)
    finally:
        await conn.close()

async def get_all_users():
    """Fetches all user IDs for broadcast."""
    conn = await asyncpg.connect(DB_URI)
    try:
        rows = await conn.fetch('SELECT user_id FROM users')
        return [row['user_id'] for row in rows]
    finally:
        await conn.close()

async def delete_user(user_id):
    """Removes a blocked user."""
    conn = await asyncpg.connect(DB_URI)
    try:
        await conn.execute('DELETE FROM users WHERE user_id = $1', user_id)
    finally:
        await conn.close()

async def increment_views(db_id):
    """Increments view count for a file."""
    conn = await asyncpg.connect(DB_URI)
    try:
        await conn.execute('UPDATE files SET views = views + 1 WHERE id = $1', int(db_id))
    finally:
        await conn.close()

async def get_stats_data():
    """Gets total users, total files, and top viewed videos."""
    conn = await asyncpg.connect(DB_URI)
    try:
        total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
        total_files = await conn.fetchval('SELECT COUNT(*) FROM files')
        # Top 5 Viewed
        top_files = await conn.fetch('SELECT caption, views FROM files ORDER BY views DESC LIMIT 5')
        # Last 5 Uploaded
        last_files = await conn.fetch('SELECT caption, views FROM files ORDER BY id DESC LIMIT 5')
        return total_users, total_files, top_files, last_files
    finally:
        await conn.close()

async def get_file(file_id_db):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow('SELECT file_id, file_type, caption, views FROM files WHERE id = $1', int(file_id_db))
    await conn.close()
    return row

async def save_file(file_id, file_type, caption):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow(
        'INSERT INTO files (file_id, file_type, caption, views) VALUES ($1, $2, $3, 0) RETURNING id',
        file_id, file_type, caption
    )
    await conn.close()
    return row['id']

# ### HELPER FUNCTIONS ###

async def is_subscribed(user_id):
    try:
        member = await bot.get_chat_member(chat_id=FS_CHANNEL_ID, user_id=user_id)
        if member.status in ['left', 'kicked']:
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return True 

def encode_payload(payload):
    return base64.urlsafe_b64encode(str(payload).encode()).decode().strip("=")

def decode_payload(payload):
    padding = '=' * (4 - len(payload) % 4)
    return int(base64.urlsafe_b64decode(payload + padding).decode())

# ### BOT HANDLERS ###

@dp.message(CommandStart())
async def start_handler(message: Message):
    # 1. Track User (Add to DB)
    await add_user(message.from_user.id)

    # 2. FORCE SUBSCRIBE CHECK
    if not await is_subscribed(message.from_user.id):
        try:
            chat = await bot.get_chat(FS_CHANNEL_ID)
            invite_link = chat.invite_link
        except:
            invite_link = "https://t.me/YOUR_BACKUP_CHANNEL"

        args = message.text.split(' ')
        payload = args[1] if len(args) > 1 else "start"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Join Backup Channel", url=invite_link)],
            [InlineKeyboardButton(text="🔄 Try Again", url=f"https://t.me/{ (await bot.get_me()).username }?start={payload}")]
        ])
        
        await message.answer(
            "⚠️ **Access Restricted**\n\nTo use this bot, you must join our Backup Channel first.", 
            reply_markup=keyboard, 
            parse_mode="Markdown"
        )
        return

    # 3. Process Deep Link
    args = message.text.split(' ')
    if len(args) > 1:
        payload = args[1]
        try:
            db_id = decode_payload(payload)
            file_data = await get_file(db_id)
            
            if file_data:
                # Increment View Count
                await increment_views(db_id)

                if file_data['file_type'] == 'video':
                    await bot.send_video(
                        chat_id=message.chat.id,
                        video=file_data['file_id'],
                        caption=file_data['caption'],
                        protect_content=True 
                    )
                elif file_data['file_type'] == 'photo':
                    await bot.send_photo(
                        chat_id=message.chat.id,
                        photo=file_data['file_id'],
                        caption=file_data['caption'],
                        protect_content=True
                    )
            else:
                await message.answer("❌ **File not found.**")
        except Exception as e:
            logger.error(f"Error sending file: {e}")
            await message.answer("❌ Invalid Link.")
    else:
        await message.answer("👋 **Welcome!**\nCheck our public channel for more:\nhttps://t.me/desichudaivideoes")

# ### NEW ADMIN COMMANDS ###

@dp.message(Command("stats"))
async def stats_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    msg = await message.answer("📊 **Calculating Stats...**")
    total_users, total_files, top_files, last_files = await get_stats_data()

    # Format Top Files
    top_text = "\n".join([f"• {row['caption'][:20]}.. - {row['views']} views" for row in top_files])
    last_text = "\n".join([f"• {row['caption'][:20]}.. - {row['views']} views" for row in last_files])

    text = (
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"📂 **Total Files:** `{total_files}`\n\n"
        f"🔥 **Top 5 Viewed:**\n{top_text}\n\n"
        f"🆕 **Last 5 Uploads:**\n{last_text}"
    )
    await msg.edit_text(text, parse_mode="Markdown")

@dp.message(Command("broadcast"))
async def broadcast_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    
    if not message.reply_to_message:
        await message.answer("⚠️ Please reply to a message to broadcast it.")
        return

    msg = await message.answer("📢 **Starting Broadcast...**")
    
    users = await get_all_users()
    total = len(users)
    success = 0
    blocked = 0
    deleted = 0
    
    for count, user_id in enumerate(users):
        try:
            # Copy the message (safest way to broadcast)
            await message.reply_to_message.copy_to(chat_id=user_id)
            success += 1
        except TelegramForbiddenError:
            # User blocked the bot
            await delete_user(user_id)
            blocked += 1
        except TelegramRetryAfter as e:
            # We hit a flood limit, sleep for the required time
            await asyncio.sleep(e.retry_after)
            try:
                await message.reply_to_message.copy_to(chat_id=user_id)
                success += 1
            except:
                pass
        except Exception as e:
            # User account deleted or other error
            deleted += 1

        # Anti-Flood Delay: Sleep 1 second every 20 messages
        if count % 20 == 0:
            await asyncio.sleep(1)
            
        # Update progress every 200 users
        if count % 200 == 0:
            await msg.edit_text(f"📢 **Broadcasting...**\nProgress: {count}/{total}")

    await msg.edit_text(
        f"✅ **Broadcast Complete**\n\n"
        f"👥 Total: `{total}`\n"
        f"✅ Success: `{success}`\n"
        f"🚫 Blocked/Deleted: `{blocked + deleted}`"
    )

# ### ADMIN UPLOAD HANDLER (UNCHANGED) ###
@dp.message(F.video | F.photo)
async def handle_file_upload(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    msg = await message.answer("⏳ **Processing...**")
    
    # 1. COPY to Storage
    try:
        await message.copy_to(chat_id=DB_CHANNEL_ID)
    except Exception as e:
        await msg.edit_text(f"❌ Error saving to DB Channel: {e}")
        return

    # 2. Extract Details
    file_thumb_id = None
    file_id = None
    file_type = None
    caption = message.caption or ""

    if message.video:
        file_id = message.video.file_id
        file_type = 'video'
        if message.video.thumbnail:
            file_thumb_id = message.video.thumbnail.file_id  
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = 'photo'
        file_thumb_id = message.photo[-1].file_id
    
    # 3. Save to NeonDB
    try:
        db_id = await save_file(file_id, file_type, caption)
        encoded_link = encode_payload(db_id)
        bot_username = (await bot.get_me()).username
        deep_link = f"https://t.me/{bot_username}?start={encoded_link}"
    except Exception as e:
        await msg.edit_text(f"❌ Database Error: {e}")
        return

    # 4. Success Message
    await msg.edit_text(f"✅ **File Saved!**\n\n🆔 DB ID: `{db_id}`\n🔗 Link: `{deep_link}`", parse_mode="Markdown")

    # 5. AUTO POST
    if LOG_CHANNEL_ID:
        try:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📥 Click to Watch / Download", url=deep_link)]
            ])
            public_caption = f"🎥 **New Video!**\n{caption}\n"

            if file_thumb_id:
                try:
                    # Download/Upload fix
                    file_info = await bot.get_file(file_thumb_id)
                    file_in_memory = io.BytesIO()
                    await bot.download_file(file_info.file_path, file_in_memory)
                    file_in_memory.seek(0)
                    photo_file = BufferedInputFile(file_in_memory.read(), filename="thumb.jpg")
                    
                    await bot.send_photo(chat_id=int(LOG_CHANNEL_ID), photo=photo_file, caption=public_caption, reply_markup=keyboard)
                except Exception as e:
                    logger.error(f"Thumb Error: {e}")
                    await bot.send_message(chat_id=int(LOG_CHANNEL_ID), text=public_caption, reply_markup=keyboard)
            else:
                await bot.send_message(chat_id=int(LOG_CHANNEL_ID), text=public_caption, reply_markup=keyboard)
        except Exception as e:
            try:
                await bot.send_message(chat_id=int(LOG_CHANNEL_ID), text=public_caption, reply_markup=keyboard)
            except:
                pass

# ### KEEP-ALIVE ###
async def handle_ping(request):
    return web.Response(text="I am alive!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

async def main():
    await start_web_server()
    await init_db()
    logger.info("Bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
