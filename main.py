import os
import logging
import asyncio
import base64
from aiogram.types import BufferedInputFile
import io # <--- Add this
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import asyncpg

# ### CONFIGURATION ###
# Loading variables from Render Environment
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
DB_URI = os.getenv("DB_URI")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID")) # Storage Channel (Private)
FS_CHANNEL_ID = int(os.getenv("FS_CHANNEL_ID")) # Force Subscribe Channel (Backup)
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")    # Public Channel (for auto-posting)

# ### LOGGING ###
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ### INITIALIZATION ###
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ### DATABASE FUNCTIONS ###
async def init_db():
    """Initializes the database connection and creates the table."""
    conn = await asyncpg.connect(DB_URI)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            caption TEXT
        )
    ''')
    return conn

async def get_file(file_id_db):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow('SELECT file_id, file_type, caption FROM files WHERE id = $1', int(file_id_db))
    await conn.close()
    return row

async def save_file(file_id, file_type, caption):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow(
        'INSERT INTO files (file_id, file_type, caption) VALUES ($1, $2, $3) RETURNING id',
        file_id, file_type, caption
    )
    await conn.close()
    return row['id']

# ### HELPER FUNCTIONS ###

async def is_subscribed(user_id):
    """Checks if the user is a member of the Force Subscribe channel."""
    try:
        member = await bot.get_chat_member(chat_id=FS_CHANNEL_ID, user_id=user_id)
        # If user is left, kicked, or restricted, they are not subscribed
        if member.status in ['left', 'kicked']:
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        # If bot is not admin or channel is invalid, we return True (fail open) 
        # to avoid locking users out due to bugs.
        return True 

def encode_payload(payload):
    return base64.urlsafe_b64encode(str(payload).encode()).decode().strip("=")

def decode_payload(payload):
    padding = '=' * (4 - len(payload) % 4)
    return int(base64.urlsafe_b64decode(payload + padding).decode())

# ### BOT HANDLERS ###

@dp.message(CommandStart())
async def start_handler(message: Message):
    # 1. FORCE SUBSCRIBE CHECK (The Barrier)
    # We check this immediately. If they aren't joined, they pass no further.
    if not await is_subscribed(message.from_user.id):
        try:
            chat = await bot.get_chat(FS_CHANNEL_ID)
            invite_link = chat.invite_link
        except:
            # Fallback if bot cannot fetch link (make sure Bot is Admin in Backup Channel)
            invite_link = "https://t.me/YOUR_BACKUP_CHANNEL_LINK_HERE" 

        # We preserve the payload so they can click "Try Again" and get the video immediately
        args = message.text.split(' ')
        payload = args[1] if len(args) > 1 else "start"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Join Backup Channel", url=invite_link)],
            [InlineKeyboardButton(text="🔄 Try Again", url=f"https://t.me/{ (await bot.get_me()).username }?start={payload}")]
        ])
        
        await message.answer(
            "⚠️ **Access Restricted**\n\nTo use this bot and view the hidden content, you must join our Backup Channel first.", 
            reply_markup=keyboard, 
            parse_mode="Markdown"
        )
        return

    # 2. IF SUBSCRIBED: Process the Deep Link
    args = message.text.split(' ')
    if len(args) > 1:
        payload = args[1]
        try:
            db_id = decode_payload(payload)
            file_data = await get_file(db_id)
            
            if file_data:
                # Send with PROTECT_CONTENT=True (No forward/save/download)
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
                    await message.answer("File type not supported.")
            else:
                await message.answer("❌ **File not found.** It might have been deleted.")
        except Exception as e:
            logger.error(f"Error sending file: {e}")
            await message.answer("❌ Invalid Link or File.")
    else:
        # If they are subscribed but just typed /start (no link)
        await message.answer("👋 **Welcome!**\n\n \nCheck our public channel for more.\n https://t.me/desichudaivideoes")

# ### ADMIN UPLOAD HANDLER (THUMBNAIL FIX) ###
@dp.message(F.video | F.photo)
async def handle_file_upload(message: Message):
    # Only the owner can upload
    if message.from_user.id != OWNER_ID:
        return

    msg = await message.answer("⏳ **Processing...**")
    
    # 1. COPY to Storage Channel
    try:
        await message.copy_to(chat_id=DB_CHANNEL_ID)
    except Exception as e:
        await msg.edit_text(f"❌ Error saving to DB Channel: {e}")
        return

    # 2. Extract File Details
    file_thumb_id = None
    file_id = None
    file_type = None
    caption = message.caption or ""

    if message.video:
        file_id = message.video.file_id
        file_type = 'video'
        # Check if thumbnail exists
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

    # 4. Success Message (Admin)
    await msg.edit_text(f"✅ **File Saved!**\n\n🆔 DB ID: `{db_id}`\n🔗 Link: `{deep_link}`", parse_mode="Markdown")

    # 5. AUTO POST TO PUBLIC CHANNEL
    if LOG_CHANNEL_ID:
        try:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📥 Click to Watch / Download", url=deep_link)]
            ])
            public_caption = f"🎥 **New Video!**\n\n{caption}\n"

            # STRATEGY: Download Thumbnail to Memory -> Upload as New Photo
            if file_thumb_id:
                try:
                    # A. Create a memory buffer
                    file_in_memory = io.BytesIO()
                    
                    # B. Download the thumbnail from Telegram servers
                    await bot.download(file=file_thumb_id, destination=file_in_memory)
                    
                    # C. Reset buffer position to start
                    file_in_memory.seek(0)
                    
                    # D. Upload as a fresh photo
                    # We give it a fake name 'thumb.jpg' so Telegram knows it's an image
                    photo_file = BufferedInputFile(file_in_memory.read(), filename="thumb.jpg")
                    
                    await bot.send_photo(
                        chat_id=int(LOG_CHANNEL_ID),
                        photo=photo_file,
                        caption=public_caption,
                        reply_markup=keyboard
                    )
                except Exception as e:
                    # If download fails, fallback to text
                    logger.error(f"Thumbnail Download Failed: {e}")
                    raise e # Trigger the fallback below
            else:
                # No thumbnail exists at all
                await bot.send_message(
                    chat_id=int(LOG_CHANNEL_ID),
                    text=public_caption,
                    reply_markup=keyboard
                )

        except Exception as e:
            # Final Fallback: Text Only
            try:
                await bot.send_message(
                    chat_id=int(LOG_CHANNEL_ID),
                    text=public_caption,
                    reply_markup=keyboard
                )
            except:
                pass # If even text fails, we give up silently
            
# ### KEEP-ALIVE SERVER (FOR RENDER) ###
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

# ### MAIN ENTRY POINT ###
async def main():
    await start_web_server() # Start web server
    await init_db()          # Connect to DB
    logger.info("Bot is starting...")
    await dp.start_polling(bot) # Start Bot

if __name__ == "__main__":
    asyncio.run(main())
