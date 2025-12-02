import os
import logging
import asyncio
import base64
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import asyncpg

# ### CONFIGURATION ###
# We load these from the Environment Variables (Render settings)
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
DB_URI = os.getenv("DB_URI")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID")) # Storage Channel
FS_CHANNEL_ID = int(os.getenv("FS_CHANNEL_ID")) # Force Subscribe Channel
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID") # Public Post Channel (Optional)

# ### LOGGING ###
# This helps you see errors in the Render logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ### INITIALIZATION ###
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ### DATABASE FUNCTIONS ###
async def init_db():
    """Initializes the database connection and creates the table if it doesn't exist."""
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
    # We allow the database to generate the unique ID (Serial)
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
        if member.status in ['left', 'kicked']:
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        # If bot isn't admin or can't check, we default to True to avoid breaking flow
        return True 

def encode_payload(payload):
    """Encodes the DB ID into a URL-safe string."""
    return base64.urlsafe_b64encode(str(payload).encode()).decode().strip("=")

def decode_payload(payload):
    """Decodes the URL-safe string back to a DB ID."""
    padding = '=' * (4 - len(payload) % 4)
    return int(base64.urlsafe_b64decode(payload + padding).decode())

# ### BOT HANDLERS ###

@dp.message(CommandStart())
async def start_handler(message: Message):
    # 1. Extract the payload (the weird code after /start)
    args = message.text.split(' ')
    if len(args) > 1:
        payload = args[1]
        
        # 2. FORCE SUBSCRIBE CHECK
        if not await is_subscribed(message.from_user.id):
            # Create Invite Link Button
            try:
                chat = await bot.get_chat(FS_CHANNEL_ID)
                invite_link = chat.invite_link
            except:
                invite_link = "https://t.me/YOUR_CHANNEL" # Fallback

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Join Backup Channel", url=invite_link)],
                [InlineKeyboardButton(text="🔄 Try Again", url=f"https://t.me/{ (await bot.get_me()).username }?start={payload}")]
            ])
            await message.answer("⚠️ **You must join our backup channel to view this video.**\n\nClick the button below to join, then click 'Try Again'.", reply_markup=keyboard, parse_mode="Markdown")
            return

        # 3. RETRIEVE AND SEND FILE
        try:
            db_id = decode_payload(payload)
            file_data = await get_file(db_id)
            
            if file_data:
                # Send with PROTECT_CONTENT=True (No forward/save)
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
            await message.answer("❌ Invalid Link.")
    else:
        await message.answer("👋 **Welcome!**\nI am a File Storage Bot. Send me a file to save it (Admin Only).")

# ### ADMIN UPLOAD HANDLER (UPDATED FOR THUMBNAILS) ###
@dp.message(F.video | F.photo)
async def handle_file_upload(message: Message):
    # Only the owner can upload
    if message.from_user.id != OWNER_ID:
        return

    msg = await message.answer("⏳ **Processing...**")
    
    # 1. Forward to Storage Channel (Backup)
    try:
        forwarded_msg = await message.forward(DB_CHANNEL_ID)
    except Exception as e:
        await msg.edit_text(f"❌ Error forwarding to DB Channel: {e}")
        return

    # 2. Get File ID, Type, and Thumbnail
    file_thumb = None
    if message.video:
        file_id = forwarded_msg.video.file_id
        file_type = 'video'
        caption = message.caption or ""
        # Try to get the video thumbnail
        if message.video.thumbnail:
            file_thumb = message.video.thumbnail.file_id
    elif message.photo:
        file_id = forwarded_msg.photo[-1].file_id # Get highest quality
        file_type = 'photo'
        caption = message.caption or ""
        file_thumb = forwarded_msg.photo[-1].file_id
    
    # 3. Save to NeonDB
    try:
        db_id = await save_file(file_id, file_type, caption)
        encoded_link = encode_payload(db_id)
        bot_username = (await bot.get_me()).username
        deep_link = f"https://t.me/{bot_username}?start={encoded_link}"
    except Exception as e:
        await msg.edit_text(f"❌ Database Error: {e}")
        return

    # 4. Success Message to Admin
    await msg.edit_text(f"✅ **File Saved!**\n\n🆔 DB ID: `{db_id}`\n🔗 Link: `{deep_link}`", parse_mode="Markdown")

    # 5. AUTO POST TO PUBLIC CHANNEL (Now with Thumbnail)
    if LOG_CHANNEL_ID:
        try:
            # Create the button
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📥 Click to Watch / Download", url=deep_link)]
            ])
            
            # Text for the post
            public_caption = f"🎥 **New Video!**\n\n{caption}\n\n👇 Click below to watch:"

            if file_thumb:
                # Send Photo + Caption + Button
                await bot.send_photo(
                    chat_id=int(LOG_CHANNEL_ID),
                    photo=file_thumb,
                    caption=public_caption,
                    reply_markup=keyboard
                )
            else:
                # Fallback to text if no thumbnail found
                await bot.send_message(
                    chat_id=int(LOG_CHANNEL_ID),
                    text=public_caption,
                    reply_markup=keyboard
                )
        except Exception as e:
            await message.answer(f"⚠️ Could not post to Public Channel: {e}")

# ### KEEP-ALIVE SERVER (FOR RENDER) ###
async def handle_ping(request):
    return web.Response(text="I am alive!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    # Render provides the PORT env var. Default to 8080 if not found.
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

# ### MAIN ENTRY POINT ###
async def main():
    # Start the Web Server (Background Task)
    await start_web_server()
    
    # Init DB
    await init_db()
    
    # Start Bot
    logger.info("Bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
