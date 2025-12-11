import os
import logging
import asyncio
import base64
import io
from random import choice
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, CallbackQuery
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
import asyncpg

# ### CONFIGURATION ###
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
DB_URI = os.getenv("DB_URI")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID"))
FS_CHANNEL_ID = int(os.getenv("FS_CHANNEL_ID"))
FS_CHANNEL_ID_2 = int(os.getenv("FS_CHANNEL_ID_2"))
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID") # <--- FIXED: Added this back

# ### CHANNEL MAP (The Routing System) ###
CHANNEL_MAP = {
    "donut": int(os.getenv("CH_DONUT_ID", 0)),
    "brownie": int(os.getenv("CH_BROWNIE_ID", 0)),
    "eclair": int(os.getenv("CH_ECLAIR_ID", 0)),
    "peachpie": int(os.getenv("CH_PEACH_ID", 0)),
    "creamroll": int(os.getenv("CH_SOFT_ID", 0)), 
    "berry": int(os.getenv("CH_SOFT_ID", 0)),     
    "macaron": int(os.getenv("CH_SOFT_ID", 0)),
    "lavacake": int(os.getenv("CH_BROWNIE_ID", 0)) 
}

# ### METAPHOR DICTIONARY ###
PRODUCTS = {
    "donut": "🍩 Donut",
    "brownie": "🍫 Brownie",
    "eclair": "🧁 Éclair",
    "peachpie": "🍑 Peach Pie",
    "creamroll": "🍥 Cream Roll",
    "berry": "🫐 Berry Mix",
    "macaron": "🍪 Macaron",
    "lavacake": "🔥 Lava Cake"
}

FLAVORS = {
    "desi": "🇮🇳 Desi",
    "asian": "🌏 Asian",
    "western": "👱‍♀️ Western",
    "african": "🌍 African"
}

# ### LOGGING ###
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ### INITIALIZATION ###
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ### DATABASE FUNCTIONS ###
async def init_db():
    conn = await asyncpg.connect(DB_URI)
    
    # 1. Create table if it doesn't exist (Standard)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            caption TEXT,
            product TEXT,
            flavor TEXT,
            views INTEGER DEFAULT 0,
            uploaded_at TIMESTAMP DEFAULT NOW()
        )
    ''')

    # 2. THE FIX: Force add columns for existing tables
    # We use a loop to catch the "DuplicateColumnError" if they already exist
    for col in ['product', 'flavor']:
        try:
            await conn.execute(f'ALTER TABLE files ADD COLUMN {col} TEXT')
        except asyncpg.exceptions.DuplicateColumnError:
            pass # Column already exists, skip it
            
    # 3. Create other tables
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS batches (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            expected_count INTEGER,
            collected_ids INTEGER[],
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    await conn.execute('CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY)')
    return conn

async def save_file(file_id, file_type, caption):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow(
        'INSERT INTO files (file_id, file_type, caption, views) VALUES ($1, $2, $3, 0) RETURNING id',
        file_id, file_type, caption
    )
    await conn.close()
    return row['id']

async def update_file_meta(db_id, product, flavor):
    conn = await asyncpg.connect(DB_URI)
    await conn.execute('UPDATE files SET product=$1, flavor=$2 WHERE id=$3', product, flavor, int(db_id))
    await conn.close()

async def get_file(db_id):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow('SELECT * FROM files WHERE id = $1', int(db_id))
    await conn.close()
    return row

async def get_next_video(product, flavor):
    conn = await asyncpg.connect(DB_URI)
    row = await conn.fetchrow(
        'SELECT * FROM files WHERE product=$1 AND flavor=$2 ORDER BY RANDOM() LIMIT 1', 
        product, flavor
    )
    await conn.close()
    return row

async def add_user(user_id):
    conn = await asyncpg.connect(DB_URI)
    await conn.execute('INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING', user_id)
    await conn.close()

async def is_subscribed(user_id, channel_id):
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        if member.status in ['left', 'kicked']: return False
        return True
    except: return True

# ### UTILS ###
def encode_payload(payload):
    return base64.urlsafe_b64encode(str(payload).encode()).decode().strip("=")

def decode_payload(payload):
    padding = '=' * (4 - len(payload) % 4)
    return int(base64.urlsafe_b64decode(payload + padding).decode())

# ### BOT HANDLERS ###

@dp.message(CommandStart())
async def start_handler(message: Message):
    await add_user(message.from_user.id)
    
    # 1. Force Join Check
    u_id = message.from_user.id
    if not (await is_subscribed(u_id, FS_CHANNEL_ID) and await is_subscribed(u_id, FS_CHANNEL_ID_2)):
        try: link1 = (await bot.get_chat(FS_CHANNEL_ID)).invite_link
        except: link1 = "https://t.me/BACKUP1"
        try: link2 = (await bot.get_chat(FS_CHANNEL_ID_2)).invite_link
        except: link2 = "https://t.me/BACKUP2"
        
        args = message.text.split(' ')
        payload = args[1] if len(args) > 1 else "start"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Join Channel 1", url=link1)],
            [InlineKeyboardButton(text="📢 Join Channel 2", url=link2)],
            [InlineKeyboardButton(text="🔄 Try Again", url=f"https://t.me/{(await bot.get_me()).username}?start={payload}")]
        ])
        await message.answer("⚠️ **Access Restricted**\nJoin both channels to enter the bakery.", reply_markup=kb)
        return

    # 2. Logic Router
    args = message.text.split(' ')
    payload = args[1] if len(args) > 1 else ""

    if payload.startswith("browse_"):
        try:
            _, prod, flav = payload.split("_")
            video = await get_next_video(prod, flav)
            if video:
                await send_video_to_user(message.chat.id, video)
            else:
                await message.answer(f"😕 No {prod}s found in {flav} flavor yet.")
        except:
            await message.answer("❌ Invalid menu option.")
            
    elif payload and payload != "start":
        try:
            db_id = decode_payload(payload)
            video = await get_file(db_id)
            if video:
                await send_video_to_user(message.chat.id, video)
            else:
                await message.answer("❌ Video not found.")
        except:
            await message.answer("❌ Invalid link.")
            
    else:
        # 3. Main Menu (Product Channels)
        # We use standard Invite Links or Usernames here. 
        # Since channels are private, ensure you have the Invite Link ready or let user just click button
        # NOTE: You must replace these placeholders with your actual Invite Links if you want them to work perfectly.
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Yummy Donut 🤤", url="https://t.me/+L4qyqfjkDA0xZTVl")],
            [InlineKeyboardButton(text="Hot Brownies 🍫", url="https://t.me/+aBJN7J7nnV9hMDQ1")],
            [InlineKeyboardButton(text="Creamy Éclairs 🧁", url="https://t.me/+BPU1yousVjI1YTE1")],
            [InlineKeyboardButton(text="Peach Pies 🍑", url="https://t.me/+DBrJZcFMWchjMjc1")],
            [InlineKeyboardButton(text="Softies", url="https://t.me/+TRYCv65PRns1YWQ1")]
        ])
        await message.answer("👋 **Welcome to The Viral Bakery!**\n\nChoose your order SIR!:", reply_markup=kb)

async def send_video_to_user(chat_id, video_data):
    caption = f"{video_data['caption'] or ''}\n\n🍰 **{PRODUCTS.get(video_data['product'], 'Sweet')}** • {FLAVORS.get(video_data['flavor'], 'Special')}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🍩 Next Video", url=f"https://t.me/{(await bot.get_me()).username}?start=browse_{video_data['product']}_{video_data['flavor']}")]
    ])
    if video_data['file_type'] == 'video':
        await bot.send_video(chat_id, video_data['file_id'], caption=caption, protect_content=True, reply_markup=kb)
    else:
        await bot.send_photo(chat_id, video_data['file_id'], caption=caption, protect_content=True, reply_markup=kb)

# ### ADMIN BATCH & UPLOAD LOGIC ###

@dp.message(Command("batch"))
async def start_batch(message: Message):
    if message.from_user.id != OWNER_ID: return
    try:
        count = int(message.text.split()[1])
        conn = await asyncpg.connect(DB_URI)
        await conn.execute('INSERT INTO batches (admin_id, expected_count, collected_ids) VALUES ($1, $2, $3)', OWNER_ID, count, [])
        await conn.close()
        await message.answer(f"👨‍🍳 **Batch Started!**\nUpload {count} videos now.")
    except:
        await message.answer("Usage: /batch 10")

@dp.message(F.video | F.photo)
async def handle_upload(message: Message):
    if message.from_user.id != OWNER_ID: return
    
    msg = await message.answer("⏳ Saving...")
    
    # 1. Copy to Storage DB (Raw Backup)
    try: await message.copy_to(DB_CHANNEL_ID)
    except: pass
    
    # 2. Extract Data
    fid = message.video.file_id if message.video else message.photo[-1].file_id
    ftype = 'video' if message.video else 'photo'
    caption = message.caption or ""
    
    # 3. Save to DB
    db_id = await save_file(fid, ftype, caption)
    
    # 4. Check Batch
    conn = await asyncpg.connect(DB_URI)
    batch = await conn.fetchrow('SELECT * FROM batches WHERE admin_id=$1 ORDER BY id DESC LIMIT 1', OWNER_ID)
    
    if batch and len(batch['collected_ids'] or []) < batch['expected_count']:
        new_list = (batch['collected_ids'] or []) + [db_id]
        await conn.execute('UPDATE batches SET collected_ids=$1 WHERE id=$2', new_list, batch['id'])
        
        if len(new_list) >= batch['expected_count']:
            kb = build_product_kb(batch['id'])
            await message.answer(f"✅ **Batch Collected!**\n\nSelect Product:", reply_markup=kb)
        else:
            await msg.edit_text(f"✅ Saved ({len(new_list)}/{batch['expected_count']})")
    else:
        # Single file upload
        kb = build_product_kb(f"single_{db_id}")
        await msg.edit_text("✅ Saved. Select Product:", reply_markup=kb)
    
    await conn.close()

def build_product_kb(ref):
    buttons = []
    row = []
    for k, v in PRODUCTS.items():
        row.append(InlineKeyboardButton(text=v, callback_data=f"prod_{k}_{ref}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("prod_"))
async def product_selected(callback: CallbackQuery):
    _, prod, ref = callback.data.split("_", 2)
    buttons = []
    for k, v in FLAVORS.items():
        buttons.append([InlineKeyboardButton(text=v, callback_data=f"flav_{prod}_{k}_{ref}")])
    await callback.message.edit_text(f"Selected: **{PRODUCTS[prod]}**\nChoose Flavor:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("flav_"))
async def flavor_selected(callback: CallbackQuery):
    _, prod, flav, ref = callback.data.split("_", 3)
    
    ids_to_update = []
    conn = await asyncpg.connect(DB_URI)
    
    if ref.startswith("single_"):
        ids_to_update = [int(ref.split("_")[1])]
    else:
        batch = await conn.fetchrow('SELECT collected_ids FROM batches WHERE id=$1', int(ref))
        if batch: ids_to_update = batch['collected_ids']
        await conn.execute('DELETE FROM batches WHERE id=$1', int(ref))
    await conn.close()
    
    status_msg = await callback.message.edit_text("🚀 Publishing...")
    bot_username = (await bot.get_me()).username
    target_channel = CHANNEL_MAP.get(prod, int(LOG_CHANNEL_ID or 0)) # Uses LOG_CHANNEL_ID if prod not found
    
    for db_id in ids_to_update:
        await update_file_meta(db_id, prod, flav)
        f_data = await get_file(db_id)
        encoded = encode_payload(db_id)
        deep_link = f"https://t.me/{bot_username}?start={encoded}"
        
        caption_public = f"{f_data['caption']}\n\n{PRODUCTS[prod]} • {FLAVORS[flav]}\n#{prod} #{flav}"
        caption_storage = f"🆔 ID: {db_id}\n📂 {PRODUCTS[prod]}\n🌍 {FLAVORS[flav]}\n📝 {f_data['caption']}"

        # Post to Public Channel (Target)
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔞 Watch in Bot", url=deep_link)]])
            if f_data['file_type'] == 'video':
                await bot.send_video(target_channel, f_data['file_id'], caption=caption_public, reply_markup=kb)
            else:
                await bot.send_photo(target_channel, f_data['file_id'], caption=caption_public, reply_markup=kb)
        except Exception as e:
            logger.error(f"Failed Public Post: {e}")

        # Post to Storage Channel (Archive with Metadata) - FIXED REQUIREMENT
        try:
            if f_data['file_type'] == 'video':
                await bot.send_video(DB_CHANNEL_ID, f_data['file_id'], caption=caption_storage)
            else:
                await bot.send_photo(DB_CHANNEL_ID, f_data['file_id'], caption=caption_storage)
        except Exception as e:
            logger.error(f"Failed Storage Post: {e}")

    await status_msg.edit_text(f"✅ **Published!**\n{len(ids_to_update)} files sent to {PRODUCTS[prod]}.")

# ### SERVER ###
async def handle_ping(request): return web.Response(text="Alive")
async def main():
    app = web.Application(); app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 8080))).start()
    
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__": asyncio.run(main())
