import re
import asyncio
import logging
import aiohttp
import signal
import sys
from datetime import datetime
from pyrogram.enums import ParseMode
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    UserAlreadyParticipant,
    InviteHashExpired,
    InviteHashInvalid,
    PeerIdInvalid,
    ChannelPrivate,
    UsernameNotOccupied,
    FloodWait,
    MessageTooLong,
    ChatWriteForbidden,
    UserBannedInChannel
)
from upstash_redis import Redis

# Logging setup
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cc_scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Telegram API credentials
API_ID = "23925218"
API_HASH = "396fd3b1c29a427df8cc6fb54f3d307c"
PHONE_NUMBER = "+918123407093"

# Source and target groups
SOURCE_GROUPS = [
    -1002273285238,
    -1002682944548,
]
TARGET_CHANNELS = [
    -1002783784144,
]

# Upstash Redis credentials (replace with your tokens)
UPSTASH_REDIS_URL = "https://usable-owl-50993.upstash.io"
UPSTASH_REDIS_WRITE_TOKEN = "AscxAAIjcDFlYTA5MmU2MmI0Y2Y0YjMwYjk4YzgyOTQ3NzBhYzFmNHAxMA"
UPSTASH_REDIS_READ_TOKEN = "AscxAAIgcDG1JuewTL-asJBwhXVvzeOvj8U-1rc1PsjKNNnF7rDvcg"

# Corrected Redis client initialization
redis_write = Redis(url=UPSTASH_REDIS_URL, token=UPSTASH_REDIS_WRITE_TOKEN)
redis_read = Redis(url=UPSTASH_REDIS_URL, token=UPSTASH_REDIS_READ_TOKEN)

# Other settings
POLLING_INTERVAL = 3
MESSAGE_BATCH_SIZE = 50
MAX_WORKERS = 50
SEND_DELAY = 2
PROCESS_DELAY = 0.3
BIN_TIMEOUT = 10
MAX_CONCURRENT_CARDS = 25
MAX_PROCESSED_MESSAGES = 5000
RETRY_ATTEMPTS = 3

user = Client(
    "cc_monitor_user",
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER,
    workers=MAX_WORKERS,
    sleep_threshold=60
)

is_running = True
last_processed_message_ids = {}
processed_messages = set()
accessible_channels = []
stats = {
    'messages_processed': 0,
    'cards_found': 0,
    'cards_sent': 0,
    'cards_duplicated': 0,
    'send_failures': 0,
    'errors': 0,
    'start_time': None,
    'bin_lookups_success': 0,
    'bin_lookups_failed': 0
}

bin_cache = {}
card_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CARDS)

async def redis_set(key, value):
    await redis_write.set(key, value)

async def redis_get(key):
    return await redis_read.get(key)

# BIN lookup function
async def get_bin_info_antipublic(bin_number: str):
    if bin_number in bin_cache:
        logger.info(f"✅ BIN {bin_number} found in cache")
        return bin_cache[bin_number]

    url = f"https://bins.antipublic.cc/bins/{bin_number}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        timeout = aiohttp.ClientTimeout(total=BIN_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and isinstance(data, dict):
                        bin_info = {
                            'scheme': data.get('scheme', 'UNKNOWN').upper(),
                            'type': data.get('type', 'UNKNOWN').upper(),
                            'brand': data.get('brand', 'UNKNOWN').upper(),
                            'bank': data.get('bank', 'UNKNOWN BANK'),
                            'country_name': data.get('country_name', 'UNKNOWN'),
                            'country_flag': data.get('country_flag', '🌍'),
                        }
                        bin_cache[bin_number] = bin_info
                        stats['bin_lookups_success'] += 1
                        logger.info(f"✅ BIN {bin_number} lookup successful from antipublic")
                        return bin_info
                else:
                    logger.warning(f"⚠️ BIN lookup returned status {response.status} for {bin_number}")
    except Exception as e:
        logger.warning(f"❌ BIN lookup error for {bin_number}: {e}")

    stats['bin_lookups_failed'] += 1
    logger.warning(f"❌ BIN lookup failed for {bin_number}")
    return None

async def discover_accessible_channels():
    global accessible_channels
    accessible_channels = []

    logger.info("🔍 Discovering accessible channels...")

    try:
        async for dialog in user.get_dialogs():
            if dialog.chat.type in ["channel", "supergroup"]:
                if dialog.chat.id in TARGET_CHANNELS:
                    accessible_channels.append(dialog.chat.id)
    except Exception as e:
        logger.error(f"❌ Error getting dialogs: {e}")

    for channel_id in TARGET_CHANNELS:
        if channel_id not in accessible_channels:
            try:
                chat = await user.get_chat(channel_id)
                accessible_channels.append(channel_id)
            except Exception as e:
                logger.error(f"❌ Cannot access channel {channel_id}: {e}")

    return len(accessible_channels) > 0

async def test_channel_sending():
    if not accessible_channels:
        logger.error("❌ No accessible channels to test")
        return False

    for channel_id in accessible_channels:
        try:
            test_message = f"🔧 CC Scraper connection test - {datetime.now().strftime('%H:%M:%S')}"
            sent_msg = await user.send_message(chat_id=channel_id, text=test_message)
            await asyncio.sleep(3)
            try:
                await user.delete_messages(channel_id, sent_msg.id)
            except:
                pass
        except Exception as e:
            logger.error(f"❌ Cannot send to channel {channel_id}: {e}")
            if channel_id in accessible_channels:
                accessible_channels.remove(channel_id)

    return len(accessible_channels) > 0

async def send_to_target_channels_enhanced(formatted_message, cc_data):
    card_hash = cc_data.split('|')[0]
    cached = await redis_get(f"processed_card:{card_hash}")
    if cached:
        logger.info(f"🔄 DUPLICATE CC DETECTED (cached): {cc_data[:12]}*** - SKIPPING")
        stats['cards_duplicated'] += 1
        return False

    await redis_set(f"processed_card:{card_hash}", "1")

    if not accessible_channels:
        logger.error("❌ No accessible channels available for sending")
        stats['send_failures'] += 1
        return False

    success_count = 0

    for i, channel_id in enumerate(accessible_channels):
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await user.send_message(
                    chat_id=channel_id,
                    text=formatted_message,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
                stats['cards_sent'] += 1
                success_count += 1
                break
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                continue
            except (ChatWriteForbidden, UserBannedInChannel):
                if channel_id in accessible_channels:
                    accessible_channels.remove(channel_id)
                break
            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    stats['send_failures'] += 1
                else:
                    await asyncio.sleep(1)
        if i < len(accessible_channels) - 1:
            await asyncio.sleep(SEND_DELAY)

    return success_count > 0

def extract_credit_cards_enhanced(text):
    if not text:
        return []

    patterns = [
        r'\b(\d{13,19})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})\b',
        r'\b(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})\b',
        r'(\d{13,19})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
        r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
    ]

    credit_cards = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if len(match) == 4:
                card_number, month, year, cvv = match
                card_number = re.sub(r'[\s\-]', '', card_number)
            elif len(match) == 7:
                card1, card2, card3, card4, month, year, cvv = match
                card_number = card1 + card2 + card3 + card4
            else:
                continue

            if not (13 <= len(card_number) <= 19):
                continue

            try:
                month_int = int(month)
                if not (1 <= month_int <= 12):
                    continue
            except ValueError:
                continue

            if len(year) == 4:
                year = year[-2:]
            elif len(year) != 2:
                continue

            if not (3 <= len(cvv) <= 4):
                continue

            credit_cards.append(f"{card_number}|{month.zfill(2)}|{year}|{cvv}")

    return list(dict.fromkeys(credit_cards))

def format_card_message_enhanced(cc_data, bin_info):
    scheme = "UNKNOWN"
    card_type = "UNKNOWN"
    brand = "UNKNOWN"
    bank_name = "UNKNOWN BANK"
    country_name = "UNKNOWN"
    country_emoji = "🌍"
    bin_number = cc_data.split('|')[0][:6]

    if bin_info:
        scheme = bin_info.get('scheme', 'UNKNOWN').upper()
        card_type = bin_info.get('type', 'UNKNOWN').upper()
        brand = bin_info.get('brand', 'UNKNOWN').upper()
        bank_name = bin_info.get('bank', 'UNKNOWN BANK')
        country_name = bin_info.get('country_name', 'UNKNOWN')
        country_emoji = bin_info.get('country_flag', '🌍')
    else:
        scheme = brand = "UNKNOWN"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    message = f"""[ϟ] 𝐀𝐩𝐩𝐫𝐨𝐯𝐞𝐝 𝐒𝐜𝐫𝐚𝐩𝐩𝐞𝐫
━━━━━━━━━━━━━
[ϟ] 𝗖𝗖 - <code>{cc_data}</code> 
[ϟ] 𝗦𝘁𝗮𝘁𝘂𝘀 : APPROVED ✅
[ϟ] 𝗚𝗮𝘁𝗲 - Stripe Auth
━━━━━━━━━━━━━
[ϟ] 𝗕𝗶𝗻 : {bin_number}
[ϟ] 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 : {country_name} {country_emoji}
[ϟ] 𝗜𝘀𝘀𝘂𝗲𝗿 : {bank_name}
[ϟ] 𝗧𝘆𝗽𝗲 : {card_type} - {brand}
━━━━━━━━━━━━━
[ϟ] 𝗧𝗶𝗺𝗲 : {timestamp}
[ϟ] 𝗦𝗰𝗿𝗮𝗽𝗽𝗲𝗱 𝗕𝘆 : @Bunny"""
    return message

async def process_single_card_enhanced(cc_data):
    async with card_semaphore:
        try:
            logger.info(f"🔄 PROCESSING CC: {cc_data[:12]}***")
            bin_number = cc_data.split('|')[0][:6]

            try:
                bin_info = await asyncio.wait_for(get_bin_info_antipublic(bin_number), timeout=BIN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"⏰ BIN lookup timeout for {bin_number}")
                bin_info = None

            formatted_message = format_card_message_enhanced(cc_data, bin_info)
            success = await send_to_target_channels_enhanced(formatted_message, cc_data)

            if success:
                logger.info(f"✅ Successfully processed and sent CC: {cc_data[:12]}***")
            else:
                logger.error(f"❌ Failed to send CC: {cc_data[:12]}***")

            await asyncio.sleep(PROCESS_DELAY)

        except Exception as e:
            logger.error(f"❌ Error processing CC {cc_data}: {e}")
            stats['errors'] += 1

async def process_message_for_ccs_enhanced(message):
    global processed_messages
    try:
        if message.id in processed_messages:
            return

        processed_messages.add(message.id)
        stats['messages_processed'] += 1

        if len(processed_messages) > MAX_PROCESSED_MESSAGES:
            processed_messages = set(list(processed_messages)[-2500:])

        text = message.text or message.caption
        if not text:
            return

        logger.info(f"📝 PROCESSING MESSAGE {message.id}: {text[:50]}...")
        credit_cards = extract_credit_cards_enhanced(text)
        if not credit_cards:
            return

        logger.info(f"🎯 FOUND {len(credit_cards)} CARDS in message {message.id}")
        stats['cards_found'] += len(credit_cards)

        for cc_data in credit_cards:
            await process_single_card_enhanced(cc_data)

    except Exception as e:
        logger.error(f"❌ Error processing message {message.id}: {e}")
        stats['errors'] += 1

async def poll_for_new_messages_enhanced():
    global last_processed_message_ids, is_running
    logger.info("🔄 Starting enhanced polling...")

    for group_id in SOURCE_GROUPS:
        try:
            async for message in user.get_chat_history(group_id, limit=1):
                last_processed_message_ids[group_id] = message.id
                break
        except Exception:
            last_processed_message_ids[group_id] = 0

    while is_running:
        try:
            for group_id in SOURCE_GROUPS:
                await poll_single_group_enhanced(group_id)
                await asyncio.sleep(0.5)

            await asyncio.sleep(POLLING_INTERVAL)
        except Exception as e:
            logger.error(f"❌ Error in polling loop: {e}")
            stats['errors'] += 1
            await asyncio.sleep(5)

async def poll_single_group_enhanced(group_id):
    try:
        last_id = last_processed_message_ids.get(group_id, 0)

        new_messages = []
        async for message in user.get_chat_history(group_id, limit=MESSAGE_BATCH_SIZE):
            if message.id <= last_id:
                break
            new_messages.append(message)

        new_messages.reverse()

        if new_messages:
            for message in new_messages:
                await process_message_for_ccs_enhanced(message)
                last_processed_message_ids[group_id] = max(last_processed_message_ids[group_id], message.id)
                await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"❌ Error polling group {group_id}: {e}")
        stats['errors'] += 1

@user.on_message(filters.chat(SOURCE_GROUPS))
async def realtime_message_handler_enhanced(client, message):
    logger.info(f"⚡ REAL-TIME MESSAGE: {message.id} from group {message.chat.id}")
    asyncio.create_task(process_message_for_ccs_enhanced(message))

async def print_stats_enhanced():
    while is_running:
        await asyncio.sleep(60)
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 CC MONITOR STATS - Uptime: {uptime}")
            logger.info(f"📨 Messages: {stats['messages_processed']}")
            logger.info(f"🎯 Cards Found: {stats['cards_found']}")
            logger.info(f"✅ Cards Sent: {stats['cards_sent']}")
            logger.info(f"🔄 Duplicates: {stats['cards_duplicated']}")
            logger.info(f"❌ Send Failures: {stats['send_failures']}")
            logger.info(f"🔍 BIN Success: {stats['bin_lookups_success']}")
            logger.info(f"💾 Cache Size: {len(bin_cache)}")
            logger.info(f"📢 Active Channels: {len(accessible_channels)}")
            logger.info(f"❌ Total Errors: {stats['errors']}")

def signal_handler(signum, frame):
    global is_running
    logger.info(f"🛑 SHUTDOWN SIGNAL {signum} - Stopping monitor...")
    is_running = False

async def main():
    global is_running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logger.info("🚀 STARTING ENHANCED CC MONITOR...")
        logger.info(f"⚙️ SETTINGS:")
        logger.info(f"   📡 Monitoring {len(SOURCE_GROUPS)} groups: {SOURCE_GROUPS}")
        logger.info(f"   📤 Target channels: {TARGET_CHANNELS}")
        logger.info(f"   ⏱️ Polling interval: {POLLING_INTERVAL}s")
        logger.info(f"   ⏳ Send delay: {SEND_DELAY}s")
        logger.info(f"   🔄 Retry attempts: {RETRY_ATTEMPTS}")

        stats['start_time'] = datetime.now()

        await user.start()
        logger.info("✅ User client started successfully!")
        await asyncio.sleep(2)

        channel_discovery_ok = await discover_accessible_channels()
        if not channel_discovery_ok:
            logger.error("❌ No accessible channels found!")
            return

        channel_test_ok = await test_channel_sending()
        if not channel_test_ok:
            logger.error("❌ Channel sending test failed!")
            return

        logger.info("🚀 Starting background tasks...")
        polling_task = asyncio.create_task(poll_for_new_messages_enhanced())
        stats_task = asyncio.create_task(print_stats_enhanced())

        try:
            logger.info("✅ CC MONITOR FULLY ACTIVE!")
            await idle()
        finally:
            polling_task.cancel()
            stats_task.cancel()
            try:
                await asyncio.gather(polling_task, stats_task, return_exceptions=True)
            except:
                pass

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        stats['errors'] += 1
    finally:
        logger.info("🛑 Stopping client...")
        try:
            if user.is_connected:
                await user.stop()
                logger.info("✅ Client stopped successfully")
        except Exception as e:
            logger.error(f"❌ Error stopping client: {e}")

        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 FINAL STATS:")
            logger.info(f"   ⏱️ Total Uptime: {uptime}")
            logger.info(f"   📨 Messages Processed: {stats['messages_processed']}")
            logger.info(f"   🎯 Cards Found: {stats['cards_found']}")
            logger.info(f"   ✅ Cards Sent: {stats['cards_sent']}")
            logger.info(f"   🔄 Duplicates Blocked: {stats['cards_duplicated']}")
            logger.info(f"   ❌ Send Failures: {stats['send_failures']}")
            logger.info(f"   💾 BINs Cached: {len(bin_cache)}")
            logger.info(f"   ❌ Total Errors: {stats['errors']}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 CC MONITOR STOPPED BY USER")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)
