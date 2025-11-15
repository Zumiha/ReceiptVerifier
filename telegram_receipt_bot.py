"""
Telegram Bot for Russian Receipt Verification

Features:
- QR code string parsing
- Manual parameter input via interactive conversation
- Receipt export to CSV with persistent storage
- File existence check before API calls
- Concurrent request handling with asyncio
"""

import asyncio
import logging
import os
from typing import Dict, Optional, List, Callable
from pathlib import Path
import glob
import json
import hashlib
from datetime import datetime, timedelta
from functools import wraps

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# Import receipt verification logic
from receipt_verifier import ReceiptVerifier, RequestBuilder, Receipt, RequestParams

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
ASKING_FN, ASKING_FD, ASKING_FP, ASKING_T, ASKING_N, ASKING_S = range(6)

# API rate limiting
MAX_DAILY_REQUESTS = 15

# Standard main menu keyboard - reused everywhere
MAIN_MENU_KEYBOARD = [
    ['📱 Распознать из QR строки'],
    ['⌨️ Ввод параметров вручную'],
    ['📊 Статистика', '❓ Help']
]

def get_main_menu_markup() -> ReplyKeyboardMarkup:
    """Get standard main menu keyboard markup."""
    return ReplyKeyboardMarkup(MAIN_MENU_KEYBOARD, resize_keyboard=True)

class AuthManager:
    """
    Manage authorized users with JSON file persistence.
    
    File format: authorized_users.json
    {
        "authorized_users": [123456789, 987654321],
        "admin_contact": "@YourAdminUsername"
    }
    """
    
    def __init__(self, auth_file: str = "authorized_users.json"):
        self.auth_file = auth_file
        self.authorized_users: List[int] = []
        self.admin_contact: str = ""
        self._load_authorized_users()
        
    def _load_authorized_users(self):
        if not os.path.exists(self.auth_file):
            logger.warning(f"{self.auth_file} not found, creating default file")
            default_data = {
                "authorized_users": [],
                "admin_contact": "@admin"
            }
            with open(self.auth_file, 'w') as f:
                json.dump(default_data, f, indent=2)
            self.authorized_users = []
            self.admin_contact = "@admin"
            return
            
        try:
            with open(self.auth_file, 'r') as f:
                data = json.load(f)
                self.authorized_users = data.get('authorized_users', [])
                self.admin_contact = data.get('admin_contact', '@admin')
                logger.info(f"Loaded {len(self.authorized_users)} authorized users")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error loading {self.auth_file}: {e}")
            self.authorized_users = []
            self.admin_contact = "@admin"
            
    def is_authorized(self, user_id: int) -> bool:
        """Check if user is authorized."""
        return user_id in self.authorized_users
        
    def get_admin_contact(self) -> str:
        """Get admin contact for unauthorized message."""
        return self.admin_contact
        
    def reload(self):
        """Reload authorized users from file."""
        self._load_authorized_users()


def require_auth(func: Callable):
    """Decorator to restrict handler access to authorized users only."""
    @wraps(func)
    async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
            
        if not self.auth_manager.is_authorized(user.id):
            logger.warning(f"Unauthorized access attempt by user {user.id} (@{user.username})")
            await update.message.reply_text(
                "🔒 *Доступ запрещен*\n\n"
                f"У вас нет прав доступа к этому боту.\n"
                f"Свяжитесь с администратором {self.auth_manager.get_admin_contact()} для доступа.\n\n"
                f"Ваш ID: `{user.id}`",
                parse_mode='Markdown'
            )
            return
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper


class APIRateLimiter:
    """
    Track daily API request count with persistent JSON storage.
    
    Uses simple date-based tracking:
    - Stores requests as {"date": "YYYY-MM-DD", "count": N}
    - Resets counter when date changes
    - Thread-safe for single-process deployment
    
    File format: api_requests.json
    """
    
    def __init__(self, limit: int = MAX_DAILY_REQUESTS, tracking_file: str = "api_requests.json"):
        self.limit = limit
        self.tracking_file = tracking_file
        self._ensure_file_exists()
        
    def _ensure_file_exists(self):
        """Create tracking file if it doesn't exist."""
        if not os.path.exists(self.tracking_file):
            self._write_data({"date": datetime.now().strftime("%Y-%m-%d"), "count": 0})
            
    def _read_data(self) -> Dict:
        """Read tracking data from JSON file."""
        try:
            with open(self.tracking_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            # Corrupted or missing file - reset
            default_data = {"date": datetime.now().strftime("%Y-%m-%d"), "count": 0}
            self._write_data(default_data)
            return default_data
            
    def _write_data(self, data: Dict):
        """Write tracking data to JSON file."""
        with open(self.tracking_file, 'w') as f:
            json.dump(data, f, indent=2)
            
    def _reset_if_new_day(self, data: Dict) -> Dict:
        """Reset counter if date has changed."""
        today = datetime.now().strftime("%Y-%m-%d")
        if data.get("date") != today:
            data = {"date": today, "count": 0}
            self._write_data(data)
        return data
        
    def can_make_request(self) -> bool:
        """Check if request is allowed under daily limit."""
        data = self._read_data()
        data = self._reset_if_new_day(data)
        return data["count"] < self.limit
        
    def increment(self):
        """Increment request counter."""
        data = self._read_data()
        data = self._reset_if_new_day(data)
        data["count"] += 1
        self._write_data(data)
        logger.info(f"API request count: {data['count']}/{self.limit}")
        
    def get_remaining(self) -> int:
        """Get remaining requests for today."""
        data = self._read_data()
        data = self._reset_if_new_day(data)
        return max(0, self.limit - data["count"])
        
    def get_stats(self) -> Dict:
        """Get current statistics."""
        data = self._read_data()
        data = self._reset_if_new_day(data)
        return {
            "date": data["date"],
            "used": data["count"],
            "limit": self.limit,
            "remaining": max(0, self.limit - data["count"])
        }


class ReceiptBot:
    """
    Main bot controller with file-based receipt caching.
    
    Architecture:
    - Filesystem-based duplicate detection
    - Standardized CSV naming: YYYY-MM-DD_HH-MM-SS_fn_fd_fp.csv
    """
    
    def __init__(self, telegram_token: str, receipt_token: str, receipts_dir: str = "receipts"):
        self.telegram_token = telegram_token
        self.receipts_dir = receipts_dir
        
        # Create receipts directory
        Path(self.receipts_dir).mkdir(exist_ok=True)

        # Initialize authorization manager
        self.auth_manager = AuthManager()

        # Initialize rate limiter
        self.rate_limiter = APIRateLimiter(limit=MAX_DAILY_REQUESTS)
        
        # Receipt verifier with LRU cache and exponential backoff
        self.verifier = ReceiptVerifier(
            token=receipt_token,
            max_retries=3,
            cache_size=500
        )

        # Store QR strings temporarily for reverify callbacks (hash -> qr_string)
        self.qr_cache: Dict[str, str] = {}

        self.app: Optional[Application] = None
        
    def _find_existing_receipt(self, params: Dict[str, str]) -> Optional[str]:
        """
        Search for existing receipt CSV by fiscal parameters.
        
        Uses glob pattern: *_<fn>_<fd>_<fp>.csv
        Time Complexity: O(n) where n = number of files
        
        Args:
            params: Dict with 'fn', 'fd', 'fp' keys
            
        Returns:
            Path to existing CSV file or None
        """
        fn = params.get('fn', '')
        fd = params.get('fd', '')
        fp = params.get('fp', '')
        
        if not (fn and fd and fp):
            return None
            
        pattern = os.path.join(self.receipts_dir, f"*_{fn}_{fd}_{fp}.csv")
        matches = glob.glob(pattern)
        
        return matches[0] if matches else None
        
    def _extract_params_from_qr(self, qr_string: str) -> Dict[str, str]:
        """
        Extract fiscal parameters from QR string.
        
        Args:
            qr_string: QR string like t=...&fn=...&i=...&fp=...
            
        Returns:
            Dict with fn, fd, fp keys
        """
        params = {}
        for pair in qr_string.split('&'):
            if '=' in pair:
                key, value = pair.split('=', 1)
                if key in ['fn', 'fd', 'fp', 'i']:
                    params[key] = value
        
        # API uses 'i' for fiscal document number
        if 'i' in params:
            params['fd'] = params['i']
            
        return params
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - show main menu (no auth required)."""
        user = update.effective_user
        
        # Check authorization
        if not self.auth_manager.is_authorized(user.id):
            await update.message.reply_text(
                "🔒 *Доступ запрещен*\n\n"
                f"У вас нет прав доступа к этому боту.\n"
                f"Свяжитесь с администратором {self.auth_manager.get_admin_contact()} для доступа.\n\n"
                f"Ваш ID: `{user.id}`",
                parse_mode='Markdown'
            )
            return
        
        welcome_msg = (
            "🧾 *Бот для проверки российских чеков*\n\n"
            "Я могу проверить российские фискальные чеки из:\n"
            "• QR строки (t=...&s=...&fn=...)\n"
            "• Фискальным параметрам\n\n"
            "Выберите один из вариантов ниже:"
        )
        await update.message.reply_text(
            welcome_msg,             
            reply_markup=get_main_menu_markup(), 
            parse_mode='Markdown'
        )
    
    @require_auth    
    async def menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /menu command - explicit return to main menu."""
        await update.message.reply_text(
            "📋 *Главное меню*\n\nВыберите действие:",
            reply_markup=get_main_menu_markup(),
            parse_mode='Markdown'
        )

    @require_auth    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command - show usage instructions."""
        help_text = (
            "🔍 *Как проверить квитанции:*\n\n"
            "*1. Метод QR-строки:*\n"
            "Отправьте QR-строку, например:\n"
            "`t=20251028T1524&s=1008.00&fn=7384440900730779&i=1145&fp=3909409245&n=1`\n\n"
            "*2. Ввод параметров вручную:*\n"
            "Нажмите «Ввод параметров вручную» и следуйте инструкциям.\n\n"
            "*Команды:*\n"
            "/start — Главное меню\n"
            "/menu — Вернуться в меню\n"
            "/stats — Статистика кэша\n"
            "/cancel — Отмена операции\n"
            "/help — Это сообщение"
        )
        await update.message.reply_text(
            help_text, 
            parse_mode='Markdown',
            reply_markup=get_main_menu_markup()
        )

    @require_auth    
    async def menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /menu command - explicit return to main menu."""
        await update.message.reply_text(
            "📋 *Главное меню*\n\nВыберите действие:",
            parse_mode='Markdown',
            reply_markup=get_main_menu_markup()
        )

    @require_auth    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /stats command."""
        cache_stats = self.verifier.get_cache_stats()
        api_stats = self.rate_limiter.get_stats()
        
        stats_msg = (
            f"📊 *Статистика*\n\n"
            f"*Кэширование:*\n"
            f"Hits: {cache_stats['hits']}\n"
            f"Misses: {cache_stats['misses']}\n"
            f"Hit Rate: {cache_stats['hit_rate']}\n"
            f"Cached: {cache_stats['size']}\n\n"
            f"*API использование (сегодня):*\n"
            f"Использовано: {api_stats['used']}/{api_stats['limit']}\n"
            f"Осталось: {api_stats['remaining']}\n"
            f"Дата: {api_stats['date']}"
        )
        await update.message.reply_text(
            stats_msg, 
            parse_mode='Markdown',
            reply_markup=get_main_menu_markup()
        )

    @require_auth    
    async def handle_qr_string(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Handle QR string input with file existence check and rate limiting.
        
        Flow:
        1. Validate QR format
        2. Extract fiscal params
        3. Check for existing CSV file
        4. If found: offer download/re-verify
        5. If not found: check rate limit, then call API
        """
        qr_string = update.message.text.strip()
        
        # Validate QR format
        if not all(param in qr_string for param in ['t=', 's=', 'fn=']):
            await update.message.reply_text(
                "❌ Неправильный форма QR строки. Ожидается:\n"
                "`t=...&s=...&fn=...&i=...&fp=...&n=1`",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
            return
        
        # Extract parameters for file lookup
        params = self._extract_params_from_qr(qr_string)
        
        # Check if receipt already exists
        existing_file = self._find_existing_receipt(params)
        
        if existing_file:
            # Found existing receipt - offer choices
            filename = os.path.basename(existing_file)
            
            # Check if filename is too long for callback_data (Telegram 64-byte limit)
            download_data = f"download_{filename}"
            logger.info(f"Download callback_data length: {len(download_data.encode('utf-8'))} bytes: {download_data}")
            
            if len(download_data.encode('utf-8')) > 64:
                file_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
                self.qr_cache[file_hash] = filename
                download_data = f"download_{file_hash}"
                logger.info(f"Using hash instead: {download_data}")
            
            # Use hash for QR string to stay under 64-byte callback_data limit
            qr_hash = hashlib.md5(qr_string.encode()).hexdigest()[:8]
            self.qr_cache[qr_hash] = qr_string
            reverify_data = f"reverify_qr_{qr_hash}"
            logger.info(f"Reverify callback_data length: {len(reverify_data.encode('utf-8'))} bytes: {reverify_data}")

            keyboard = [
                [InlineKeyboardButton("📥 Загрузить сохраненный", callback_data=download_data)],
                [InlineKeyboardButton("🔄 Повторный запрос", callback_data=reverify_data)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"✅ *Чек уже сохранен*\n\n"
                f"Файл: `{os.path.basename(existing_file)}`\n\n"
                f"Выберите действие:",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            return
        
        # Check rate limit before API call
        if not self.rate_limiter.can_make_request():
            remaining = self.rate_limiter.get_remaining()
            await update.message.reply_text(
                f"⚠️ *Дневной лимит запросов достигнут*\n\n"
                f"Не более {MAX_DAILY_REQUESTS} запросовв день.\n"
                f"Осталось сегодня: {remaining}\n\n"
                f"Попробуйте завтра или загрузите имеющиеся чеки.",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
            return
            
        # No existing file and under limit - proceed with API verification
        await update.message.reply_text("🔄 Отправляется запрос через API...")
        await update.message.chat.send_action('typing')
        
        try:
            request = RequestBuilder.from_qr_string(qr_string)
            receipt = await asyncio.to_thread(self.verifier.verify_receipt, request)
            
            # Only increment counter if API call was successful
            if receipt.is_valid:
                self.rate_limiter.increment()
                
            await self._send_receipt_result(update, receipt)
            
        except Exception as e:
            logger.error(f"Неудачная попытка верификации: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Ошибка: {str(e)}",
                reply_markup=get_main_menu_markup()
            )

    @require_auth        
    async def manual_entry_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start manual parameter entry conversation."""
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "📝 *Ввод параметров вручную*\n\n"
            "Шаг 1/6: Введите *номер Фискального Накопителя* (ФН)\n"
            "Пример: `7384440900730779`\n\n"
            "Введите /cancel для отмены.",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )
        return ASKING_FN
        
    async def ask_fn(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Store FN and ask for FD."""
        text = update.message.text.strip()

        # Check for cancel button
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        if not text.isdigit():
            await update.message.reply_text("❌ ФН должен быть циферным. Попробуйте снова:")
            return ASKING_FN
            
        context.user_data['fn'] = text
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "✅ ФН сохранен\n\n"
            "Шаг 2/6: Введите *номер Фискального Документа* (ФД)\n"
            "Пример: `1145`",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )
        return ASKING_FD
        
    async def ask_fd(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Store FD and ask for FP."""
        text = update.message.text.strip()

        # Check for cancel button
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        if not text.isdigit():
            await update.message.reply_text("❌ ФД должен быть циферным. Попробуйте снова:")
            return ASKING_FD
            
        context.user_data['fd'] = text
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "✅ ФД сохранен\n\n"
            "Шаг 3/6: Введите *Фискальный признак документа* (ФП)\n"
            "Пример: `3909409245`",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )
        return ASKING_FP
        
    async def ask_fp(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Store FP and ask for datetime."""
        text = update.message.text.strip()
        
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        if not text.isdigit():
            await update.message.reply_text("❌ ФП должен быть циферным. Попробуйте снова:")
            return ASKING_FP
            
        context.user_data['fp'] = text
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "✅ ФП сохранен\n\n"
            "Шаг 4/6: Введите *Дату и время*\n"
            "Формат: `ГГГГММДДТЧЧмм`\n"
            "Пример: `20250101T1212`",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )
        return ASKING_T
        
    async def ask_t(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Store datetime and ask for operation type."""
        text = update.message.text.strip()
        
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        if len(text) != 13 or 'T' not in text:
            await update.message.reply_text(
                "❌ Неправильный формат. Используйте ГГГГММДДТЧЧмм\n"
                "Пример: `20250101T1212`",
                parse_mode='Markdown'
            )
            return ASKING_T
            
        context.user_data['t'] = text
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "✅ Дата и время сохранены\n\n"
            "Шаг 5/6: Введите *Вид чека* (n)\n"
            "• 1 - Приход\n"
            "• 2 - Возврат прихода\n"
            "• 3 - Расход\n"
            "• 4 - Возврат расхода",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return ASKING_N
        
    async def ask_n(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Store operation type and ask for sum."""
        text = update.message.text.strip()
        
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        if text not in ['1', '2', '3', '4']:
            await update.message.reply_text("❌ Должен быть 1, 2, 3, или 4. Попробуйте снова:")
            return ASKING_N
            
        context.user_data['n'] = text
        keyboard = [['❌ Отмена и возврат в меню']]
        await update.message.reply_text(
            "✅ Вид чека сохранен\n\n"
            "Шаг 6/6: Введите *Итог* в рублях\n"
            "Пример: `1000.00` or `1000`",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return ASKING_S
        
    async def ask_s(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Final step: collect sum, check for existing file, verify if needed."""
        text = update.message.text.strip()
        
        if text == '❌ Отмена и возврат в меню':
            return await self.cancel(update, context)
        
        try:
            float(text)
        except ValueError:
            await update.message.reply_text("❌ Итог должен быть в цифрах. Попробуйте снова:")
            return ASKING_S
            
        context.user_data['s'] = text
        
        # Build params for file lookup
        params = {
            'fn': context.user_data['fn'],
            'fd': context.user_data['fd'],
            'fp': context.user_data['fp']
        }
        
        # Check if receipt already exists
        existing_file = self._find_existing_receipt(params)
    
        if existing_file:
            # Found existing receipt - offer choices
            filename = os.path.basename(existing_file)
            download_data = f"download_{filename}"
            
            # Check if filename is too long for callback_data
            download_data = f"download_{filename}"
            if len(download_data.encode('utf-8')) > 64:
                file_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
                self.qr_cache[file_hash] = filename
                download_data = f"download_{file_hash}"
            
            manual_params = f"{params['fn']}_{params['fd']}_{params['fp']}_{context.user_data['t']}_{context.user_data['n']}_{text}"
            keyboard = [
                [InlineKeyboardButton("📥 Загрузить сохраненный", callback_data=download_data)],
                [InlineKeyboardButton("🔄 Повторный запрос", callback_data=f"reverify_manual_{manual_params}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"✅ *Чек уже сохранен*\n\n"
                f"Файл: `{os.path.basename(existing_file)}`\n\n"
                f"Выберите действие:",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            context.user_data.clear()
            return ConversationHandler.END
        
        # Check rate limit before API call
        if not self.rate_limiter.can_make_request():
            remaining = self.rate_limiter.get_remaining()
            await update.message.reply_text(
                f"⚠️ *Дневной лимит запросов достигнут*\n\n"
                f"Не более {MAX_DAILY_REQUESTS} запросовв день.\n"
                f"Осталось сегодня: {remaining}\n\n"
                f"Попробуйте завтра или загрузите имеющиеся чеки.",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
            context.user_data.clear()
            return ConversationHandler.END
        
        # No existing file and under limit - proceed with API verification
        await update.message.reply_text("🔄 Отправляется запрос через API...")
        await update.message.chat.send_action('typing')

        try:
            params_dict = {
                'fn': context.user_data['fn'],
                'fd': context.user_data['fd'],
                'fp': context.user_data['fp'],
                't': context.user_data['t'],
                'n': context.user_data['n'],
                's': context.user_data['s'],
                'qr': '0'
            }
            
            params_obj = RequestBuilder.from_manual_params(params_dict)
            
            receipt = await asyncio.to_thread(self.verifier.verify_receipt, params_obj)
            
            # Only increment counter if API call was successful
            if receipt.is_valid:
                self.rate_limiter.increment()
                
            await self._send_receipt_result(update, receipt)
            
        except Exception as e:
            logger.error(f"Запрос параметров вручную не произведен: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Ошибка: {str(e)}",
                reply_markup=get_main_menu_markup()
            )
            
        context.user_data.clear()
        return ConversationHandler.END
        
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancel operation and return to main menu."""
        context.user_data.clear()
        await update.message.reply_text(
            "❌ Операция отменена.",
            reply_markup=get_main_menu_markup()
        )
        return ConversationHandler.END
        
    async def _send_receipt_result(self, update: Update, receipt: Receipt) -> None:
        """
        Format and send receipt result with auto-save to disk.
        Generates CSV with format: YYYY-MM-DD_HH-MM-SS_fn_fd_fp.csv
        """
        if not receipt.is_valid:
            await update.message.reply_text(
                f"❌ *Верефикация не прошла*\n\n"
                f"Ошибка: {receipt.error_message}",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
            return
        
        # Save CSV to disk immediately
        csv_filename = receipt.to_csv(receipts_dir=self.receipts_dir)
        
        if not csv_filename:
            logger.error("Неудалось сохранить CSV файл")
            await update.message.reply_text(
                "❌ Неудалось сохранить данные чека",
                reply_markup=get_main_menu_markup()
            )
            return
            
        # Format receipt info
        result_msg = (
            f"✅ *Чек проверен*\n\n"
            f"🏪 {receipt.organization}\n"
            f"📍 {receipt.address}\n"
            f"🆔 ИНН: `{receipt.inn}`\n\n"
            f"📅 Дата: {receipt.date} {receipt.time}\n"
            f"💰 Итого: *{receipt.total_sum:.2f} ₽*\n"
            f"💵 Наличные: {receipt.cash_sum:.2f} ₽\n"
            f"💳 Картой: {receipt.card_sum:.2f} ₽\n\n"
        )
        
        # Add items (limit to 10)
        if receipt.items:
            result_msg += "*Покупки:*\n"
            for i, item in enumerate(receipt.items[:10], 1):
                result_msg += f"{i}. {item.name} - {item.price:.2f} ₽ × {item.quantity}\n"
            if len(receipt.items) > 10:
                result_msg += f"... и еще {len(receipt.items) - 10} покупок\n"
        
        result_msg += f"\n📁 Сохранено: `{os.path.basename(csv_filename)}`"
                
        # Add download button (use only filename to avoid 64-byte callback_data limit)
        download_data = f"download_{os.path.basename(csv_filename)}"
        logger.info(f"New receipt download callback_data length: {len(download_data.encode('utf-8'))} bytes: {download_data}")
        
        if len(download_data.encode('utf-8')) > 64:
            file_hash = hashlib.md5(os.path.basename(csv_filename).encode()).hexdigest()[:8]
            self.qr_cache[file_hash] = os.path.basename(csv_filename)
            download_data = f"download_{file_hash}"
            logger.info(f"Using hash instead: {download_data}")
        
        keyboard = [[InlineKeyboardButton("📥 Загрузить CSV", callback_data=download_data)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            result_msg,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
    async def handle_csv_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle CSV download button - send file from disk."""
        query = update.callback_query
        await query.answer()
        
        # Extract identifier from callback data
        identifier = query.data.replace('download_', '', 1)

        # Check if it's a hash (8 chars) or filename
        if len(identifier) == 8 and identifier in self.qr_cache:
            filename = self.qr_cache[identifier]
        else:
            filename = identifier

        csv_path = os.path.join(self.receipts_dir, filename)
        
        if not os.path.exists(csv_path):
            await query.edit_message_text(
                "❌ Файл не найден. Возможно он был удален.",
                reply_markup=None
            )
            await query.message.reply_text(
                "Используйте меню ниже:",
                reply_markup=get_main_menu_markup()
            )
            return
            
        try:
            with open(csv_path, 'rb') as csv_file:
                await query.message.reply_document(
                    document=csv_file,
                    filename=filename,
                    caption="📊 данные Чека"
                )
            
            # Remove download button after successful download
            await query.edit_message_reply_markup(reply_markup=None)
                
        except Exception as e:
            logger.error(f"Загрузка CSV неудачна: {e}", exc_info=True)
            await query.message.reply_text(
                f"❌ Ошибка загрузки: {str(e)}",
                reply_markup=get_main_menu_markup()
            )
            
    async def handle_reverify(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle re-verification request."""
        query = update.callback_query
        await query.answer()
        
        # Check rate limit first
        if not self.rate_limiter.can_make_request():
            remaining = self.rate_limiter.get_remaining()
            await query.message.reply_text(
                f"⚠️ *Дневной лимит запросов достигнут*\n\n"
                f"Не более {MAX_DAILY_REQUESTS} запросов в день.\n"
                f"Осталось сегодня: {remaining}\n\n"
                f"Попробуйте завтра или загрузите имеющиеся чеки.",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
            return
        
        callback_data = query.data
        
        try:
            if callback_data.startswith('reverify_manual_'):
                # Manual entry re-verification
                parts = callback_data.replace('reverify_manual_', '').split('_')
                fn, fd, fp, t, n, s = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                
                await query.message.reply_text("🔄 Перепроверка через API...")
                
                params_dict = {
                    'fn': fn,
                    'fd': fd,
                    'fp': fp,
                    't': t,
                    'n': n,
                    's': s,
                    'qr': '0'
                }
                params = RequestBuilder.from_manual_params(params_dict)
                receipt = await asyncio.to_thread(self.verifier.verify_receipt, params)
                
                # Increment counter if successful
                if receipt.is_valid:
                    self.rate_limiter.increment()
                
                # Create mock update for _send_receipt_result
                mock_update = Update(update_id=update.update_id, message=query.message)
                await self._send_receipt_result(mock_update, receipt)
                
            elif callback_data.startswith('reverify_qr_'):
                # QR string re-verification using hash lookup
                qr_hash = callback_data.replace('reverify_qr_', '', 1)
                qr_string = self.qr_cache.get(qr_hash)
                
                if not qr_string:
                    await query.message.reply_text(
                        "❌ Сессия устарела. Отправьте QR строку снова.",
                        reply_markup=get_main_menu_markup()
                    )
                    return
                
                await query.message.reply_text("🔄 Перепроверка через API...")
                
                request = RequestBuilder.from_qr_string(qr_string)
                receipt = await asyncio.to_thread(self.verifier.verify_receipt, request)
                
                # Increment counter if successful
                if receipt.is_valid:
                    self.rate_limiter.increment()
                
                mock_update = Update(update_id=update.update_id, message=query.message)
                await self._send_receipt_result(mock_update, receipt)
                
        except Exception as e:
            logger.error(f"Перепроверка неудачна: {e}", exc_info=True)
            await query.message.reply_text(
                f"❌ Ошибка переproверки: {str(e)}",
                reply_markup=get_main_menu_markup()
            )
            
    async def handle_button_press(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Route keyboard button presses."""
        text = update.message.text
        
        if text == '📱 Распознать из QR строки':
            await update.message.reply_text(
                "📱 Отправьте QR строку в формате:\n"
                "`t=...&s=...&fn=...&i=...&fp=...&n=1`",
                parse_mode='Markdown',
                reply_markup=get_main_menu_markup()
            )
        elif text == '📊 Статистика':
            await self.stats_command(update, context)
        elif text == '❓ Help':
            await self.help_command(update, context)
            
    def build_application(self) -> Application:
        """Build bot application with all handlers."""
        self.app = Application.builder().token(self.telegram_token).build()
        
        # Conversation handler for manual entry
        conv_handler = ConversationHandler(
            entry_points=[MessageHandler(filters.Regex('^⌨️ Ввод параметров вручную$'), self.manual_entry_start)],
            states={
                ASKING_FN: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_fn)],
                ASKING_FD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_fd)],
                ASKING_FP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_fp)],
                ASKING_T: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_t)],
                ASKING_N: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_n)],
                ASKING_S: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.ask_s)],
            },
            fallbacks=[CommandHandler('cancel', self.cancel)],
        )
        
        # Register handlers
        self.app.add_handler(CommandHandler('start', self.start_command))
        self.app.add_handler(CommandHandler('menu', self.menu_command))
        self.app.add_handler(CommandHandler('help', self.help_command))
        self.app.add_handler(CommandHandler('stats', self.stats_command))
        self.app.add_handler(conv_handler)
        self.app.add_handler(CallbackQueryHandler(self.handle_csv_download, pattern='^download_'))
        self.app.add_handler(CallbackQueryHandler(self.handle_reverify, pattern='^reverify'))
        self.app.add_handler(MessageHandler(filters.Regex('^(📱|📊|❓)'), self.handle_button_press))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_qr_string))
        
        return self.app
        
    def run(self) -> None:
        """Start bot with polling mode."""
        app = self.build_application()
        logger.info("🤖 Bot starting...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)

def load_config(config_file: str = "config.json") -> Dict[str, str]:
    """
    Load configuration from JSON file.
    
    Expected format:
    {
        "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF...",
        "RECEIPT_API_TOKEN": "36119.XH3NCszf0..."
    }
    
    Args:
        config_file: Path to configuration JSON file
        
    Returns:
        Dictionary with token values
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        KeyError: If required tokens missing from config
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(
            f"Configuration file '{config_file}' not found. "
            f"Create it with TELEGRAM_BOT_TOKEN and RECEIPT_API_TOKEN."
        )
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")
    
    # Validate required fields
    required = ['TELEGRAM_BOT_TOKEN', 'RECEIPT_API_TOKEN']
    missing = [key for key in required if key not in config]
    
    if missing:
        raise KeyError(f"Missing required configuration: {', '.join(missing)}")
    
    return config

def main():
    """Entry point with JSON configuration file."""
    # Try loading from config file first
    try:
        config = load_config("config.json")
        telegram_token = config['TELEGRAM_BOT_TOKEN']
        receipt_token = config['RECEIPT_API_TOKEN']
        logger.info("Configuration loaded from config.json")
    except FileNotFoundError:
        # Fallback to environment variables
        logger.warning("config.json not found, using environment variables")
        telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        receipt_token = os.getenv('RECEIPT_API_TOKEN')
    except (KeyError, ValueError) as e:
        logger.error(f"Configuration error: {e}")
        return
    
    if not telegram_token or not receipt_token:
        logger.error("Missing required tokens!")
        logger.error("Create config.json with TELEGRAM_BOT_TOKEN and RECEIPT_API_TOKEN")
        logger.error("Or set environment variables")
        return
        
    bot = ReceiptBot(telegram_token, receipt_token)
    bot.run()


if __name__ == '__main__':
    main()