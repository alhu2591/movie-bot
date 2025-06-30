import os
import threading
import asyncio
import schedule
from datetime import datetime, timedelta
import html
import logging
import time # For time.sleep in schedule_job
import subprocess # For package installation

# Import modules
import db_manager
import scrapers
import utils # Contains clean_title, deduce_category, validate_url_async
import config # New: Import configuration settings

# Import necessary Telegram types
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import telegram # To get telegram.__version__

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Package Installation and Verification (Less Aggressive) ---
def ensure_packages_installed():
    """
    Verifies that critical Python packages are importable.
    Assumes packages are installed via requirements.txt by the deployment environment.
    """
    logger.info("Verifying critical imports...")
    try:
        # Attempt to import core libraries
        import requests
        import beautifulsoup4
        import lxml
        import python_telegram_bot
        import aiohttp
        import schedule
        logger.info("✅ All core Python packages are importable.")
    except ImportError as e:
        logger.critical(f"❌ Critical ImportError: {e}")
        logger.critical("One or more required packages are not installed. Please ensure 'requirements.txt' is correct and dependencies are installed.")
        sys.exit(1) # Exit if critical imports fail

    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
        from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
        logger.info("✅ Core Python-Telegram-Bot imports successful.")
    except ImportError as e:
        logger.critical(f"❌ Critical ImportError for python-telegram-bot components: {e}")
        logger.critical("This usually means 'python-telegram-bot' is not correctly installed or a conflicting 'telegram' package exists.")
        logger.critical("Ensure you are using a compatible Python version (e.g., 3.11 or 3.12) and 'python-telegram-bot==20.7' is in requirements.txt.")
        sys.exit(1)

# Call this at the very beginning of the script execution
ensure_packages_installed()


# Global variable to store the next scheduled update time
next_update_time = None

# --- Flask keep_alive server setup ---
app = Flask(__name__)
@app.route('/')
def home():
    return "🎬 بوت الأفلام يعمل بنجاح! | 12 موقع سينمائي | تحديث كل 6 ساعات | Keep-Alive مفعل"

def run_flask_app():
    app.run(host='0.0.0.0', port=8080)

# Start Flask in a separate thread
threading.Thread(target=run_flask_app, daemon=True).start()

# --- Async Functions for Telegram Handlers ---

async def send_new_movies(context: ContextTypes.DEFAULT_TYPE): 
    """
    Scrapes for new movies and sends them to users based on their preferences.
    """
    logger.info("Starting scheduled movie scraping and sending process.")
    new_movies_to_send = await scrapers.scrape_movies_and_get_new()
    if not new_movies_to_send:
        logger.info("No new movies to send in this round.")
        return

    users_with_prefs = db_manager.get_all_users_with_preferences()

    for user_id, receive_movies, receive_series, receive_anime in users_with_prefs:
        try:
            filtered_movies = []
            for movie in new_movies_to_send: 
                if (movie.get('category') == 'فيلم' and receive_movies) or \
                   (movie.get('category') == 'مسلسل' and receive_series) or \
                   (movie.get('category') == 'أنمي' and receive_anime):
                    filtered_movies.append(movie)
            
            if not filtered_movies:
                continue 

            await context.bot.send_message(
                chat_id=user_id,
                text="🎬 <b>أفلام جديدة متاحة:</b>\n\n",
                parse_mode='HTML'
            )
            await asyncio.sleep(0.5) 

            for movie in filtered_movies: 
                escaped_title = html.escape(movie['title'])
                
                photo_caption_text = (
                    f"🎬 <b>العنوان:</b> {escaped_title}\n"
                )
                if movie.get('release_year'):
                    photo_caption_text += f"📅 <b>سنة الإصدار:</b> {movie['release_year']}\n"
                if movie.get('genres'):
                    photo_caption_text += f"🎭 <b>النوع:</b> {movie['genres']}\n" # New: Display genres
                photo_caption_text += (
                    f"🎬 <b>المصدر:</b> {movie['source']}\n"
                    f"🎬 <b>الفئة:</b> {movie['category']}\n"
                )
                
                if movie.get('description'):
                    description_text = movie['description'].strip()
                    if description_text:
                        photo_caption_text += f"\n📝 <b>الوصف:</b> {description_text}\n"
                
                # Inline keyboard for Watch, Rate, and Add to Favorites
                keyboard = [
                    [InlineKeyboardButton("اضغط هنا للمشاهدة", url=movie["url"])],
                    [
                        InlineKeyboardButton("⭐ تقييم الفيلم", callback_data=f'rate_{movie["url"]}'),
                        InlineKeyboardButton("❤️ إضافة للمفضلة", callback_data=f'add_fav_{movie["url"]}') # New: Add to Favorites button
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                image_to_send = movie['image_url'] if movie['image_url'] else "https://placehold.co/600x400/cccccc/333333?text=No+Image+Available"

                try:
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=image_to_send,
                        caption=photo_caption_text, 
                        parse_mode='HTML',
                        reply_markup=reply_markup 
                    )
                    await asyncio.sleep(0.3) 

                except Exception as photo_e:
                    logger.error(f"❌ Error sending photo to user {user_id} for movie {movie['title']}: {photo_e}")
                    # Fallback to text message if photo fails
                    fallback_text = (
                        f"🎬 <b>العنوان:</b> {escaped_title}\n"
                    )
                    if movie.get('release_year'):
                        fallback_text += f"📅 <b>سنة الإصدار:</b> {movie['release_year']}\n"
                    if movie.get('genres'):
                        fallback_text += f"🎭 <b>النوع:</b> {movie['genres']}\n"
                    fallback_text += (
                        f"🎬 <b>المصدر:</b> {movie['source']}\n"
                        f"🎬 <b>الفئة:</b> {movie['category']}\n"
                    )
                    if movie.get('description'):
                        fallback_text += f"\n📝 <b>الوصف:</b> {movie['description'].strip()}\n"
                    fallback_text += f'\n🔗 <b>رابط المشاهدة:</b> <a href="{movie["url"]}">اضغط هنا للمشاهدة</a>'
                    
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=fallback_text,
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"❌ Error sending movies to user {user_id}: {e}")

async def self_ping_async():
    """Pings the local Flask server to keep the service alive."""
    try:
        # Use aiohttp for self-ping for consistency with other async operations
        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8080", timeout=10) as response:
                response.raise_for_status()
                logger.info(f"✅ Self-ping successful! Status: {response.status_code}")
    except aiohttp.ClientError as e:
        logger.error(f"❌ Self-ping failed: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error during self-ping: {e}")

# --- Telegram Bot Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    user = update.effective_user
    db_manager.add_user(user.id, user.username, user.first_name, user.last_name) 
    await main_menu_internal(user.id, context, user_first_name=user.first_name)

async def main_menu_internal(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user_first_name: str = "عزيزي المستخدم"):
    """Sends the main menu with persistent ReplyKeyboard buttons."""
    keyboard = [
        [KeyboardButton("⚙️ إعدادات التنبيهات")],
        [KeyboardButton("🔄 تحديث الآن")],
        [KeyboardButton("🔍 بحث عن فيلم"), KeyboardButton("📊 حالة المواقع")],
        [KeyboardButton("⏰ التحديث التالي"), KeyboardButton("❤️ مفضلاتي")] # New: Favorites button
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    welcome_msg = (
        f"🎉 مرحباً {user_first_name}!\n"
        "أنا بوت الأفلام الذكي، سأرسل لك أحدث الأفلام تلقائياً من 12 موقع سينمائي:\n"
        "- Wecima, TopCinema, CimaClub, TukTukCima, EgyBest, MyCima,\n"
        "- Akoam, Shahid4u, Aflamco, Cima4u, Fushaar, Aflaam, EgyDead.\n\n"
        f"⏰ سيصلك تحديث بالأفلام الجديدة كل {config.SCRAPE_INTERVAL_HOURS} ساعات تلقائياً.\n" 
        "استخدم الأزرار أدناه للتحكم في البوت."
    )
    await context.bot.send_message(chat_id=chat_id, text=welcome_msg, parse_mode='HTML', reply_markup=reply_markup)

async def settings_command_internal(chat_id: int, context: ContextTypes.DEFAULT_TYPE, message_id: int = None, edit_mode: bool = False):
    """Sends or edits the settings message with inline keyboard for preferences."""
    user_prefs = db_manager.get_user_preferences(chat_id)
    
    movies_status = "✅ مفعل" if user_prefs["movies"] else "❌ معطل"
    series_status = "✅ مفعل" if user_prefs["series"] else "❌ معطل"
    anime_status = "✅ مفعل" if user_prefs["anime"] else "❌ معطل"

    settings_text = (
        "⚙️ <b>إعدادات التنبيهات:</b>\n"
        "اختر أنواع المحتوى التي ترغب في تلقي تنبيهات عنها:\n\n"
        f"• الأفلام: {movies_status}\n"
        f"• المسلسلات: {series_status}\n"
        f"• الأنمي: {anime_status}\n\n"
        "اضغط على الزر لتغيير الحالة."
    )

    keyboard = [
        [InlineKeyboardButton(f"الأفلام: {movies_status}", callback_data='toggle_movies')],
        [InlineKeyboardButton(f"المسلسلات: {series_status}", callback_data='toggle_series')],
        [InlineKeyboardButton(f"الأنمي: {anime_status}", callback_data='toggle_anime')],
        [InlineKeyboardButton("⬅️ رجوع", callback_data='back_to_main_menu')] 
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if edit_mode and message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=settings_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Error editing settings message for user {chat_id} (message {message_id}): {e}")
            await context.bot.send_message(chat_id=chat_id, text=settings_text, parse_mode='HTML', reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id=chat_id, text=settings_text, parse_mode='HTML', reply_markup=reply_markup)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles inline keyboard button callbacks."""
    query = update.callback_query
    await query.answer() 

    chat_id = query.message.chat_id 

    if query.data.startswith('toggle_'):
        pref_type = query.data.replace('toggle_', '')
        current_prefs = db_manager.get_user_preferences(chat_id)
        new_value = 0 if current_prefs.get(pref_type) else 1
        
        if db_manager.update_user_preference(chat_id, f"receive_{pref_type}", new_value):
            await settings_command_internal(chat_id, context, query.message.message_id, edit_mode=True)
        else:
            await context.bot.send_message(chat_id=chat_id, text="حدث خطأ أثناء تحديث التفضيلات.")
    elif query.data == 'back_to_main_menu':
        user = update.effective_user
        await main_menu_internal(chat_id, context, user_first_name=user.first_name)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete settings message: {e}")
    elif query.data.startswith('rate_'):
        movie_url = query.data.replace('rate_', '')
        movie = db_manager.get_movie_by_url(movie_url)
        if movie:
            keyboard = [[InlineKeyboardButton(f"{star}⭐", callback_data=f'submit_rating_{star}_{movie_url}') for star in range(1, 6)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"يرجى تقييم فيلم <b>{html.escape(movie['title'])}</b> من 1 إلى 5 نجوم:",
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await context.bot.send_message(chat_id=chat_id, text="عذراً، لم أتمكن من العثور على تفاصيل هذا الفيلم للتقييم.")
    elif query.data.startswith('submit_rating_'):
        parts = query.data.split('_')
        rating = int(parts[2])
        movie_url = "_".join(parts[3:])
        
        if db_manager.add_movie_rating(movie_url, rating):
            movie = db_manager.get_movie_by_url(movie_url)
            if movie:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text=f"شكراً لتقييمك! تم تقييم <b>{html.escape(movie['title'])}</b> بـ {rating} نجوم. المتوسط الحالي: {movie['average_rating']:.1f} ({movie['rating_count']} تقييمات).",
                    parse_mode='HTML'
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text=f"شكراً لتقييمك! تم تسجيل تقييمك بـ {rating} نجوم.",
                    parse_mode='HTML'
                )
        else:
            await context.bot.send_message(chat_id=chat_id, text="حدث خطأ أثناء تسجيل تقييمك.")
    elif query.data.startswith('add_fav_'):
        movie_url = query.data.replace('add_fav_', '')
        user_id = query.from_user.id
        movie = db_manager.get_movie_by_url(movie_url)
        if movie:
            if db_manager.add_favorite(user_id, movie_url):
                await context.bot.send_message(chat_id=chat_id, text=f"✅ تم إضافة <b>{html.escape(movie['title'])}</b> إلى مفضلتك!", parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>{html.escape(movie['title'])}</b> موجود بالفعل في مفضلتك.", parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id=chat_id, text="عذراً، لم أتمكن من العثور على تفاصيل هذا الفيلم لإضافته إلى المفضلة.")
    elif query.data.startswith('remove_fav_'):
        movie_url = query.data.replace('remove_fav_', '')
        user_id = query.from_user.id
        movie = db_manager.get_movie_by_url(movie_url)
        if movie:
            if db_manager.remove_favorite(user_id, movie_url):
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text=f"🗑️ تم إزالة <b>{html.escape(movie['title'])}</b> من مفضلتك.",
                    parse_mode='HTML'
                )
                # After removal, potentially refresh the favorites list
                await show_favorites(update, context)
            else:
                await context.bot.send_message(chat_id=chat_id, text="⚠️ الفيلم ليس في مفضلتك أصلاً.")
        else:
            await context.bot.send_message(chat_id=chat_id, text="عذراً، لم أتمكن من العثور على تفاصيل هذا الفيلم لإزالته من المفضلة.")


async def handle_settings_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '⚙️ إعدادات التنبيهات' persistent button."""
    chat_id = update.effective_chat.id
    await settings_command_internal(chat_id, context)

async def handle_manual_update_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '🔄 تحديث الآن' persistent button."""
    chat_id = update.effective_chat.id
    await update.message.reply_text("بدء تحديث الأفلام يدوياً... قد يستغرق الأمر بضع دقائق.", parse_mode='HTML')
    try:
        await send_new_movies(context)
        await update.message.reply_text("✅ تم الانتهاء من التحديث اليدوي للأفلام.", parse_mode='HTML')
    except Exception as e:
        logger.exception("Fatal error during manual movie update.")
        error_message_for_user = f"❌ حدث خطأ أثناء التحديث اليدوي للأفلام. التفاصيل: <code>{html.escape(str(e))[:150]}...</code>"
        await update.message.reply_text(error_message_for_user, parse_mode='HTML')
        if config.ADMIN_CHAT_ID and str(chat_id) != config.ADMIN_CHAT_ID: # Only send to admin if not the admin who triggered it
            try:
                await context.bot.send_message(
                    chat_id=config.ADMIN_CHAT_ID,
                    text=f"⚠️ خطأ في التحديث اليدوي لبوت الأفلام (تم تشغيله بواسطة {update.effective_user.id}): \n<code>{html.escape(str(e))}</code>",
                    parse_mode='HTML'
                )
            except Exception as admin_e:
                    logger.error(f"Failed to send error message to admin: {admin_e}")

async def show_site_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '📊 حالة المواقع' persistent button or /sitestatus command."""
    statuses = db_manager.get_site_statuses()
    message = "📊 <b>حالة المواقع:</b>\n\n"
    if not statuses:
        message += "لا توجد بيانات حالة للمواقع بعد."
    else:
        for site_name, last_scraped, status, last_error in statuses:
            last_scraped_dt = None
            if isinstance(last_scraped, str):
                try:
                    # Try parsing with microseconds first
                    last_scraped_dt = datetime.strptime(last_scraped, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    # Fallback to parsing without microseconds
                    try:
                        last_scraped_dt = datetime.strptime(last_scraped, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        pass # Keep as None if parsing fails
            else: # Assume it's already a datetime object if not string
                last_scraped_dt = last_scraped

            last_scraped_str = last_scraped_dt.strftime('%Y-%m-%d %H:%M') if last_scraped_dt else "N/A"
            status_emoji = "✅" if status == 'active' else "❌"
            error_details = f"\n  (خطأ: <code>{html.escape(last_error[:100])}...</code>)" if last_error else ""
            message += f"<b>{site_name}</b>: {status_emoji} آخر جلب: {last_scraped_str}, الحالة: {status}{error_details}\n"
    
    await update.message.reply_text(message, parse_mode='HTML')

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '🔍 بحث عن فيلم' persistent button or /search command."""
    query_text = ""
    # Check if the message is from the persistent button or a command with arguments
    if update.message.text and update.message.text.startswith("🔍 بحث عن فيلم"):
        # If it's the button, prompt for search query
        await update.message.reply_text("يرجى إدخال كلمة للبحث بعد الأمر. مثال: <code>/search فيلم أكشن</code>", parse_mode='HTML')
        return
    elif context.args:
        query_text = " ".join(context.args).strip()
    
    if not query_text:
        await update.message.reply_text("يرجى إدخال كلمة للبحث.")
        return

    await update.message.reply_text(f"⏳ جارٍ البحث عن: <b>{html.escape(query_text)}</b>...", parse_mode='HTML')

    results = db_manager.get_movies_for_search(query_text, limit=5)

    if not results:
        await update.message.reply_text(f"⚠️ لم يتم العثور على أي نتائج لـ: <b>{html.escape(query_text)}</b>", parse_mode='HTML')
        return

    await update.message.reply_text(f"🔍 <b>نتائج البحث عن '{html.escape(query_text)}':</b>\n\n", parse_mode='HTML')

    for movie_info in results:
        title, url, source, image_url, category, description, release_year, average_rating = movie_info
        escaped_title = html.escape(title)
        
        photo_caption_text = (
            f"🎬 <b>العنوان:</b> {escaped_title}\n"
        )
        if release_year:
            photo_caption_text += f"📅 <b>سنة الإصدار:</b> {release_year}\n"
        # Assuming genres can be inferred from category for now or added to movie_info
        photo_caption_text += (
            f"🎬 <b>المصدر:</b> {source}\n"
            f"🎬 <b>الفئة:</b> {category}\n"
        )
        if average_rating and average_rating > 0:
            photo_caption_text += f"⭐ <b>التقييم:</b> {average_rating:.1f}\n"

        if description:
            description_text = description.strip()
            if description_text:
                photo_caption_text += f"\n📝 <b>الوصف:</b> {description_text}\n"
        
        keyboard = [
            [InlineKeyboardButton("اضغط هنا للمشاهدة", url=url)],
            [
                InlineKeyboardButton("⭐ تقييم الفيلم", callback_data=f'rate_{url}'),
                InlineKeyboardButton("❤️ إضافة للمفضلة", callback_data=f'add_fav_{url}') # Add to Favorites button for search results
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        image_to_send = image_url if image_url else "https://placehold.co/600x400/cccccc/333333?text=No+Image+Available"

        try:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=image_to_send,
                caption=photo_caption_text, 
                parse_mode='HTML',
                reply_markup=reply_markup 
            )
            await asyncio.sleep(0.3) 
        except Exception as photo_e:
            logger.error(f"❌ Error sending photo to user {update.effective_chat.id} for movie {title} during search: {photo_e}")
            fallback_text = (
                f"🎬 <b>العنوان:</b> {escaped_title}\n"
            )
            if release_year:
                fallback_text += f"📅 <b>سنة الإصدار:</b> {release_year}\n"
            fallback_text += (
                f"🎬 <b>المصدر:</b> {source}\n"
                f"🎬 <b>الفئة:</b> {category}\n"
            )
            if average_rating and average_rating > 0:
                fallback_text += f"⭐ <b>التقييم:</b> {average_rating:.1f}\n"
            if description:
                fallback_text += f"\n📝 <b>الوصف:</b> {description.strip()}\n"
            fallback_text += f'\n🔗 <b>رابط المشاهدة:</b> <a href="{url}">اضغط هنا للمشاهدة</a>'
            
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=fallback_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            await asyncio.sleep(0.3)

async def next_update_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '⏰ التحديث التالي' persistent button or /nextupdate command."""
    global next_update_time
    if next_update_time:
        time_diff = next_update_time - datetime.now()
        hours, remainder = divmod(time_diff.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        message = (
            f"⏰ التحديث التلقائي التالي سيكون خلال:\n"
            f"<b>{int(hours)}</b> ساعة و <b>{int(minutes)}</b> دقيقة."
        )
    else:
        message = "⏰ لم يتم تحديد وقت التحديث التالي بعد. قد يكون التحديث الأول قيد التقدم."
    
    await update.message.reply_text(message, parse_mode='HTML')

async def show_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the '❤️ مفضلاتي' persistent button or /favorites command."""
    user_id = update.effective_user.id
    favorites = db_manager.get_favorites(user_id)

    if not favorites:
        await update.message.reply_text("❤️ قائمة مفضلتك فارغة حالياً. يمكنك إضافة أفلام من نتائج البحث أو التحديثات الجديدة.")
        return

    await update.message.reply_text("❤️ <b>أفلامك المفضلة:</b>\n\n", parse_mode='HTML')

    for movie_info in favorites:
        title, url, source, image_url, category, description, release_year, average_rating, rating_count, genres = movie_info
        escaped_title = html.escape(title)
        
        photo_caption_text = (
            f"🎬 <b>العنوان:</b> {escaped_title}\n"
        )
        if release_year:
            photo_caption_text += f"📅 <b>سنة الإصدار:</b> {release_year}\n"
        if genres:
            photo_caption_text += f"🎭 <b>النوع:</b> {genres}\n"
        photo_caption_text += (
            f"🎬 <b>المصدر:</b> {source}\n"
            f"🎬 <b>الفئة:</b> {category}\n"
        )
        if average_rating and average_rating > 0:
            photo_caption_text += f"⭐ <b>التقييم:</b> {average_rating:.1f} ({rating_count} تقييمات)\n"
        
        if description:
            description_text = description.strip()
            if description_text:
                photo_caption_text += f"\n📝 <b>الوصف:</b> {description_text}\n"
        
        # Inline keyboard for each favorite movie
        keyboard = [
            [InlineKeyboardButton("اضغط هنا للمشاهدة", url=url)],
            [InlineKeyboardButton("⭐ تقييم", callback_data=f'rate_{url}'),
             InlineKeyboardButton("🗑️ إزالة من المفضلة", callback_data=f'remove_fav_{url}')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            image_to_send = image_url if image_url else "https://placehold.co/600x400/cccccc/333333?text=No+Image+Available"
            await context.bot.send_photo(
                chat_id=user_id,
                photo=image_to_send,
                caption=photo_caption_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"Error sending favorite movie {title} to user {user_id}: {e}")
            fallback_text = (
                f"🎬 <b>العنوان:</b> {escaped_title}\n"
                f"📅 <b>سنة الإصدار:</b> {release_year if release_year else 'N/A'}\n"
                f"🎬 <b>المصدر:</b> {source}\n"
                f"🎬 <b>الفئة:</b> {category}\n"
                f"⭐ <b>التقييم:</b> {average_rating:.1f} ({rating_count} تقييمات)\n" if average_rating and average_rating > 0 else ""
                f'\n🔗 <b>رابط المشاهدة:</b> <a href="{url}">اضغط هنا للمشاهدة</a>\n'
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=fallback_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            await asyncio.sleep(0.3)


# --- Scheduling Job ---
def schedule_job(application):
    """
    Runs scheduled tasks in a separate thread.
    Tasks include: sending new movies, cleaning old movies, and self-ping.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run_async_task_wrapper_send_new_movies():
        global next_update_time
        try:
            await send_new_movies(application) 
        except Exception as e:
            logger.error(f"Error in scheduled new movie sending task: {e}")
        finally:
            # Update next_update_time after the task completes
            # This logic assumes 'schedule' library correctly updates its next_run time
            # after a job execution.
            next_run_time_obj = schedule.next_run()
            if next_run_time_obj:
                next_update_time = next_run_time_obj
                logger.info(f"Next update time set to: {next_update_time}")


    async def run_async_task_wrapper_self_ping():
        try:
            await self_ping_async()
        except Exception as e:
            logger.error(f"Error in scheduled Self-Ping task: {e}")

    # Schedule tasks using config values
    schedule.every(config.SCRAPE_INTERVAL_HOURS).hours.do(lambda: asyncio.run_coroutine_threadsafe(run_async_task_wrapper_send_new_movies(), loop))
    schedule.every().day.at(config.DB_CLEANUP_TIME).do(db_manager.cleanup_old_movies) 
    schedule.every(config.SELF_PING_INTERVAL_MINUTES).minutes.do(lambda: asyncio.run_coroutine_threadsafe(run_async_task_wrapper_self_ping(), loop))

    logger.info("Starting initial movie collection...")
    # Run initial tasks immediately
    asyncio.run_coroutine_threadsafe(run_async_task_wrapper_send_new_movies(), loop)
    asyncio.run_coroutine_threadsafe(run_async_task_wrapper_self_ping(), loop)

    while True:
        schedule.run_pending()
        time.sleep(30) # Check schedule every 30 seconds

# --- Main Bot Execution ---
def main():
    """Main function to initialize and run the Telegram bot."""
    db_manager.init_db() 
    
    global application
    application = Application.builder().token(config.BOT_TOKEN).build()
    
    logger.info(f"python-telegram-bot version: {telegram.__version__}")

    # Command Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("sitestatus", show_site_status))
    application.add_handler(CommandHandler("search", search_movies))
    application.add_handler(CommandHandler("nextupdate", next_update_time_command))
    application.add_handler(CommandHandler("favorites", show_favorites)) # New: /favorites command
    
    # Callback Query Handler for Inline Buttons (e.g., settings toggles, rating, add/remove favorite)
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # Message Handlers for Persistent Keyboard Buttons (Regex matching text)
    application.add_handler(MessageHandler(filters.Regex(r"^⚙️ إعدادات التنبيهات$"), handle_settings_button))
    application.add_handler(MessageHandler(filters.Regex(r"^🔄 تحديث الآن$"), handle_manual_update_button))
    application.add_handler(MessageHandler(filters.Regex(r"^📊 حالة المواقع$"), show_site_status))
    application.add_handler(MessageHandler(filters.Regex(r"^🔍 بحث عن فيلم$"), search_movies))
    application.add_handler(MessageHandler(filters.Regex(r"^⏰ التحديث التالي$"), next_update_time_command))
    application.add_handler(MessageHandler(filters.Regex(r"^❤️ مفضلاتي$"), show_favorites)) # New: Persistent button for favorites

    # Start scheduling in a separate thread
    threading.Thread(target=schedule_job, args=(application,), daemon=True).start()

    logger.info("✅ Movie bot is now running with 12 cinema sites (powered by aiohttp and BeautifulSoup).") 
    logger.info(f"⏱️ Movies updated automatically every {config.SCRAPE_INTERVAL_HOURS} hours; manual update option available.") 
    logger.info("🌐 Keep-Alive server running on port 8080.")
    logger.info("⚙️ Use commands like /start, /settings, /search, /nextupdate, /sitestatus, and /favorites.") 
    application.run_polling()

if __name__ == '__main__':
    main()
