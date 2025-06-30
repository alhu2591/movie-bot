import subprocess
import sys # تم إضافة هذا الاستيراد لحل مشكلة NameError
import os
import re
import sqlite3
import threading
import schedule
import time
import asyncio
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from flask import Flask
from urllib.parse import urlparse, urlunparse
from playwright.async_api import async_playwright
import site # تم إضافة هذا الاستيراد لتحسين اكتشاف مسار المكتبات

# --- Logging Setup ---
import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Add virtual environment site-packages to sys.path explicitly ---
# This is a more robust way to ensure all installed packages are discoverable at runtime,
# especially in environments like Render.com where default sys.path might be incomplete.
try:
    # sys.prefix points to the root of the virtual environment
    # Construct the path to site-packages based on common venv structure and Python version
    python_version_dir = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages_path = os.path.join(sys.prefix, 'lib', python_version_dir, 'site-packages')

    if os.path.isdir(site_packages_path) and site_packages_path not in sys.path:
        sys.path.insert(0, site_packages_path)
        logger.info(f"Added explicit site-packages path to sys.path: {site_packages_path}")
    else:
        logger.warning(f"Could not find or add site-packages path: {site_packages_path}")
except Exception as e:
    logger.critical(f"FATAL ERROR: Failed to configure sys.path for package discovery: {e}")
    sys.exit(1) # Exit early if we can't ensure packages are found


# --- Package Installation and Verification ---
def ensure_packages_installed():
    """
    تتحقق من أن المكتبات الأساسية قابلة للاستيراد.
    تفترض أن المكتبات مثبتة عبر requirements.txt بواسطة بيئة النشر.
    """
    logger.info("Verifying critical imports...")
    try:
        # محاولة استيراد المكتبات الأساسية
        import bs4 # تم تغيير هذا السطر من 'beautifulsoup4' إلى 'bs4'
        import lxml
        import python_telegram_bot
        import aiohttp
        import schedule
        import playwright # تم إضافة playwright للتحقق
        logger.info("✅ جميع مكتبات Python الأساسية قابلة للاستيراد.")
    except ImportError as e:
        logger.critical(f"❌ خطأ حرج في الاستيراد: {e}")
        logger.critical("واحد أو أكثر من المكتبات المطلوبة غير مثبت. يرجى التأكد من أن 'requirements.txt' صحيح وأن الاعتمادات مثبتة.")
        sys.exit(1) # الخروج إذا فشل الاستيراد الحرج

    try:
        # تم نقل هذا الاستيراد إلى هنا لضمان أن 'telegram' موجود قبل محاولة استيراد مكوناته
        # هذا يحل مشكلة محتملة إذا كان python-telegram-bot غير مثبت بشكل صحيح
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
        from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
        logger.info("✅ استيراد مكونات Python-Telegram-Bot الأساسية بنجاح.")
    except ImportError as e:
        logger.critical(f"❌ خطأ حرج في الاستيراد لمكونات python-telegram-bot: {e}")
        logger.critical("هذا يعني عادةً أن 'python-telegram-bot' غير مثبت بشكل صحيح أو أن هناك حزمة 'telegram' متعارضة موجودة.")
        logger.critical("تأكد من أنك تستخدم إصدار Python متوافق (مثل 3.11 أو 3.12) وأن 'python-telegram-bot==20.7' موجود في requirements.txt.")
        sys.exit(1)

# استدعاء الدالة عند بدء تشغيل البوت
ensure_packages_installed()

# تأكد من تثبيت المتصفحات تلقائياً عند التشغيل
def install_playwright_browsers():
    try:
        logger.info("Attempting to install Playwright browsers...")
        # استخدام --with-deps لضمان تثبيت جميع الاعتمادات الضرورية
        # استخدام sys.executable لضمان استخدام Python الصحيح في البيئة الافتراضية
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "--with-deps"], capture_output=True, text=True, check=True)
        logger.info("✅ تم تثبيت متصفحات Playwright بنجاح.")
        logger.debug(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.critical(f"❌ خطأ في تثبيت متصفحات Playwright:\n{e.stderr}")
        sys.exit(1) # الخروج إذا فشل تثبيت المتصفح
    except Exception as e:
        logger.critical(f"❌ حدث خطأ غير متوقع أثناء تثبيت المتصفحات: {e}")
        sys.exit(1)

# استدعاء الدالة عند بدء تشغيل البوت
install_playwright_browsers()

# --- إعدادات البوت ---
TOKEN = os.getenv("BOT_TOKEN", "7576844775:AAGyos4JkSNiiiwQ5oeCJdAw-2ajMkVdUUA") # تم تحديث هذا الرمز برمز البوت الجديد الخاص بك.

# --- إعداد خادم keep_alive ---
app = Flask(__name__)
@app.route('/')
def home():
    return "🎬 بوت الأفلام يعمل بنجاح! | 12 موقع سينمائي | تحديث كل ساعة"
def run_flask_app():
    app.run(host='0.0.0.0', port=8080)
threading.Thread(target=run_flask_app, daemon=True).start()

# --- تهيئة قاعدة البيانات ---
def init_db():
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS movies
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL,
                 url TEXT NOT NULL UNIQUE,
                 source TEXT NOT NULL,
                 image_url TEXT,
                 last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # Add image_url column if it doesn't exist
    try:
        c.execute("ALTER TABLE movies ADD COLUMN image_url TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering table: {e}")
    
    c.execute('''CREATE TABLE IF NOT EXISTS users
                (user_id INTEGER PRIMARY KEY,
                 username TEXT,
                 first_name TEXT,
                 last_name TEXT,
                 join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

# --- إضافة مستخدم جديد ---
def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
              (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

# --- تنظيف العناوين ---
def clean_title(title):
    # إزالة (سنة) أو [سنة] أو كلمات مثل "مترجم" أو "اون لاين"
    title = re.sub(r'\s*\(\d{4}\)|\s*\[.*?\]|\s*مترجم|\s*اون لاين|\s*online|\s*HD|\s*WEB-DL|\s*BluRay|\s*نسخة مدبلجة', '', title, flags=re.IGNORECASE)
    # إزالة أي أحرف غير أبجدية رقمية أو مسافات، باستثناء المسافات
    title = re.sub(r'[^\w\s\u0600-\u06FF]+', '', title) # يدعم العربية
    # استبدال مسافات متعددة بمسافة واحدة
    title = re.sub(r'\s{2,}', ' ', title)
    return title.strip()


# --- دوال تحليل المواقع (تم التحديث) ---

def parse_wecima(soup):
    movies = []
    for item in soup.select("div.GridItem"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Wecima: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("strong.hasyear") or item.select_one("img")
            title = ""
            if title_tag:
                if title_tag.name == 'strong':
                    title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    title = title_tag.get("alt", "N/A")
            if not title or title == "N/A":
                logger.debug(f"Wecima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر" # Provide a default title

            image_url = None
            bg_style_tag = item.select_one("span.BG--GridItem")
            if bg_style_tag and 'data-lazy-style' in bg_style_tag.attrs:
                match = re.search(r'url\((.*?)\)', bg_style_tag['data-lazy-style'])
                if match:
                    image_url = match.group(1).strip("'\"")
            
            if not image_url:
                img_tag = item.select_one("img")
                if img_tag:
                    image_url = img_tag.get("data-src") or img_tag.get("src")
            if not image_url:
                logger.debug(f"Wecima: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image" # Placeholder

            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Wecima"})
        except Exception as e:
            logger.error(f"❌ Error parsing Wecima item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_topcinema(soup):
    movies = []
    for item in soup.select("div.col-lg-2.col-md-3.col-sm-4.col-xs-6.col-6.MovieBlock"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"TopCinema: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.Title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"TopCinema: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TopCinema: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "TopCinema"})
        except Exception as e:
            logger.error(f"❌ Error parsing TopCinema item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_cimaclub(soup):
    movies = []
    for item in soup.select("div.Small--Box"):
        try:
            link_tag = item.select_one("a.recent--block")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"CimaClub: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title = "عنوان غير متوفر" # Default value
            
            # Attempt 1: Get title from h2 within inner--title
            title_h2_tag = item.select_one(".inner--title h2")
            if title_h2_tag:
                extracted_title = title_h2_tag.get_text(strip=True)
                if extracted_title:
                    title = extracted_title
            
            # Attempt 2: If h2 failed, try img alt attribute
            if title == "عنوان غير متوفر":
                img_tag_for_title = item.select_one("div.Poster img")
                if img_tag_for_title:
                    extracted_title = img_tag_for_title.get("alt", "")
                    if extracted_title:
                        title = extracted_title
            
            # Attempt 3: If img alt failed, try link title attribute
            if title == "عنوان غير متوفر":
                extracted_title = link_tag.get("title", "")
                if extracted_title:
                    title = extracted_title

            if title == "عنوان غير متوفر":
                logger.debug(f"CimaClub: Could not extract title for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر" # Ensure default if all attempts fail

            img_tag = item.select_one("div.Poster img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"CimaClub: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image" # Placeholder
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "CimaClub"})
        except Exception as e:
            logger.error(f"❌ Error parsing CimaClub item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_tuktukcima(soup):
    movies = []
    for item in soup.select("div.Blocks ul li.MovieBlock"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"TukTukCima: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.Title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"TukTukCima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TukTukCima: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "TukTukCima"})
        except Exception as e:
            logger.error(f"❌ Error parsing TukTukCima item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_egy_onl(soup):
    movies = []
    for item in soup.select("div.Blocks ul.MovieList div.movie-box"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"EgyBest: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            # العنوان موجود في alt للصورة
            title_tag = item.select_one("img")
            title = title_tag.get("alt", "N/A") if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"EgyBest: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            image_url = title_tag.get("data-src") or title_tag.get("src") if title_tag else None
            if not image_url:
                logger.debug(f"EgyBest: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "EgyBest"})
        except Exception as e:
            logger.error(f"❌ Error parsing EgyBest item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies


def parse_mycima(soup):
    movies = []
    for item in soup.select("div.GridItem"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"MyCima: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("strong.hasyear") or item.select_one("img")
            title = ""
            if title_tag:
                if title_tag.name == 'strong':
                    title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    title = title_tag.get("alt", "N/A")
            if not title or title == "N/A":
                logger.debug(f"MyCima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"

            image_url = None
            bg_style_tag = item.select_one("span.BG--GridItem")
            if bg_style_tag and 'data-lazy-style' in bg_style_tag.attrs:
                match = re.search(r'url\((.*?)\)', bg_style_tag['data-lazy-style'])
                if match:
                    image_url = match.group(1).strip("'\"")
            
            if not image_url:
                img_tag = item.select_one("img")
                if img_tag:
                    image_url = img_tag.get("data-src") or img_tag.get("src")
            if not image_url:
                logger.debug(f"MyCima: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"

            movies.append({"title": title, "url": link, "image_url": image_url, "source": "MyCima"})
        except Exception as e:
            logger.error(f"❌ Error parsing MyCima item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_akoam(soup):
    movies = []
    for item in soup.select("div.movie-box"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Akoam: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.Title") or item.select_one("img") # Title can be in h2 or img alt
            title = ""
            if title_tag:
                if title_tag.name == 'h2':
                    title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    title = title_tag.get("alt", "N/A")
            if not title or title == "N/A":
                logger.debug(f"Akoam: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Akoam: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Akoam"})
        except Exception as e:
            logger.error(f"❌ Error parsing Akoam item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_shahid4u(soup):
    movies = []
    for item in soup.select("div.GridItem"):
        try:
            link_tag = item.select_one("a.MovieBlock") # Specific anchor tag
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Shahid4u: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.MovieTitle")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"Shahid4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("src") if img_tag else None # Shahid4u يستخدم src مباشرة
            if not image_url:
                logger.debug(f"Shahid4u: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Shahid4u"})
        except Exception as e:
            logger.error(f"❌ Error parsing Shahid4u item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_aflamco(soup):
    movies = []
    for item in soup.select("div.ModuleItem"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Aflamco: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.ModuleTitle")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"Aflamco: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Aflamco: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Aflamco"})
        except Exception as e:
            logger.error(f"❌ Error parsing Aflamco item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_cima4u(soup):
    movies = []
    for item in soup.select("div.MovieBlock"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Cima4u: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.Title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"Cima4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Cima4u: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Cima4u"})
        except Exception as e:
            logger.error(f"❌ Error parsing Cima4u item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_fushaar(soup):
    movies = []
    for item in soup.select("div.Blocks .MovieBlock"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Fushaar: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h2.Title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"Fushaar: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-lazy-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Fushaar: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Fushaar"})
        except Exception as e:
            logger.error(f"❌ Error parsing Fushaar item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_aflaam(soup):
    movies = []
    for item in soup.select("div.movies-list-grid div.item"):
        try:
            link_tag = item.select_one("a.box")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"Aflaam: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            title_tag = item.select_one("h3.entry-title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not title or title == "N/A":
                logger.debug(f"Aflaam: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                title = "عنوان غير متوفر"
            
            img_tag = item.select_one("picture img.lazy") 
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Aflaam: Image URL not found for title '{title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": title, "url": link, "image_url": image_url, "source": "Aflaam"})
        except Exception as e:
            logger.error(f"❌ Error parsing Aflaam item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

# --- قائمة المواقع (12 موقع) ---
# Function to get the base URL
def get_base_url(full_url):
    parsed_url = urlparse(full_url)
    return urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', '')) + "/"

SCRAPERS = [
    {"name": "Wecima", "url": get_base_url("https://wecima.video"), "parser": parse_wecima},
    {"name": "TopCinema", "url": get_base_url("https://web6.topcinema.cam"), "parser": parse_topcinema},
    {"name": "CimaClub", "url": get_base_url("https://cimaclub.day"), "parser": parse_cimaclub},
    {"name": "TukTukCima", "url": get_base_url("https://tuktukcima.art"), "parser": parse_tuktukcima},
    {"name": "EgyBest", "url": get_base_url("https://egy.onl"), "parser": parse_egy_onl}, 
    {"name": "MyCima", "url": get_base_url("https://mycima.video"), "parser": parse_mycima},
    {"name": "Akoam", "url": get_base_url("https://akw.onl"), "parser": parse_akoam},
    {"name": "Shahid4u", "url": get_base_url("https://shahed4uapp.com"), "parser": parse_shahid4u},
    {"name": "Aflamco", "url": get_base_url("https://aflamco.cloud"), "parser": parse_aflamco},
    {"name": "Cima4u", "url": get_base_url("https://cima4u.cam"), "parser": parse_cima4u},
    {"name": "Fushaar", "url": get_base_url("https://www.fushaar.com"), "parser": parse_fushaar},
    {"name": "Aflaam", "url": get_base_url("https://aflaam.com"), "parser": parse_aflaam}
]

# --- جلب الأفلام من موقع واحد (المعدلة لاستخدام Playwright) ---
async def scrape_site_async(scraper, page): # تأخذ page بدلاً من driver
    try:
        logger.info(f"جارٍ فحص موقع: {scraper['name']}")
        # الانتقال إلى الصفحة والانتظار حتى تحميل المحتوى
        await page.goto(scraper["url"], wait_until="domcontentloaded", timeout=60000) 

        # الحصول على محتوى الصفحة بعد تحميلها بالكامل
        page_content = await page.content()

        # حفظ نسخة من HTML الصفحة للمعاينة (للتصحيح)
        with open(f"debug_{scraper['name']}.html", "wb") as f:
            f.write(page_content.encode('utf-8')) # تأكد من الترميز

        soup = BeautifulSoup(page_content, 'html.parser')
        movies = scraper["parser"](soup)

        # طباعة عدد الأفلام المستخرجة
        if movies:
            logger.info(f"✅ {len(movies)} فيلم تم استخراجه من {scraper['name']}")
        else:
            logger.warning(f"⚠️ لم يتم العثور على أفلام في {scraper['name']} باستخدام المحددات الحالية. يرجى التحقق من debug_{scraper['name']}.html")

        return movies

    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع أثناء تحليل {scraper['name']}: {e}")
        return []

# --- جلب الأفلام من جميع المواقع (المعدلة لاستخدام Playwright) ---
async def scrape_movies_async(): # تحويل الدالة لتصبح async
    new_movies = []
    total_added_count = 0 
    browser = None # تهيئة browser خارج try لتأكيد إغلاقه في finally
    try:
        # تعيين مسار المتصفحات لـ Playwright
        # هذا يخبر Playwright بالبحث عن المتصفحات في دليل ذاكرة التخزين المؤقت لـ Replit
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(os.path.expanduser("~"), ".cache", "ms-playwright")
        
        async with async_playwright() as p:
            # تشغيل متصفح Chromium في الوضع المخفي
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
            ) 

            conn = sqlite3.connect('movies.db')
            c = conn.cursor()

            # إنشاء مهام كشط لكل موقع بالتوازي
            tasks = []
            for scraper in SCRAPERS:
                page = await browser.new_page() # إنشاء صفحة جديدة لكل موقع
                tasks.append(scrape_site_async(scraper, page))

            # تنفيذ جميع مهام الكشط بالتوازي
            results = await asyncio.gather(*tasks)

            for scraper_idx, movies in enumerate(results):
                scraper = SCRAPERS[scraper_idx] # الحصول على معلومات السكرابر الأصلية
                added_count = 0
                for movie in movies:
                    try:
                        # تنظيف العنوان قبل إدخاله في قاعدة البيانات
                        clean_title_text = clean_title(movie["title"])
                        # التحقق مما إذا كان الفيلم موجودًا بالفعل باستخدام الرابط النظيف (clean URL)
                        c.execute("SELECT id FROM movies WHERE url = ?", (movie["url"],))
                        if c.fetchone() is None:
                            c.execute("INSERT INTO movies (title, url, source, image_url) VALUES (?, ?, ?, ?)",
                                      (clean_title_text, movie["url"], scraper["name"], movie.get("image_url")))
                            new_movies.append({
                                "title": clean_title_text,
                                "url": movie["url"],
                                "source": scraper["name"],
                                "image_url": movie.get("image_url")
                            })
                            added_count += 1
                    except sqlite3.IntegrityError:
                        # هذا يحدث إذا كان هناك فيلم بنفس الـ URL موجود بالفعل (UNIQUE constraint)
                        pass
                    except Exception as e:
                        logger.error(f"  ❌ خطأ في إضافة فيلم من {scraper['name']} ({movie.get('title', 'N/A')}): {e}")
                
                if added_count > 0:
                    logger.info(f"  ✅ تمت إضافة {added_count} أفلام جديدة من {scraper['name']}")
                total_added_count += added_count
                conn.commit()

            conn.close()

    except Exception as e:
        logger.critical(f"⚠️ خطأ أثناء جمع الأفلام: {e}") 
    finally:
        if browser:
            await browser.close() # تأكد من إغلاق المتصفح
            logger.info("متصفح Playwright تم إغلاقه.")
            
    logger.info(f"✅ تمت إضافة {total_added_count} فيلم جديد في هذه الجولة.") 
    return new_movies

# --- إرسال الأفلام الجديدة للمستخدمين ---
async def send_new_movies(context: ContextTypes.DEFAULT_TYPE):
    # استدعاء الدالة غير المتزامنة لكشط الأفلام
    new_movies = await scrape_movies_async() 
    if not new_movies:
        logger.info("لا توجد أفلام جديدة للإرسال.")
        return

    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    users = c.fetchall()
    conn.close()

    # تجميع الأفلام حسب المصدر
    movies_by_source = {}
    for movie in new_movies:
        if movie['source'] not in movies_by_source:
            movies_by_source[movie['source']] = []
        movies_by_source[movie['source']].append(movie)

    for user_id, in users:
        try:
            message_parts = []
            message_parts.append("🎬 <b>أفلام جديدة متاحة:</b>\n\n")
            
            for source, movies in movies_by_source.items():
                message_parts.append(f"<b>{source}:</b>\n")
                # عرض أول 5 أفلام من كل مصدر
                for movie in movies[:5]: 
                    # دمج رابط الصورة كنص بجانب رابط الفيلم
                    image_link_text = f" (<a href='{movie['image_url']}'>صورة</a>)" if movie.get('image_url') else ""
                    message_parts.append(f"• <a href='{movie['url']}'>{movie['title']}</a>{image_link_text}\n")
                message_parts.append("\n")
            
            final_message = "".join(message_parts)

            await context.bot.send_message(
                chat_id=user_id,
                text=final_message,
                parse_mode='HTML',
                disable_web_page_preview=True # Keep this true to prevent large URL previews
            )
            await asyncio.sleep(0.3) # تأخير بسيط لتجنب حدود معدل Telegram API
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال الأفلام للمستخدم {user_id}: {e}")

# --- أمر بدء البوت ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)

    welcome_msg = (
        f"🎉 مرحباً {user.first_name}!\n"
        "أنا بوت الأفلام الذكي، سأرسل لك أحدث الأفلام تلقائياً من 12 موقع سينمائي شهير.\n\n"
        "📺 <b>المواقع المدعومة:</b>\n"
        "- Wecima, TopCinema, CimaClub\n"
        "- TukTukCima, EgyBest, MyCima\n"
        "- Akoam, Shahid4u, Aflamco\n"
        "- Cima4u, Fushaar, Aflaam\n\n"
        "⏰ سيصلك تحديث بالأفلام الجديدة كل ساعة تلقائياً\n"
        "للحصول على تحديث يدوي، استخدم الأمر /update"
    )
    
    await update.message.reply_text(
        welcome_msg,
        parse_mode='HTML'
    )

# --- أمر فحص حالة البوت ---
async def alive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM movies")
    movies_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users")
    users_count = c.fetchone()[0]
    conn.close()
    
    status_msg = (
        "✅ أنا شغال وقوي!\n\n"
        f"🎥 عدد الأفلام في قاعدة البيانات: <b>{movies_count}</b>\n"
        f"👥 عدد المستخدمين: <b>{users_count}</b>\n"
        "⏱️ آخر تحديث: منذ قليل\n"
        "🔄 التحديث التالي: خلال ساعة"
    )
    
    await update.message.reply_text(
        status_msg,
        parse_mode='HTML'
    )

# --- أمر التحديث اليدوي ---
async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text("⏳ جارٍ البحث عن أفلام جديدة... قد يستغرق هذا بعض الوقت.")
    
    new_movies = await scrape_movies_async() # تم تعديل الاستدعاء ليكون async
    if not new_movies:
        await update.message.reply_text("⚠️ لم يتم العثور على أفلام جديدة في هذه الجولة.")
        return
    
    message_parts = []
    message_parts.append("🎉 <b>تم العثور على أفلام جديدة:</b>\n\n")

    movies_by_source = {}
    for movie in new_movies:
        if movie['source'] not in movies_by_source:
            movies_by_source[movie['source']] = []
        movies_by_source[movie['source']].append(movie)

    for source, movies in movies_by_source.items():
        message_parts.append(f"<b>{source}:</b>\n")
        for movie in movies[:5]: # عرض أول 5 أفلام جديدة من كل مصدر
            image_link_text = f" (<a href='{movie['image_url']}'>صورة</a>)" if movie.get('image_url') else ""
            message_parts.append(f"• <a href='{movie['url']}'>{movie['title']}</a>{image_link_text}\n")
        message_parts.append("\n")
    
    final_message = "".join(message_parts)

    await update.message.reply_text(
        final_message,
        parse_mode='HTML',
        disable_web_page_preview=True
    )

# --- جدولة المهام ---
def schedule_job(application):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def run_async_task_wrapper():
        try:
            # تشغيل الدالة غير المتزامنة send_new_movies
            loop.run_until_complete(send_new_movies(application))
        except Exception as e:
            logger.error(f"خطأ في المهمة المجدولة: {e}")

    schedule.every(1).hours.do(run_async_task_wrapper)
    
    logger.info("بدء عملية جمع الأفلام الأولية...")
    run_async_task_wrapper()  

    while True:
        schedule.run_pending()
        time.sleep(30)

# --- تشغيل البوت ---
def main():
    init_db()
    logger.info("تم تهيئة قاعدة البيانات")

    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("alive", alive))
    application.add_handler(CommandHandler("update", manual_update))

    threading.Thread(target=schedule_job, args=(application,), daemon=True).start()

    logger.info("✅ البوت يعمل الآن مع 12 موقع سينمائي")
    logger.info("⏱️ تحديث الأفلام كل ساعة تلقائياً")
    logger.info("� خادم Keep-Alive يعمل على المنفذ 8080")
    logger.info("🔄 استخدم /update لتحديث يدوي")
    application.run_polling()

if __name__ == '__main__':
    main()

