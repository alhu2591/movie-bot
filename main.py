import subprocess
import sys
import os
import re
import sqlite3
import threading
import time
import asyncio
import requests
from bs4 import BeautifulSoup
from flask import Flask
from urllib.parse import urlparse, urlunparse
import schedule
from datetime import datetime, timedelta
import html
import logging

# Import necessary Telegram types for permanent keyboard
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters


# --- إعداد التسجيل (Logging) ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Aggressive Package Installation and Verification ---
def ensure_packages_installed():
    required_pip_packages = [
        "requests", "beautifulsoup4", "lxml", "python-telegram-bot"
    ]
    
    # Aggressive uninstall to clear any conflicting packages
    logger.info("Attempting aggressive uninstallation of potentially conflicting packages...")
    for pkg in ["telegram", "python-telegram-bot", "requests", "beautifulsoup4", "lxml"]:
        try:
            result = subprocess.run([sys.executable, "-m", "pip", "uninstall", "-y", pkg], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"Uninstalled '{pkg}' (if present).")
            else:
                logger.debug(f"Uninstall of '{pkg}' returned non-zero ({result.returncode}), stderr: {result.stderr.strip()}")
        except Exception as e:
            logger.warning(f"Error during '{pkg}' uninstallation: {e}")

    # Install/reinstall all required packages
    try:
        install_cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall"] + required_pip_packages
        logger.info(f"Running pip install command: {' '.join(install_cmd)}")
        result = subprocess.run(install_cmd, capture_output=True, text=True, check=True)
        logger.info("Required packages installed/reinstalled successfully.")
        logger.debug(f"pip install stdout: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.critical(f"❌ Failed to install Python packages. Error: {e.stderr}")
        logger.critical("Please check your internet connection and Replit environment settings.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Unexpected error during Python package installation: {e}")
        sys.exit(1)

    # After mass installation/reinstallation, specifically verify telegram imports
    logger.info("Verifying critical imports...")
    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
        from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
        logger.info("✅ Core Python-Telegram-Bot imports successful.")
    except ImportError as e:
        logger.critical(f"❌ Critical ImportError after package installation: {e}")
        logger.critical("This usually means 'python-telegram-bot' is not correctly installed or a conflicting 'telegram' package exists.")
        logger.critical("Please try running 'pip uninstall telegram python-telegram-bot' then 'pip install python-telegram-bot' manually in your Replit shell, and restart the Repl.")
        sys.exit(1) # Exit if critical imports fail even after reinstallation

# Call this at the very beginning of the script execution
ensure_packages_installed()


# --- إعدادات البوت ---
TOKEN = "7576844775:AAHdO2WNOetUhty_RlADiTi4QhyNXZnM2Ds" 
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID") # متغير بيئة لـ ID المشرف

# --- إعداد خادم keep_alive ---
app = Flask(__name__)
@app.route('/')
def home():
    return "🎬 بوت الأفلام يعمل بنجاح! | 12 موقع سينمائي | تحديث كل 6 ساعات | Keep-Alive مفعل" # تم تحديث الرسالة لتعكس 6 ساعات و Keep-Alive

def run_flask_app():
    app.run(host='0.0.0.0', port=8080)

# بدء Flask في مؤشر ترابط منفصل
threading.Thread(target=run_flask_app, daemon=True).start()

# --- تهيئة قاعدة البيانات ---
def init_db():
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    # جدول الأفلام
    c.execute('''CREATE TABLE IF NOT EXISTS movies
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL,
                 url TEXT NOT NULL UNIQUE,
                 source TEXT NOT NULL,
                 image_url TEXT,
                 category TEXT,
                 description TEXT,
                 release_year INTEGER,
                 last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # محاولة إضافة الأعمدة إذا لم تكن موجودة
    try:
        c.execute("ALTER TABLE movies ADD COLUMN category TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add category column: {e}")

    try:
        c.execute("ALTER TABLE movies ADD COLUMN image_url TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add image_url column: {e}")
            
    try:
        c.execute("ALTER TABLE movies ADD COLUMN description TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add description column: {e}")
            
    try:
        c.execute("ALTER TABLE movies ADD COLUMN release_year INTEGER")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add release_year column: {e}")
    
    # جدول المستخدمين مع تفضيلات التنبيهات
    c.execute('''CREATE TABLE IF NOT EXISTS users
                (user_id INTEGER PRIMARY KEY,
                 username TEXT,
                 first_name TEXT,
                 last_name TEXT,
                 join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 receive_movies INTEGER DEFAULT 1,    -- 1 for true, 0 for false
                 receive_series INTEGER DEFAULT 1,
                 receive_anime INTEGER DEFAULT 1)''')
    
    # محاولة إضافة أعمدة التفضيلات إذا لم تكن موجودة
    for col in ['receive_movies', 'receive_series', 'receive_anime']:
        try:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 1")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                logger.error(f"Error altering users table to add {col} column: {e}")

    # جدول حالة المواقع
    c.execute('''CREATE TABLE IF NOT EXISTS site_status
                (site_name TEXT PRIMARY KEY,
                 last_scraped TIMESTAMP,
                 status TEXT DEFAULT 'unknown')''') # 'active', 'failed', 'unknown'

    conn.commit()
    conn.close()
    logger.info("تم تهيئة قاعدة البيانات.")

# --- إضافة مستخدم جديد ---
def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, receive_movies, receive_series, receive_anime) VALUES (?, ?, ?, ?, 1, 1, 1)",
              (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()
    logger.info(f"تم إضافة/تحديث المستخدم: {user_id}")

# --- تحديث تفضيلات المستخدم ---
def update_user_preference(user_id, preference_type, value):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    try:
        c.execute(f"UPDATE users SET {preference_type} = ? WHERE user_id = ?", (value, user_id))
        conn.commit()
        logger.info(f"تم تحديث تفضيل {preference_type} للمستخدم {user_id} إلى {value}")
        return True
    except Exception as e:
        logger.error(f"خطأ في تحديث تفضيل المستخدم {user_id} لـ {preference_type}: {e}")
        return False
    finally:
        conn.close()

# --- جلب تفضيلات المستخدم ---
def get_user_preferences(user_id):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT receive_movies, receive_series, receive_anime FROM users WHERE user_id = ?", (user_id,))
    prefs = c.fetchone()
    conn.close()
    if prefs:
        return {"movies": bool(prefs[0]), "series": bool(prefs[1]), "anime": bool(prefs[2])}
    return {"movies": True, "series": True, "anime": True} # تفضيلات افتراضية


# --- تنظيف العناوين ---
def clean_title(title):
    # إزالة الأقواس والكلمات الزائدة مثل "مترجم" و "HD"
    title = re.sub(r'\s*\(\d{4}\)|\s*\[.*?\]|\s*مترجم|\s*اون لاين|\s*online|\s*HD|\s*WEB-DL|\s*BluRay|\s*نسخة مدبلجة', '', title, flags=re.IGNORECASE)
    # إزالة أي أحرف غير الأبجدية الرقمية والمسافات والحروف العربية
    title = re.sub(r'[^\w\s\u0600-\u06FF]+', '', title)
    # استبدال المسافات المتعددة بمسافة واحدة
    title = re.sub(r'\s{2,}', ' ', title)
    return title.strip()

# --- دالة مساعدة لتحديد الفئة ---
def deduce_category(title, url, category_hint=None):
    if category_hint and category_hint != "mixed":
        return category_hint

    title_lower = title.lower()
    url_lower = url.lower()
    
    if "مسلسل" in title_lower or "series" in url_lower or "مسلسلات" in url_lower or "/series" in url_lower or "/tv" in url_lower or "مسلسلات-اجنبي" in url_lower:
        return "مسلسل"
    if "انمي" in title_lower or "anime" in url_lower or "أنمي" in title_lower:
        return "أنمي"
    return "فيلم" # الفئة الافتراضية

# --- دالة مساعدة لاستخراج الوصف المفصل وسنة الإصدار من صفحة الفيلم الفردية باستخدام Requests ---
def extract_detailed_movie_info_requests(movie_url: str, movie_title_for_ref: str = "") -> (str, int | None):
    description = ""
    release_year = None
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        }
        response = requests.get(movie_url, headers=headers, timeout=30)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        try:
            movie_soup = BeautifulSoup(response.content, 'lxml')
        except Exception as bs_e:
            logger.warning(f"LXML parser not available for {movie_url}, falling back to html.parser: {bs_e}")
            movie_soup = BeautifulSoup(response.content, 'html.parser')

        # 1. Try to get description from meta tag
        meta_description = movie_soup.find('meta', attrs={'name': 'description'})
        if meta_description and meta_description.get('content'):
            description = meta_description['content'].strip()
            if len(description) < 100 or "مشاهدة وتحميل" in description:
                description = "" 
        
        # 2. If meta tag failed, try common description selectors
        if not description:
            for selector in [
                "div.story", "div.Description", "div.single-text", "div#plot",
                "p.movie-description", "div.entry-content p", "div.BlockItemFull p",
                "div.post-story p", "div.MovieContent__Details__Story" 
            ]:
                tag = movie_soup.select_one(selector)
                if tag:
                    text = tag.get_text(separator=' ', strip=True) 
                    if len(text) > 50 and len(text) < 5000: 
                        description = text
                        break
        
        # 3. Clean the description from promotional phrases
        if description:
            promo_phrases = [
                r'مشاهدة وتحميل (فيلم|مسلسل|انمي)?\s*', r'مشاهدة (فيلم|مسلسل|انمي)?\s*',
                r'تحميل مباشر\s*', r'تنزيل مباشر\s*', r'حصريا\s*', r'فقط\s*',
                r'اون لاين\s*', r'مباشرة\s*', r'روابط سريعة\s*',
                r'بجودة\s*(?:HD|FHD|4K|720p|1080p|BluRay|WEB-DL|HDRip|DVDRip|BDRip|WEBRip)?\s*',
                r'كامل ومترجم\s*', r'مترجم للعربية\s*', r'مدبلج\s*',
                r'بدون إعلانات\s*', r'شاهد مجانا\s*', r'مجاناً\s*',
                r'جميع حلقات\s*', r'الموسم (?:الاول|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع|العاشر|الأول|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع|العاشر|\d+)\s*',
                r'ايجي بست\s*', r'وي سيما\s*', r'ماي سيما\s*', r'سيما كلوب\s*',
                r'تكتوك سيما\s*', r'اكوام\s*', r'شاهد فور يو\s*', r'افلامكو\s*',
                r'سيما فور يو\s*', r'فوشار\s*', r'افلام\s*', r'موقع [أ-ي\w\s]*?\s*', 
                r'قصة (فيلم|مسلسل|انمي)\s*(?:جديد)?\s*(?:تدور احداث)?\s*(?:حول)?\s*', 
                r'تدور احداث (الفيلم|المسلسل|الانمي)?\s*(?:حول)?\s*',
                r'احداث (الفيلم|المسلسل|الانمي)?\s*(?:حول)?\s*',
                r'ملخص القصة\s*(?:حول)?\s*',
                r'تبدأ الاحداث عندما\s*',
                r'فيلم (جديد|حصري|الأن)?\s*', r'مسلسل (جديد|حصري|الأن)?\s*', r'انمي (جديد|حصري|الأن)?\s*',
                r'أفلام202[0-9]|مسلسلات202[0-9]|أنمي202[0-9]', 
                r'اونلاين'
            ]
            
            for phrase in promo_phrases:
                description = re.sub(phrase, '', description, flags=re.IGNORECASE | re.DOTALL).strip()
            
            description = re.sub(r'^[^\w\s\u0600-\u06FF]+', '', description).strip()
            description = re.sub(r'[^\w\s\u0600-\u06FF]+$', '', description).strip()

            if movie_title_for_ref and description.lower().startswith(movie_title_for_ref.lower()):
                description = description[len(movie_title_for_ref):].strip()
                description = re.sub(r'^[^\w\s\u0600-\u06FF]+', '', description).strip() 

            description = re.sub(r'\s{2,}', ' ', description).strip()
            
            if len(description) > 500: 
                description = description[:497] + "..." 

        # Extract release year from the detail page
        year_tag = movie_soup.find("span", class_="year") # Example for Shahid4u
        if not year_tag:
            year_tag = movie_soup.find("div", class_="MovieInfo__Details__item", string=re.compile(r"سنة الإصدار|Year")) # Generic search
            if year_tag:
                year_match = re.search(r'(\d{4})', year_tag.get_text(strip=True))
                if year_match:
                    try:
                        release_year = int(year_match.group(1))
                    except ValueError:
                        release_year = None
        
        if year_tag and not release_year: # If year_tag exists but regex didn't catch it
            year_match = re.search(r'(\d{4})', year_tag.get_text(strip=True))
            if year_match:
                try:
                    release_year = int(year_match.group(1))
                except ValueError:
                    release_year = None


    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ خطأ في جلب تفاصيل الفيلم من {movie_url} باستخدام requests: {e}")
    except Exception as e:
        logger.warning(f"⚠️ خطأ غير متوقع في جلب تفاصيل الفيلم من {movie_url}: {e}")
    
    return description, release_year


# --- دوال التحليل (Extractors) لكل موقع ---

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
            raw_title = ""
            if title_tag:
                if title_tag.name == 'strong':
                    raw_title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    raw_title = title_tag.get("alt", "N/A")
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Wecima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر" # Provide a default title

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
                logger.debug(f"Wecima: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image" # Placeholder

            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Wecima"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر Wecima: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"TopCinema: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TopCinema: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "TopCinema"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر TopCinema: {e} - Item HTML causing error: {item.prettify()}")
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
            
            raw_title = "عنوان غير متوفر" # Default value
            
            # Attempt 1: Get title from h2 within inner--title
            title_h2_tag = item.select_one(".inner--title h2")
            if title_h2_tag:
                extracted_title = title_h2_tag.get_text(strip=True)
                if extracted_title:
                    raw_title = extracted_title
            
            # Attempt 2: If h2 failed, try img alt attribute
            if raw_title == "عنوان غير متوفر":
                img_tag_for_title = item.select_one("div.Poster img")
                if img_tag_for_title:
                    extracted_title = img_tag_for_title.get("alt", "")
                    if extracted_title:
                        raw_title = extracted_title
            
            # Attempt 3: If img alt failed, try link title attribute
            if raw_title == "عنوان غير متوفر":
                extracted_title = link_tag.get("title", "")
                if extracted_title:
                    raw_title = extracted_title

            if raw_title == "عنوان غير متوفر":
                logger.debug(f"CimaClub: Could not extract title for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر" # Ensure default if all attempts fail

            img_tag = item.select_one("div.Poster img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"CimaClub: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image" # Placeholder
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "CimaClub"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر CimaClub: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"TukTukCima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TukTukCima: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "TukTukCima"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر TukTukCima: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get("alt", "N/A") if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"EgyBest: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            image_url = title_tag.get("data-src") or title_tag.get("src") if title_tag else None
            if not image_url:
                logger.debug(f"EgyBest: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "EgyBest"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر EgyBest: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = ""
            if title_tag:
                if title_tag.name == 'strong':
                    raw_title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    raw_title = title_tag.get("alt", "N/A")
            if not raw_title or raw_title == "N/A":
                logger.debug(f"MyCima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"

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
                logger.debug(f"MyCima: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"

            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "MyCima"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر MyCima: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = ""
            if title_tag:
                if title_tag.name == 'h2':
                    raw_title = title_tag.get_text(strip=True)
                else: # It's an img tag
                    raw_title = title_tag.get("alt", "N/A")
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Akoam: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Akoam: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Akoam"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر Akoam: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Shahid4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("src") if img_tag else None # Shahid4u يستخدم src مباشرة
            if not image_url:
                logger.debug(f"Shahid4u: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Shahid4u"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر Shahid4u: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Aflamco: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Aflamco: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Aflamco"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر Aflamco: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Cima4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Cima4u: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Cima4u"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر Cima4u: {e} - Item HTML causing error: {item.prettify()}")
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Fushaar: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-lazy-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Fushaar: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Fushaar"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Aflaam: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("picture img.lazy") 
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Aflaam: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "Aflaam"})
        except Exception as e:
            logger.error(f"❌ Error parsing Aflaam item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

def parse_egydead(soup):
    movies = []
    for item in soup.select("div.movie-box, div.GridItem, div.Blocks ul.MovieList div.movie-box"):
        try:
            link_tag = item.select_one("a")
            if not link_tag or not link_tag.get("href"):
                logger.debug(f"EgyDead: Skipping item due to missing link or href: {item.prettify()}")
                continue
            link = link_tag["href"]
            
            raw_title = "عنوان غير متوفر"
            title_tag = item.select_one("h2.Title") or item.select_one("strong.hasyear") or item.select_one("img")
            if title_tag:
                if title_tag.name == 'img':
                    raw_title = title_tag.get("alt", "N/A")
                else:
                    raw_title = title_tag.get_text(strip=True)
            
            if not raw_title or raw_title == "N/A":
                logger.debug(f"EgyDead: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"

            image_url = None
            img_tag = item.select_one("img")
            if img_tag:
                image_url = img_tag.get("data-src") or img_tag.get("src")
            if not image_url:
                logger.debug(f"EgyDead: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": clean_title(raw_title), "url": link, "image_url": image_url, "source": "EgyDead"})
        except Exception as e:
            logger.error(f"❌ خطأ في تحليل عنصر EgyDead: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies


# --- قائمة بجميع المواقع المدعومة (12 موقعًا في هذه النسخة) ---
# Function to get the base URL (not used for specific URLs now)
def get_base_url(full_url):
    parsed_url = urlparse(full_url)
    return urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', '')) + "/"

SCRAPERS = [
    {"name": "Wecima", "url": "https://wecima.video", "parser": parse_wecima, "category_hint": "mixed"},
    {"name": "TopCinema", "url": "https://web6.topcinema.cam/recent/", "parser": parse_topcinema, "category_hint": "mixed"},
    {"name": "CimaClub", "url": "https://cimaclub.day", "parser": parse_cimaclub, "category_hint": "mixed"},
    {"name": "TukTukCima", "url": "https://tuktukcima.art/recent/", "parser": parse_tuktukcima, "category_hint": "mixed"},
    {"name": "EgyBest", "url": "https://egy.onl/recent/", "parser": parse_egy_onl, "category_hint": "mixed"}, 
    {"name": "MyCima", "url": "https://mycima.video", "parser": parse_mycima, "category_hint": "mixed"},
    
    # Akoam
    {"name": "Akoam_Movies", "url": "https://akw.onl/movies/", "parser": parse_akoam, "category_hint": "فيلم"},
    {"name": "Akoam_Series", "url": "https://akw.onl/series/", "parser": parse_akoam, "category_hint": "مسلسل"},
    {"name": "Akoam_TV", "url": "https://akw.onl/tv/", "parser": parse_akoam, "category_hint": "مسلسل"}, # Assuming TV is mostly series

    # Shahid4u
    {"name": "Shahid4u_Movies", "url": "https://shahed4uapp.com/page/movies/", "parser": parse_shahid4u, "category_hint": "فيلم"},
    {"name": "Shahid4u_Series", "url": "https://shahed4uapp.com/page/series/", "parser": parse_shahid4u, "category_hint": "مسلسل"},

    # Aflamco
    {"name": "Aflamco_Movies", "url": "https://aflamco.cloud/%D8%A7%D9%81%D9%84%D8%A7%D9%85/", "parser": parse_aflamco, "category_hint": "فيلم"}, # Updated URL, assuming it's for movies

    # Cima4u (new domain and specific categories)
    {"name": "Cima4u_Movies", "url": "https://cema4u.vip/category/%d8%a7%d9%81%d9%84%d8%a7%d9%85-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": parse_cima4u, "category_hint": "فيلم"},
    {"name": "Cima4u_Series", "url": "https://cema4u.vip/category/%d9%85%d8%b3%d9%84%d8%b3%d9%84%d8%a7%d8%aa-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": parse_cima4u, "category_hint": "مسلسل"},

    {"name": "Fushaar", "url": "https://www.fushaar.com/?tlvaz", "parser": parse_fushaar, "category_hint": "mixed"}, # Updated URL

    # Aflaam
    {"name": "Aflaam_Movies", "url": "https://aflaam.com/movies", "parser": parse_aflaam, "category_hint": "فيلم"},
    {"name": "Aflaam_Series", "url": "https://aflaam.com/series", "parser": parse_aflaam, "category_hint": "مسلسل"},

    # New Site: EgyDead
    {"name": "EgyDead_Movies", "url": "https://egydead.video/category/%d8%a7%d9%81%d9%84%d8%a7%d9%85-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": parse_egydead, "category_hint": "فيلم"},
    {"name": "EgyDead_Series", "url": "https://egydead.video/series-category/%d9%85%d8%b3%d9%84%d8%b3%d9%84%d8%a7%d8%aa-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a-1/", "parser": parse_egydead, "category_hint": "مسلسل"},
]

# --- جلب وتحليل محتوى الصفحة الرئيسية للموقع باستخدام requests ---
def scrape_single_main_page_and_parse(scraper: dict):
    site_name = scraper["name"]
    site_url = scraper["url"]
    parser_func = scraper["parser"]
    
    logger.info(f"جارٍ فحص الصفحة الرئيسية لـ: {site_name} - {site_url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        }
        response = requests.get(site_url, headers=headers, timeout=60) # Increased timeout
        response.raise_for_status() # Raise an exception for HTTP errors
        
        try:
            soup = BeautifulSoup(response.content, 'lxml') 
        except Exception as bs_e:
            logger.warning(f"LXML parser not available for {site_name}, falling back to html.parser: {bs_e}")
            soup = BeautifulSoup(response.content, 'html.parser')
        
        movies = parser_func(soup)

        if movies:
            logger.info(f"✅ {len(movies)} فيلم تم استخراجه مبدئياً من {site_name}")
        else:
            logger.warning(f"⚠️ لم يتم العثور على أفلام في {site_name} (الصفحة الرئيسية) باستخدام المحددات الحالية.")
        return movies
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ خطأ أثناء جلب/تحليل الصفحة الرئيسية لـ {site_name} ({site_url}) باستخدام requests: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع أثناء جلب/تحليل الصفحة الرئيسية لـ {site_name} ({site_url}): {e}")
        return []

# --- جمع الأفلام من جميع المواقع وتحديث قاعدة البيانات (UPSERT) ---
def scrape_movies_and_get_new(): # ليست دالة async بعد الآن
    newly_added_movies = [] 
    total_processed_count = 0 
    
    conn = sqlite3.connect('movies.db') 
    c = conn.cursor()

    # Step 1: Scrape main pages to get initial movie links and basic info
    all_initial_movies_flat = []
    for scraper_info in SCRAPERS:
        movies_from_site = scrape_single_main_page_and_parse(scraper_info)
        # Add source and category_hint to each movie for later processing
        for movie in movies_from_site:
            movie['source_name_for_logging'] = scraper_info['name'] # Store original scraper name for logging
            movie['category_hint'] = scraper_info.get('category_hint')
        all_initial_movies_flat.extend(movies_from_site)
        time.sleep(1) # Polite delay between sites

    # Step 2: Process each initial movie, visit its detail page, and add/update in DB
    for movie_initial_data in all_initial_movies_flat:
        total_processed_count += 1
        try:
            cleaned_title_text = clean_title(movie_initial_data["title"])
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Check if movie exists in DB
            c.execute("SELECT id, title, image_url, category, description, release_year FROM movies WHERE url = ?", (movie_initial_data["url"],))
            existing_movie_db = c.fetchone()

            detailed_description = ""
            accurate_release_year = None

            # Only visit detail page if movie is potentially new or needs updating its description/year
            # or if the description/release_year from DB is missing
            if not existing_movie_db or not existing_movie_db[4] or not existing_movie_db[5]: 
                detailed_description, accurate_release_year = extract_detailed_movie_info_requests(
                    movie_initial_data["url"], cleaned_title_text
                )
                time.sleep(0.5) # Polite delay for detail pages

            # Use extracted data or fallback to initial data/existing DB data
            movie_description = detailed_description if detailed_description else (existing_movie_db[4] if existing_movie_db else "")
            movie_release_year = accurate_release_year if accurate_release_year else (existing_movie_db[5] if existing_movie_db else None)
            # If release_year is still None, try to extract from initial title (fallback)
            if not movie_release_year:
                year_match = re.search(r'(\d{4})', movie_initial_data["title"])
                if year_match:
                    try:
                        movie_release_year = int(year_match.group(1))
                    except ValueError:
                        movie_release_year = None

            # Use the category hint from the scraper definition
            category = deduce_category(cleaned_title_text, movie_initial_data["url"], movie_initial_data.get("category_hint"))

            # Update site status for the source of this movie
            # Call update_site_status function
            update_site_status(movie_initial_data["source_name_for_logging"], 'active')

            if existing_movie_db:
                db_id, old_title, old_image_url, old_category, old_description, old_release_year = existing_movie_db

                changed = False
                if old_title != cleaned_title_text: changed = True
                if old_image_url != movie_initial_data.get("image_url"): changed = True
                if old_category != category: changed = True
                if old_description != movie_description: changed = True
                if old_release_year != movie_release_year: changed = True

                if changed:
                    c.execute("""
                        UPDATE movies 
                        SET title = ?, image_url = ?, category = ?, description = ?, release_year = ?, last_updated = ?
                        WHERE url = ?
                    """, (cleaned_title_text, movie_initial_data.get("image_url"), category,
                          movie_description, movie_release_year, current_time_str, movie_initial_data["url"]))
                    logger.info(f"✅ تم تحديث الفيلم: {cleaned_title_text} من {movie_initial_data['source_name_for_logging']}")
            else:
                # Insert new movie
                c.execute("INSERT INTO movies (title, url, source, image_url, category, description, release_year, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                          (cleaned_title_text, movie_initial_data["url"], movie_initial_data["source_name_for_logging"], movie_initial_data.get("image_url"),
                           category, movie_description, movie_release_year, current_time_str))
                newly_added_movies.append({
                    "title": cleaned_title_text,
                    "url": movie_initial_data["url"],
                    "source": movie_initial_data["source_name_for_logging"],
                    "image_url": movie_initial_data.get("image_url"),
                    "category": category,
                    "description": movie_description,
                    "release_year": movie_release_year
                })
                logger.info(f"✨ تم إضافة فيلم جديد: {cleaned_title_text} من {movie_initial_data['source_name_for_logging']}")

        except Exception as e:
            logger.error(f"  ❌ خطأ في معالجة فيلم من {movie_initial_data.get('source_name_for_logging', 'N/A')} ({movie_initial_data.get('title', 'N/A')}): {e}")
        finally:
            conn.commit() 
            # No asyncio.sleep here as this is a synchronous function now

    conn.close()
    logger.info(f"✅ تم معالجة {total_processed_count} فيلم في هذه الجولة. {len(newly_added_movies)} منها جديدة.")
    return newly_added_movies

# --- دالة مساعدة لتحديث حالة الموقع في قاعدة البيانات ---
def update_site_status(site_name, status):
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    c.execute("INSERT OR REPLACE INTO site_status (site_name, last_scraped, status) VALUES (?, ?, ?)",
              (site_name, current_time_str, status))
    conn.commit()
    conn.close()

# --- دالة مساعدة لجلب حالة المواقع من قاعدة البيانات ---
def get_site_statuses():
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT site_name, last_scraped, status FROM site_status")
    statuses = c.fetchall()
    conn.close()
    return statuses

# --- إرسال الأفلام الجديدة للمستخدمين ---
async def send_new_movies(context: ContextTypes.DEFAULT_TYPE): 
    # استخدام asyncio.to_thread لتشغيل دالة الجلب المتزامنة في مؤشر ترابط منفصل
    new_movies_to_send = await asyncio.to_thread(scrape_movies_and_get_new)
    if not new_movies_to_send:
        logger.info("لا توجد أفلام جديدة للإرسال في هذه الجولة.")
        return

    conn = sqlite3.connect('movies.db') 
    c = conn.cursor()
    c.execute("SELECT user_id, receive_movies, receive_series, receive_anime FROM users")
    users_with_prefs = c.fetchall()
    conn.close()

    for user_id, receive_movies, receive_series, receive_anime in users_with_prefs:
        try:
            filtered_movies = []
            for movie in new_movies_to_send: 
                if (movie['category'] == 'فيلم' and receive_movies) or \
                   (movie['category'] == 'مسلسل' and receive_series) or \
                   (movie['category'] == 'أنمي' and receive_anime):
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
                if movie['release_year']:
                    photo_caption_text += f"📅 <b>سنة الإصدار:</b> {movie['release_year']}\n"
                photo_caption_text += (
                    f"🎬 <b>المصدر:</b> {movie['source']}\n"
                    f"🎬 <b>الفئة:</b> {movie['category']}\n"
                )
                
                if movie['description']:
                    description_text = movie['description'].strip()
                    if description_text:
                        photo_caption_text += f"\n📝 <b>الوصف:</b> {description_text}\n"
                
                keyboard = [[InlineKeyboardButton("اضغط هنا للمشاهدة", url=movie["url"])]]
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
                    logger.error(f"❌ خطأ في إرسال الصورة للمستخدم {user_id} للفيلم {movie['title']}: {photo_e}")
                    fallback_text = (
                        f"🎬 <b>العنوان:</b> {escaped_title}\n"
                    )
                    if movie['release_year']:
                        fallback_text += f"📅 <b>سنة الإصدار:</b> {movie['release_year']}\n"
                    fallback_text += (
                        f"🎬 <b>المصدر:</b> {movie['source']}\n"
                        f"🎬 <b>الفئة:</b> {movie['category']}\n"
                    )
                    if movie['description']:
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
            logger.error(f"❌ خطأ في إرسال الأفلام للمستخدم {user_id}: {e}")

# --- Self-Ping function ---
async def self_ping_async():
    try:
        # Pinging the local Flask server to keep the Repl alive
        response = requests.get("http://localhost:8080", timeout=10)
        response.raise_for_status()
        logger.info(f"✅ Self-ping successful! Status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Self-ping failed: {e}")

# --- دالة مساعدة لإرسال رسالة القائمة الرئيسية مع الأزرار الدائمة ---
async def main_menu_internal(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user_first_name: str = "عزيزي المستخدم"):
    keyboard = [
        [KeyboardButton("⚙️ إعدادات التنبيهات")],
        [KeyboardButton("🔄 تحديث الآن")]
    ]
    # استخدام ReplyKeyboardMarkup لجعل الأزرار دائمة
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    welcome_msg = (
        f"🎉 مرحباً {user_first_name}!\n"
        "أنا بوت الأفلام الذكي، سأرسل لك أحدث الأفلام تلقائياً من 12 موقع سينمائي:\n"
        "- Wecima, TopCinema, CimaClub, TukTukCima, EgyBest, MyCima,\n"
        "- Akoam, Shahid4u, Aflamco, Cima4u, Fushaar, Aflaam.\n\n"
        "⏰ سيصلك تحديث بالأفلام الجديدة كل 6 ساعات تلقائياً.\n" 
        "استخدم الأزرار أدناه للتحكم في البوت."
    )
    # إرسال رسالة جديدة مع لوحة المفاتيح الدائمة
    await context.bot.send_message(chat_id=chat_id, text=welcome_msg, parse_mode='HTML', reply_markup=reply_markup)


# --- معالج أمر /settings (يمكن استدعاؤه لتعديل الرسالة أو إرسالها لأول مرة) ---
async def settings_command_internal(chat_id: int, context: ContextTypes.DEFAULT_TYPE, message_id: int = None, edit_mode: bool = False):
    user_prefs = get_user_preferences(chat_id)
    
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
            logger.error(f"خطأ في تعديل رسالة الإعدادات للمستخدم {chat_id} (رسالة {message_id}): {e}")
            await context.bot.send_message(chat_id=chat_id, text=settings_text, parse_mode='HTML', reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id=chat_id, text=settings_text, parse_mode='HTML', reply_markup=reply_markup)

# --- معالجات أوامر البوت ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name) 
    # استدعاء main_menu_internal لإظهار الأزرار الدائمة
    await main_menu_internal(user.id, context, user_first_name=user.first_name)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer() 

    chat_id = query.message.chat_id 

    if query.data == 'settings_command':
        # هذا لن يتم استدعاؤه مباشرة من الأزرار الدائمة، بل من زر "رجوع" في قائمة الإعدادات
        await settings_command_internal(chat_id, context, query.message.message_id, edit_mode=True) 
    elif query.data.startswith('toggle_'):
        pref_type = query.data.replace('toggle_', '')
        current_prefs = get_user_preferences(chat_id)
        new_value = 0 if current_prefs.get(pref_type) else 1
        
        if update_user_preference(chat_id, f"receive_{pref_type}", new_value):
            await settings_command_internal(chat_id, context, query.message.message_id, edit_mode=True)
        else:
            await context.bot.send_message(chat_id=chat_id, text="حدث خطأ أثناء تحديث التفضيلات.")
    elif query.data == 'back_to_main_menu':
        user = update.effective_user
        # عند العودة من الإعدادات، نعرض القائمة الرئيسية بالأزرار الدائمة
        await main_menu_internal(chat_id, context, user_first_name=user.first_name)
        # نحذف رسالة الإعدادات القديمة لتنظيف الدردشة
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete settings message: {e}")

# --- معالج الزر الدائم "إعدادات التنبيهات" ---
async def handle_settings_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    # إرسال رسالة الإعدادات الجديدة (مع الأزرار الداخلية)
    await settings_command_internal(chat_id, context)

# --- معالج الزر الدائم "تحديث الآن" ---
async def handle_manual_update_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await update.message.reply_text("بدء تحديث الأفلام يدوياً... قد يستغرق الأمر بضع دقائق.", parse_mode='HTML')
    try:
        await send_new_movies(context)
        await update.message.reply_text("✅ تم الانتهاء من التحديث اليدوي للأفلام.", parse_mode='HTML')
    except Exception as e:
        logger.exception("خطأ فادح أثناء التحديث اليدوي للأفلام.")
        error_message_for_user = f"❌ حدث خطأ أثناء التحديث اليدوي للأفلام. التفاصيل: <code>{html.escape(str(e))[:150]}...</code>"
        await update.message.reply_text(error_message_for_user, parse_mode='HTML')
        if ADMIN_CHAT_ID:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=f"⚠️ خطأ في التحديث اليدوي لبوت الأفلام: \n<code>{html.escape(str(e))}</code>",
                    parse_mode='HTML'
                )
            except Exception as admin_e:
                    logger.error(f"فشل إرسال رسالة الخطأ إلى المشرف: {admin_e}")


async def show_site_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_CHAT_ID:
        await update.message.reply_text("ليس لديك الصلاحية لاستخدام هذا الأمر.")
        return

    statuses = get_site_statuses()
    message = "📊 <b>حالة المواقع:</b>\n\n"
    if not statuses:
        message += "لا توجد بيانات حالة للمواقع بعد."
    else:
        for site_name, last_scraped, status in statuses:
            if isinstance(last_scraped, str):
                try:
                    last_scraped_dt = datetime.strptime(last_scraped, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    last_scraped_dt = datetime.strptime(last_scraped.split('.')[0], '%Y-%m-%d %H:%M:%S') 
            else:
                last_scraped_dt = last_scraped

            last_scraped_str = last_scraped_dt.strftime('%Y-%m-%d %H:%M')
            message += f"<b>{site_name}</b>: آخر جلب: {last_scraped_str}, الحالة: {status}\n"
    
    await update.message.reply_text(message, parse_mode='HTML')

# --- دالة لتنظيف الأفلام القديمة من قاعدة البيانات ---
def cleanup_old_movies():
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    
    # حذف الأفلام الأقدم من 90 يوماً
    ninety_days_ago = datetime.now() - timedelta(days=90)
    c.execute("DELETE FROM movies WHERE last_updated < ?", (ninety_days_ago,))
    deleted_count = c.rowcount
    
    # تنفيذ VACUUM لإعادة استصلاح المساحة بعد الحذف
    try:
        c.execute("VACUUM")
        logger.info("✅ تم تنفيذ VACUUM على قاعدة البيانات بنجاح.")
    except Exception as e:
        logger.error(f"❌ خطأ أثناء تنفيذ VACUUM على قاعدة البيانات: {e}")

    conn.commit()
    conn.close()
    logger.info(f"✅ تم حذف {deleted_count} فيلمًا قديمًا من قاعدة البيانات.")


# --- مهمة الجدولة ---
def schedule_job(application):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run_async_task_wrapper_send_new_movies():
        try:
            await send_new_movies(application) 
        except Exception as e:
            logger.error(f"خطأ في مهمة إرسال الأفلام الجديدة المجدولة: {e}")

    async def run_async_task_wrapper_self_ping():
        try:
            await self_ping_async()
        except Exception as e:
            logger.error(f"خطأ في مهمة Self-Ping المجدولة: {e}")

    # جدولة مهمة جمع وإرسال الأفلام الجديدة كل 6 ساعات.
    schedule.every(6).hours.do(lambda: asyncio.run_coroutine_threadsafe(run_async_task_wrapper_send_new_movies(), loop))
    
    # جدولة تنظيف قاعدة البيانات يومياً في وقت معين (مثلاً 3 صباحاً)
    schedule.every().day.at("03:00").do(cleanup_old_movies) 

    # جدولة Self-Ping كل 5 دقائق
    schedule.every(5).minutes.do(lambda: asyncio.run_coroutine_threadsafe(run_async_task_wrapper_self_ping(), loop))

    logger.info("بدء عملية جمع الأفلام الأولية...")
    # تشغيل أولي لجمع الأفلام عند بدء البوت
    asyncio.run_coroutine_threadsafe(run_async_task_wrapper_send_new_movies(), loop)
    # تشغيل أولي لـ Self-Ping عند بدء البوت
    asyncio.run_coroutine_threadsafe(run_async_task_wrapper_self_ping(), loop)

    while True:
        schedule.run_pending()
        time.sleep(30) 

# --- تنفيذ البوت الرئيسي ---
def main():
    init_db() 
    
    global application
    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("sitestatus", show_site_status)) # Admin command
    application.add_handler(CallbackQueryHandler(button_callback_handler)) # For inline buttons (settings menu)

    # New handlers for permanent keyboard buttons
    application.add_handler(MessageHandler(filters.Regex(r"^⚙️ إعدادات التنبيهات$"), handle_settings_button))
    application.add_handler(MessageHandler(filters.Regex(r"^🔄 تحديث الآن$"), handle_manual_update_button))


    threading.Thread(target=schedule_job, args=(application,), daemon=True).start()

    logger.info("✅ بوت الأفلام يعمل الآن مع 12 موقع سينمائي (مدعوم بـ Requests و BeautifulSoup).") 
    logger.info("⏱️ تحديث الأفلام كل 6 ساعات تلقائياً وخيار التحديث اليدوي متاح.") 
    logger.info("🌐 خادم Keep-Alive يعمل على المنفذ 8080.")
    logger.info("⚙️ استخدم الأوامر مثل /start و /settings للمستخدمين و /sitestatus للمشرف.") 
    application.run_polling()

if __name__ == '__main__':
    main()
