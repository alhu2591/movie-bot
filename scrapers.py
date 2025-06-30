import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import logging
from utils import clean_title, deduce_category, validate_url_async, get_base_url
from db_manager import upsert_movie, update_site_status

logger = logging.getLogger(__name__)

# --- List of all supported scraping sites ---
SCRAPERS = [
    {"name": "Wecima", "url": "https://wecima.video", "parser": "parse_wecima", "category_hint": "mixed"},
    {"name": "TopCinema", "url": "https://web6.topcinema.cam/recent/", "parser": "parse_topcinema", "category_hint": "mixed"},
    {"name": "CimaClub", "url": "https://cimaclub.day", "parser": "parse_cimaclub", "category_hint": "mixed"},
    {"name": "TukTukCima", "url": "https://tuktukcima.art/recent/", "parser": "parse_tuktukcima", "category_hint": "mixed"},
    {"name": "EgyBest", "url": "https://egy.onl/recent/", "parser": "parse_egy_onl", "category_hint": "mixed"}, 
    {"name": "MyCima", "url": "https://mycima.video", "parser": "parse_mycima", "category_hint": "mixed"},
    
    # Akoam
    {"name": "Akoam_Movies", "url": "https://akw.onl/movies/", "parser": "parse_akoam", "category_hint": "فيلم"},
    {"name": "Akoam_Series", "url": "https://akw.onl/series/", "parser": "parse_akoam", "category_hint": "مسلسل"},
    {"name": "Akoam_TV", "url": "https://akw.onl/tv/", "parser": "parse_akoam", "category_hint": "مسلسل"}, # Assuming TV is mostly series

    # Shahid4u
    {"name": "Shahid4u_Movies", "url": "https://shahed4uapp.com/page/movies/", "parser": "parse_shahid4u", "category_hint": "فيلم"},
    {"name": "Shahid4u_Series", "url": "https://shahed4uapp.com/page/series/", "parser": "parse_shahid4u", "category_hint": "مسلسل"},

    # Aflamco
    {"name": "Aflamco_Movies", "url": "https://aflamco.cloud/%D8%A7%D9%81%D9%84%D8%A7%D9%85/", "parser": "parse_aflamco", "category_hint": "فيلم"},

    # Cima4u (new domain and specific categories)
    {"name": "Cima4u_Movies", "url": "https://cema4u.vip/category/%d8%a7%d9%81%d9%84%d8%a7%d9%81-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": "parse_cima4u", "category_hint": "فيلم"},
    {"name": "Cima4u_Series", "url": "https://cema4u.vip/category/%d9%85%d8%b3%d9%84%d8%b3%d9%84%d8%a7%d8%aa-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": "parse_cima4u", "category_hint": "مسلسل"},

    {"name": "Fushaar", "url": "https://www.fushaar.com/?tlvaz", "parser": "parse_fushaar", "category_hint": "mixed"},

    # Aflaam
    {"name": "Aflaam_Movies", "url": "https://aflaam.com/movies", "parser": "parse_aflaam", "category_hint": "فيلم"},
    {"name": "Aflaam_Series", "url": "https://aflaam.com/series", "parser": "parse_aflaam", "category_hint": "مسلسل"},

    # New Site: EgyDead
    {"name": "EgyDead_Movies", "url": "https://egydead.video/category/%d8%a7%d9%81%d9%84%d8%a7%d9%81-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a/", "parser": "parse_egydead", "category_hint": "فيلم"},
    {"name": "EgyDead_Series", "url": "https://egydead.video/series-category/%d9%85%d8%b3%d9%84%d8%b3%d9%84%d8%a7%d8%aa-%d8%a7%d8%ac%d9%86%d8%a8%d9%8a-1/", "parser": "parse_egydead", "category_hint": "مسلسل"},
]

# --- Helper function to extract detailed movie info using AIOHTTP ---
async def extract_detailed_movie_info_async(session: aiohttp.ClientSession, movie_url: str, movie_title_for_ref: str = "") -> (str, int | None, str):
    """
    Extracts detailed description, release year, and genres from a movie's detail page.
    """
    description = ""
    release_year = None
    genres = "" # New: to store comma-separated genres
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        }
        async with session.get(movie_url, headers=headers, timeout=30) as response:
            response.raise_for_status()
            content = await response.text()
        
        try:
            movie_soup = BeautifulSoup(content, 'lxml')
        except Exception as bs_e:
            logger.warning(f"LXML parser not available for {movie_url}, falling back to html.parser: {bs_e}")
            movie_soup = BeautifulSoup(content, 'html.parser')

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
                "div.post-story p", "div.MovieContent__Details__Story",
                "div.MovieInfo__Details__Story" # Added for Shahid4u like sites
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

        # Extract genres (new)
        genre_selectors = [
            "a[href*='genre']", # Common for genre links
            "div.MovieInfo__Details__item strong:contains('النوع') + span a", # Example for Shahid4u
            "div.info-list a[href*='genre']", # Another common pattern
            "div.category-list a" # Generic category list
        ]
        extracted_genres = []
        for selector in genre_selectors:
            genre_tags = movie_soup.select(selector)
            for tag in genre_tags:
                genre_text = tag.get_text(strip=True)
                if genre_text and len(genre_text) < 50: # Avoid very long or irrelevant text
                    extracted_genres.append(genre_text)
            if extracted_genres:
                break # Stop after finding genres from the first working selector
        
        if extracted_genres:
            genres = ", ".join(sorted(list(set(extracted_genres)))) # Unique and sorted

    except aiohttp.ClientError as e:
        logger.warning(f"⚠️ Error fetching movie details from {movie_url} using aiohttp: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Unexpected error fetching movie details from {movie_url}: {e}")
    
    return description, release_year, genres


# --- Parser Functions for each site ---
# (These functions remain largely the same, but now return raw_title for better detail extraction)

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

            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Wecima"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"TopCinema: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TopCinema: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "TopCinema"})
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
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "CimaClub"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"TukTukCima: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"TukTukCima: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "TukTukCima"})
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
            
            # Title is in img alt attribute
            title_tag = item.select_one("img")
            raw_title = title_tag.get("alt", "N/A") if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"EgyBest: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            image_url = title_tag.get("data-src") or title_tag.get("src") if title_tag else None
            if not image_url:
                logger.debug(f"EgyBest: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "EgyBest"})
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

            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "MyCima"})
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
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Akoam"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Shahid4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("src") if img_tag else None # Shahid4u uses src directly
            if not image_url:
                logger.debug(f"Shahid4u: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Shahid4u"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Aflamco: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Aflamco: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Aflamco"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Cima4u: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Cima4u: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Cima4u"})
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
            raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
            if not raw_title or raw_title == "N/A":
                logger.debug(f"Fushaar: Title not found or N/A for link {link} - Item HTML: {item.prettify()}")
                raw_title = "عنوان غير متوفر"
            
            img_tag = item.select_one("img")
            image_url = img_tag.get("data-lazy-src") or img_tag.get("src") if img_tag else None
            if not image_url:
                logger.debug(f"Fushaar: Image URL not found for title '{raw_title}' (link: {link}) - Item HTML: {item.prettify()}")
                image_url = "https://placehold.co/200x300/cccccc/333333?text=No+Image"
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Fushaar"})
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
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "Aflaam"})
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
            
            movies.append({"title": raw_title, "url": link, "image_url": image_url, "source": "EgyDead"})
        except Exception as e:
            logger.error(f"❌ Error parsing EgyDead item: {e} - Item HTML causing error: {item.prettify()}")
            continue
    return movies

# --- Main scraping logic ---
async def scrape_single_main_page_and_parse(session: aiohttp.ClientSession, scraper_info: dict):
    """
    Fetches and parses the main page of a single site using aiohttp.
    Returns a list of dictionaries with initial movie data.
    """
    site_name = scraper_info["name"]
    site_url = scraper_info["url"]
    parser_func_name = scraper_info["parser"]
    
    # Get the parser function by name from the current module
    parser_func = globals().get(parser_func_name)
    if not parser_func:
        logger.error(f"Parser function '{parser_func_name}' not found for {site_name}.")
        update_site_status(site_name, 'failed', f"Parser function '{parser_func_name}' not found.")
        return []

    logger.info(f"Scanning main page for: {site_name} - {site_url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        }
        async with session.get(site_url, headers=headers, timeout=60) as response:
            response.raise_for_status()
            content = await response.text()
        
        try:
            soup = BeautifulSoup(content, 'lxml') 
        except Exception as bs_e:
            logger.warning(f"LXML parser not available for {site_name}, falling back to html.parser: {bs_e}")
            soup = BeautifulSoup(content, 'html.parser')
        
        movies = parser_func(soup)

        if movies:
            logger.info(f"✅ {len(movies)} initial movies extracted from {site_name}")
        else:
            logger.warning(f"⚠️ No movies found on {site_name} (main page) with current selectors.")
            update_site_status(site_name, 'failed', f"No movies found on main page.")
        return movies
    except aiohttp.ClientError as e:
        error_msg = f"HTTP/Client error fetching/parsing main page for {site_name} ({site_url}): {e}"
        logger.error(f"❌ {error_msg}")
        update_site_status(site_name, 'failed', error_msg)
        return []
    except Exception as e:
        error_msg = f"Unexpected error fetching/parsing main page for {site_name} ({site_url}): {e}"
        logger.error(f"❌ {error_msg}")
        update_site_status(site_name, 'failed', error_msg)
        return []

async def scrape_movies_and_get_new() -> list:
    """
    Orchestrates scraping from all sites, fetches detailed info, and updates the database.
    Returns a list of newly added movies.
    """
    newly_added_movies = [] 
    total_processed_count = 0 
    
    async with aiohttp.ClientSession() as session:
        # Step 1: Scrape main pages concurrently to get initial movie links and basic info
        scrape_tasks = []
        for scraper_info in SCRAPERS:
            scrape_tasks.append(scrape_single_main_page_and_parse(session, scraper_info))
        
        all_initial_movies_results = await asyncio.gather(*scrape_tasks)

        all_initial_movies_flat = []
        for scraper_idx, movies_from_site in enumerate(all_initial_movies_results):
            scraper_info = SCRAPERS[scraper_idx]
            if movies_from_site:
                for movie in movies_from_site:
                    movie['source_name_for_logging'] = scraper_info['name']
                    movie['category_hint'] = scraper_info.get('category_hint')
                all_initial_movies_flat.extend(movies_from_site)
            else:
                # Site status already updated in scrape_single_main_page_and_parse if failed
                pass

        # Step 2: Process each initial movie, visit its detail page, and add/update in DB
        # This part processes sequentially with a small delay for politeness.
        for movie_initial_data in all_initial_movies_flat:
            total_processed_count += 1
            try:
                # Validate URL before proceeding to detail page scraping
                if not await validate_url_async(session, movie_initial_data["url"]):
                    logger.warning(f"Skipping invalid or unreachable URL: {movie_initial_data['url']}")
                    update_site_status(movie_initial_data["source_name_for_logging"], 'failed', f"Invalid URL: {movie_initial_data['url']}")
                    continue

                cleaned_title_text = clean_title(movie_initial_data["title"])

                detailed_description, accurate_release_year, genres = await extract_detailed_movie_info_async(
                    session, movie_initial_data["url"], cleaned_title_text
                )
                await asyncio.sleep(0.5) # Polite delay for detail pages

                # Use extracted data or fallback to initial data
                movie_description = detailed_description if detailed_description else ""
                movie_release_year = accurate_release_year
                if not movie_release_year: # Fallback to year in title if not found on detail page
                    year_match = re.search(r'(\d{4})', movie_initial_data["title"])
                    if year_match:
                        try:
                            movie_release_year = int(year_match.group(1))
                        except ValueError:
                            movie_release_year = None
                
                # Use the category hint from the scraper definition or deduce from title/URL
                category = deduce_category(cleaned_title_text, movie_initial_data["url"], movie_initial_data.get("category_hint"))

                movie_data_for_db = {
                    "title": cleaned_title_text,
                    "url": movie_initial_data["url"],
                    "source": movie_initial_data["source_name_for_logging"],
                    "image_url": movie_initial_data.get("image_url"),
                    "category": category,
                    "description": movie_description,
                    "release_year": movie_release_year,
                    "genres": genres # New: Add genres
                }

                is_new = upsert_movie(movie_data_for_db)
                if is_new:
                    newly_added_movies.append(movie_data_for_db)
                
                update_site_status(movie_initial_data["source_name_for_logging"], 'active', None)

            except Exception as e:
                logger.error(f"  ❌ Error processing movie from {movie_initial_data.get('source_name_for_logging', 'N/A')} ({movie_initial_data.get('title', 'N/A')}): {e}")
                update_site_status(movie_initial_data["source_name_for_logging'], 'failed', str(e))

    logger.info(f"✅ Processed {total_processed_count} movies in this round. {len(newly_added_movies)} are new.")
    return newly_added_movies
