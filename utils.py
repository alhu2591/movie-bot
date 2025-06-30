import re
import aiohttp
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def clean_title(title: str) -> str:
    """
    Cleans movie titles by removing common extraneous information and special characters.
    Handles Arabic and English characters.
    """
    # Remove years in parentheses, brackets, and common promotional/quality phrases
    title = re.sub(r'\s*\(\d{4}\)|\s*\[.*?\]|\s*مترجم|\s*اون لاين|\s*online|\s*HD|\s*WEB-DL|\s*BluRay|\s*نسخة مدبلجة|\s*كامل|\s*جودة عالية|\s*كاملة|\s*مباشر|\s*مشاهدة|\s*تحميل|\s*سيرفرات|\s*سيرفر|\s*فيلم|\s*مسلسل|\s*انمي', '', title, flags=re.IGNORECASE)
    # Remove any non-alphanumeric, non-space, non-Arabic characters
    title = re.sub(r'[^\w\s\u0600-\u06FF]+', '', title)
    # Replace multiple spaces with a single space
    title = re.sub(r'\s{2,}', ' ', title)
    return title.strip()

def deduce_category(title: str, url: str, category_hint: str = None) -> str:
    """
    Deduces the category (فيلم, مسلسل, أنمي) based on title, URL, and an optional hint.
    """
    if category_hint and category_hint != "mixed":
        return category_hint

    title_lower = title.lower()
    url_lower = url.lower()
    
    # Prioritize series keywords
    if "مسلسل" in title_lower or "series" in url_lower or "مسلسلات" in url_lower or "/series" in url_lower or "/tv" in url_lower or "مسلسلات-اجنبي" in url_lower:
        return "مسلسل"
    # Then anime keywords
    if "انمي" in title_lower or "anime" in url_lower or "أنمي" in title_lower:
        return "أنمي"
    # Default to movie
    return "فيلم"

async def validate_url_async(session: aiohttp.ClientSession, url: str) -> bool:
    """
    Asynchronously checks if a URL is valid and returns a 200 OK status.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        }
        async with session.head(url, headers=headers, timeout=10) as response:
            return response.status == 200
    except aiohttp.ClientError as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error during URL validation for {url}: {e}")
        return False

def get_base_url(full_url: str) -> str:
    """Extracts the base URL (scheme + netloc) from a full URL."""
    parsed_url = urlparse(full_url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}/"
