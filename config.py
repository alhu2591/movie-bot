import os

# --- Bot Settings ---
# It's highly recommended to use environment variables for sensitive data like tokens.
# If BOT_TOKEN is not set as an environment variable, the default value is used (for testing/local development).
BOT_TOKEN = os.getenv("BOT_TOKEN", "7576844775:AAE8pDuHLQOz3HVOUoxIv3a_e685Ic2VZH4") 
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID") # Set this env variable for admin features (e.g., '123456789')

# --- Scraping and Scheduling Settings ---
SCRAPE_INTERVAL_HOURS = 6 # How often to scrape for new movies (in hours)
SELF_PING_INTERVAL_MINUTES = 5 # How often to ping the Flask server to keep the service alive (in minutes)
DB_CLEANUP_TIME = "03:00" # Time of day for database cleanup (HH:MM format, 24-hour)
MOVIE_RETENTION_DAYS = 90 # How many days to keep movie records in the database

# --- Cache Settings (for scraped data to reduce redundant requests) ---
# Cache expiry time for scraped main page data (in seconds)
# This helps reduce repeated requests to websites within the scrape interval
CACHE_EXPIRY_SECONDS = SCRAPE_INTERVAL_HOURS * 3600 
