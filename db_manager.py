import sqlite3
from datetime import datetime, timedelta
import logging
import config # New: Import configuration settings

logger = logging.getLogger(__name__)

def init_db():
    """
    Initializes the SQLite database, creating tables and adding indexes if they don't exist.
    Tables: movies, users, site_status, favorites.
    """
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()

    # --- Movies Table ---
    c.execute('''CREATE TABLE IF NOT EXISTS movies
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL,
                 url TEXT NOT NULL UNIQUE,
                 source TEXT NOT NULL,
                 image_url TEXT,
                 category TEXT,
                 description TEXT,
                 release_year INTEGER,
                 average_rating REAL DEFAULT 0.0,
                 rating_count INTEGER DEFAULT 0,
                 genres TEXT, -- New: to store comma-separated genres
                 last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Add new columns if they don't exist (for schema evolution)
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

    try:
        c.execute("ALTER TABLE movies ADD COLUMN average_rating REAL DEFAULT 0.0")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add average_rating column: {e}")
            
    try:
        c.execute("ALTER TABLE movies ADD COLUMN rating_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add rating_count column: {e}")
    
    try:
        c.execute("ALTER TABLE movies ADD COLUMN genres TEXT") # New: genres column
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering movies table to add genres column: {e}")

    # Create indexes for faster lookups on common query columns
    c.execute("CREATE INDEX IF NOT EXISTS idx_movies_url ON movies (url)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_movies_category ON movies (category)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_movies_last_updated ON movies (last_updated)")

    # --- Users Table ---
    c.execute('''CREATE TABLE IF NOT EXISTS users
                (user_id INTEGER PRIMARY KEY,
                 username TEXT,
                 first_name TEXT,
                 last_name TEXT,
                 join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 receive_movies INTEGER DEFAULT 1,
                 receive_series INTEGER DEFAULT 1,
                 receive_anime INTEGER DEFAULT 1)''')
    
    # Add preference columns if they don't exist
    for col in ['receive_movies', 'receive_series', 'receive_anime']:
        try:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 1")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                logger.error(f"Error altering users table to add {col} column: {e}")

    # --- Site Status Table ---
    c.execute('''CREATE TABLE IF NOT EXISTS site_status
                (site_name TEXT PRIMARY KEY,
                 last_scraped TIMESTAMP,
                 status TEXT DEFAULT 'unknown',
                 last_error TEXT)''')
    
    # Add last_error column if it doesn't exist
    try:
        c.execute("ALTER TABLE site_status ADD COLUMN last_error TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            logger.error(f"Error altering site_status table to add last_error column: {e}")

    # --- Favorites Table (New) ---
    c.execute('''CREATE TABLE IF NOT EXISTS favorites
                (user_id INTEGER NOT NULL,
                 movie_url TEXT NOT NULL,
                 added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 PRIMARY KEY (user_id, movie_url),
                 FOREIGN KEY (user_id) REFERENCES users(user_id),
                 FOREIGN KEY (movie_url) REFERENCES movies(url))''')
    
    # Create index for faster lookup of favorites by user
    c.execute("CREATE INDEX IF NOT EXISTS idx_favorites_user_id ON favorites (user_id)")


    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")

def add_user(user_id: int, username: str, first_name: str, last_name: str):
    """Adds a new user or updates an existing user's details."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, receive_movies, receive_series, receive_anime) VALUES (?, ?, ?, ?, 1, 1, 1)",
              (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()
    logger.info(f"User added/updated: {user_id}")

def update_user_preference(user_id: int, preference_type: str, value: int) -> bool:
    """Updates a specific preference (e.g., receive_movies) for a user."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    try:
        c.execute(f"UPDATE users SET {preference_type} = ? WHERE user_id = ?", (value, user_id))
        conn.commit()
        logger.info(f"Updated preference {preference_type} for user {user_id} to {value}")
        return True
    except Exception as e:
        logger.error(f"Error updating user preference {user_id} for {preference_type}: {e}")
        return False
    finally:
        conn.close()

def get_user_preferences(user_id: int) -> dict:
    """Retrieves notification preferences for a given user."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT receive_movies, receive_series, receive_anime FROM users WHERE user_id = ?", (user_id,))
    prefs = c.fetchone()
    conn.close()
    if prefs:
        return {"movies": bool(prefs[0]), "series": bool(prefs[1]), "anime": bool(prefs[2])}
    return {"movies": True, "series": True, "anime": True} # Default preferences

def update_site_status(site_name: str, status: str, error_message: str = None):
    """Updates the scraping status and last error for a given site."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    c.execute("INSERT OR REPLACE INTO site_status (site_name, last_scraped, status, last_error) VALUES (?, ?, ?, ?)",
              (site_name, current_time_str, status, error_message))
    conn.commit()
    conn.close()

def get_site_statuses() -> list:
    """Retrieves the scraping status for all sites."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT site_name, last_scraped, status, last_error FROM site_status")
    statuses = c.fetchall()
    conn.close()
    return statuses

def cleanup_old_movies():
    """Deletes movies older than MOVIE_RETENTION_DAYS from the database and vacuums."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    
    retention_date = datetime.now() - timedelta(days=config.MOVIE_RETENTION_DAYS)
    c.execute("DELETE FROM movies WHERE last_updated < ?", (retention_date,))
    deleted_count = c.rowcount
    
    try:
        c.execute("VACUUM")
        logger.info("VACUUM executed successfully on the database.")
    except Exception as e:
        logger.error(f"Error during VACUUM: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Deleted {deleted_count} old movies from the database.")

def get_movies_for_search(query_text: str, limit: int = 5) -> list:
    """Searches for movies by title or description."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    search_pattern = f"%{query_text}%"
    c.execute("""
        SELECT title, url, source, image_url, category, description, release_year, average_rating
        FROM movies
        WHERE title LIKE ? OR description LIKE ? OR genres LIKE ?
        ORDER BY last_updated DESC
        LIMIT ?
    """, (search_pattern, search_pattern, search_pattern, limit)) # Added genres to search
    results = c.fetchall()
    conn.close()
    return results

def get_all_users_with_preferences() -> list:
    """Retrieves all users with their notification preferences."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("SELECT user_id, receive_movies, receive_series, receive_anime FROM users")
    users_with_prefs = c.fetchall()
    conn.close()
    return users_with_prefs

def upsert_movie(movie_data: dict) -> bool:
    """Inserts or updates a movie record in the database. Returns True if newly added, False if updated/exists."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    c.execute("SELECT id, title, image_url, category, description, release_year, genres FROM movies WHERE url = ?", (movie_data["url"],))
    existing_movie_db = c.fetchone()

    if existing_movie_db:
        db_id, old_title, old_image_url, old_category, old_description, old_release_year, old_genres = existing_movie_db

        changed = False
        if old_title != movie_data["title"]: changed = True
        if old_image_url != movie_data.get("image_url"): changed = True
        if old_category != movie_data.get("category"): changed = True
        if old_description != movie_data.get("description"): changed = True
        if old_release_year != movie_data.get("release_year"): changed = True
        if old_genres != movie_data.get("genres"): changed = True # New: check genres

        if changed:
            c.execute("""
                UPDATE movies 
                SET title = ?, image_url = ?, category = ?, description = ?, release_year = ?, genres = ?, last_updated = ?
                WHERE url = ?
            """, (movie_data["title"], movie_data.get("image_url"), movie_data.get("category"),
                  movie_data.get("description"), movie_data.get("release_year"), movie_data.get("genres"), current_time_str, movie_data["url"]))
            conn.commit()
            logger.info(f"Updated movie: {movie_data['title']} from {movie_data['source']}")
            return False # Not newly added
    else:
        c.execute("INSERT INTO movies (title, url, source, image_url, category, description, release_year, genres, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (movie_data["title"], movie_data["url"], movie_data["source"], movie_data.get("image_url"),
                   movie_data.get("category"), movie_data.get("description"), movie_data.get("release_year"), movie_data.get("genres"), current_time_str))
        conn.commit()
        logger.info(f"Added new movie: {movie_data['title']} from {movie_data['source']}")
        return True # Newly added

def add_movie_rating(movie_url: str, rating: int) -> bool:
    """
    Adds a new rating for a movie and updates its average_rating and rating_count.
    Returns True if successful, False otherwise.
    """
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    try:
        c.execute("SELECT average_rating, rating_count FROM movies WHERE url = ?", (movie_url,))
        result = c.fetchone()

        if result:
            current_avg_rating, current_rating_count = result
            new_rating_count = current_rating_count + 1
            new_total_sum = (current_avg_rating * current_rating_count) + rating
            new_average_rating = new_total_sum / new_rating_count

            c.execute("""
                UPDATE movies
                SET average_rating = ?, rating_count = ?
                WHERE url = ?
            """, (new_average_rating, new_rating_count, movie_url))
            conn.commit()
            logger.info(f"Updated rating for {movie_url}: New avg={new_average_rating:.2f}, count={new_rating_count}")
            return True
        else:
            logger.warning(f"Attempted to rate non-existent movie: {movie_url}")
            return False
    except Exception as e:
        logger.error(f"Error adding movie rating for {movie_url}: {e}")
        return False
    finally:
        conn.close()

def get_movie_by_url(url: str) -> dict | None:
    """Retrieves a single movie's details by its URL."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("""
        SELECT title, url, source, image_url, category, description, release_year, average_rating, rating_count, genres
        FROM movies
        WHERE url = ?
    """, (url,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "title": row[0],
            "url": row[1],
            "source": row[2],
            "image_url": row[3],
            "category": row[4],
            "description": row[5],
            "release_year": row[6],
            "average_rating": row[7],
            "rating_count": row[8],
            "genres": row[9] # New: include genres
        }
    return None

def add_favorite(user_id: int, movie_url: str) -> bool:
    """Adds a movie to a user's favorites. Returns True if added, False if already exists."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO favorites (user_id, movie_url) VALUES (?, ?)", (user_id, movie_url))
        conn.commit()
        logger.info(f"User {user_id} added {movie_url} to favorites.")
        return True
    except sqlite3.IntegrityError:
        logger.info(f"User {user_id} already has {movie_url} in favorites.")
        return False
    except Exception as e:
        logger.error(f"Error adding favorite for user {user_id}, movie {movie_url}: {e}")
        return False
    finally:
        conn.close()

def remove_favorite(user_id: int, movie_url: str) -> bool:
    """Removes a movie from a user's favorites. Returns True if removed, False if not found."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    try:
        c.execute("DELETE FROM favorites WHERE user_id = ? AND movie_url = ?", (user_id, movie_url))
        conn.commit()
        if c.rowcount > 0:
            logger.info(f"User {user_id} removed {movie_url} from favorites.")
            return True
        else:
            logger.info(f"User {user_id} did not have {movie_url} in favorites.")
            return False
    except Exception as e:
        logger.error(f"Error removing favorite for user {user_id}, movie {movie_url}: {e}")
        return False
    finally:
        conn.close()

def get_favorites(user_id: int) -> list:
    """Retrieves a list of favorite movies for a given user."""
    conn = sqlite3.connect('movies.db')
    c = conn.cursor()
    c.execute("""
        SELECT m.title, m.url, m.source, m.image_url, m.category, m.description, m.release_year, m.average_rating, m.rating_count, m.genres
        FROM favorites f
        JOIN movies m ON f.movie_url = m.url
        WHERE f.user_id = ?
        ORDER BY f.added_date DESC
    """, (user_id,))
    results = c.fetchall()
    conn.close()
    return results
