import sqlite3
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from .constants import get_database_path
import time
import threading
import asyncio
import queue

logger = logging.getLogger('DiscordBot')

class DatabaseHandler:
    def __init__(self, db_path: Optional[str] = None, dev_mode: bool = False, pool_size: int = 5):
        """Initialize database path and ensure directory exists."""
        try:
            # Use provided path or get appropriate path based on mode
            self.db_path = db_path if db_path else get_database_path(dev_mode)
            
            # Ensure the directory exists
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize connection pool
            self.pool_size = pool_size
            self.connection_pool = queue.Queue(maxsize=pool_size)
            for _ in range(pool_size):
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                self.connection_pool.put(conn)
            
            # Initialize the database schema
            self.write_lock = threading.Lock()
            self._init_db()
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    def _get_connection_from_pool(self):
        try:
            # Try to get a connection within 5 seconds
            conn = self.connection_pool.get(timeout=5)
            logger.debug(f"Retrieved connection from pool, remaining connections: {self.connection_pool.qsize()}")
            return conn
        except queue.Empty:
            # Pool is exhausted, log a warning and create a new connection
            logger.warning('Connection pool exhausted. Creating a new connection.')
            return sqlite3.connect(self.db_path, check_same_thread=False)

    def _get_connection(self):
        """Get a new database connection with proper configuration."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    def _return_connection_to_pool(self, conn):
        self.connection_pool.put(conn)

    def close(self):
        """Close the database connection."""
        while not self.connection_pool.empty():
            conn = self.connection_pool.get()
            conn.close()

    def __del__(self):
        """Ensure connection is closed when object is destroyed."""
        self.close()

    def _execute_with_retry(self, operation, max_retries=5, initial_delay=0.2):
        """Execute a database operation with retry logic."""
        last_error = None
        
        for attempt in range(max_retries):
            conn = self._get_connection_from_pool()
            try:
                result = operation(conn)
                conn.commit()
                return result
            except sqlite3.OperationalError as e:
                last_error = e
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.warning(f"Database locked, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise
            finally:
                self._return_connection_to_pool(conn)
        
        if last_error:
            raise last_error
        raise Exception("Maximum retries exceeded")

    def _init_db(self):
        """Initialize all database tables."""
        def init_operation(conn):
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id BIGINT PRIMARY KEY,
                    channel_name TEXT NOT NULL,
                    description TEXT,
                    suitable_posts TEXT,
                    unsuitable_posts TEXT,
                    rules TEXT,
                    setup_complete BOOLEAN DEFAULT FALSE,
                    nsfw BOOLEAN DEFAULT FALSE,
                    enriched BOOLEAN DEFAULT FALSE,
                    category_id BIGINT
                )
            """)

            # Channel summary threads table with foreign key
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channel_summary (
                    channel_id BIGINT,
                    summary_thread_id BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (channel_id) REFERENCES channels(channel_id),
                    PRIMARY KEY (channel_id, created_at)
                )
            """)
            
            # Daily summaries table with foreign key
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    daily_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    channel_id BIGINT NOT NULL REFERENCES channels(channel_id),
                    full_summary TEXT,
                    short_summary TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, channel_id) ON CONFLICT REPLACE
                )
            """)
            
            # Members table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS members (
                    member_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    global_name TEXT,
                    server_nick TEXT,
                    avatar_url TEXT,
                    discriminator TEXT,
                    bot BOOLEAN DEFAULT FALSE,
                    system BOOLEAN DEFAULT FALSE,
                    accent_color INTEGER,
                    banner_url TEXT,
                    discord_created_at TEXT,
                    guild_join_date TEXT,
                    role_ids TEXT,  /* JSON array of role IDs */
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Messages table with FTS support
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id BIGINT PRIMARY KEY,
                    channel_id BIGINT NOT NULL,
                    author_id BIGINT NOT NULL,
                    content TEXT,
                    created_at TEXT,
                    attachments TEXT,
                    embeds TEXT,
                    reaction_count INTEGER,
                    reactors TEXT,
                    reference_id BIGINT,
                    edited_at TEXT,
                    is_pinned BOOLEAN,
                    thread_id BIGINT,
                    message_type TEXT,
                    flags INTEGER,
                    jump_url TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (author_id) REFERENCES members(member_id),
                    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
                )
            """)
            
            # Full-text search for messages
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                    content,
                    content='messages',
                    content_rowid='message_id'
                )
            """)
            
            # Create indexes
            self._create_indexes(cursor)
            
            cursor.close()
        
        self._execute_with_retry(lambda conn: init_operation(conn))

    def _create_indexes(self, cursor):
        """Create all necessary indexes."""
        indexes = [
            ("idx_channel_id", "messages(channel_id)"),
            ("idx_created_at", "messages(created_at)"),
            ("idx_author_id", "messages(author_id)"),
            ("idx_reference_id", "messages(reference_id)"),
            ("idx_daily_summaries_date", "daily_summaries(date)"),
            ("idx_daily_summaries_channel", "daily_summaries(channel_id)"),
            ("idx_members_username", "members(username)")
        ]
        
        for index_name, index_def in indexes:
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} ON {index_def}
                """)
            except sqlite3.Error as e:
                logger.error(f"Error creating index {index_name}: {e}")

    def execute_query(self, query: str, params: tuple = ()) -> List[tuple]:
        """Execute a SQL query and return the results."""
        def query_operation(conn):
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return results
            
        return self._execute_with_retry(query_operation)

    def _store_messages(self, messages: List[Dict]):
        logger.debug(f"Starting to store {len(messages)} messages")
        with self.write_lock:
            def store_operation(conn):
                cursor = conn.cursor()
                
                for message in messages:
                    try:
                        # Get and validate message ID
                        message_id = message.get('message_id')
                        if message_id is None:
                            # Fall back to 'id' if message_id isn't present
                            message_id = message.get('id')
                            if message_id is None:
                                raise ValueError("Message must have either 'message_id' or 'id' field")

                        # Ensure all fields are properly serialized
                        attachments = message.get('attachments', [])
                        embeds = message.get('embeds', [])
                        reactors = message.get('reactors', [])
                        
                        # Convert to empty lists if None or 'null'
                        if attachments is None or attachments == 'null':
                            attachments = []
                        if embeds is None or embeds == 'null':
                            embeds = []
                        if reactors is None or reactors == 'null':
                            reactors = []
                        
                        attachments_json = json.dumps(attachments if isinstance(attachments, (list, dict)) else [])
                        embeds_json = json.dumps(embeds if isinstance(embeds, (list, dict)) else [])
                        reactors_json = json.dumps(reactors if isinstance(reactors, (list, dict)) else [])
                        
                        created_at = message.get('created_at')
                        if created_at:
                            created_at = created_at.isoformat() if hasattr(created_at, 'isoformat') else str(created_at)
                        edited_at = message.get('edited_at')
                        if edited_at:
                            edited_at = edited_at.isoformat() if hasattr(edited_at, 'isoformat') else str(edited_at)
                        
                        try:
                            cursor.execute("""
                                INSERT OR REPLACE INTO messages 
                                (message_id, channel_id, author_id,
                                 content, created_at, attachments, embeds, reaction_count, 
                                 reactors, reference_id, edited_at, is_pinned, thread_id, 
                                 message_type, flags, jump_url, is_deleted, indexed_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, CURRENT_TIMESTAMP)
                            """, (
                                message_id,
                                message.get('channel_id'),
                                message.get('author_id') or (message.get('author', {}).get('id')),
                                message.get('content'),
                                created_at,
                                attachments_json,
                                embeds_json,
                                message.get('reaction_count', 0),
                                reactors_json,
                                message.get('reference_id'),
                                edited_at,
                                message.get('is_pinned', False),
                                message.get('thread_id'),
                                message.get('message_type'),
                                message.get('flags', 0),
                                message.get('jump_url')
                            ))
                        except sqlite3.Error as e:
                            logger.error(f"Database error storing message {message_id}: {e}")
                            continue
                    
                    except Exception as e:
                        logger.error(f"Error processing individual message: {e}")
                        logger.error(f"Problem message ID: {message.get('message_id') or message.get('id')}")
                        logger.debug(f"Problem message: {json.dumps(message, default=str)}")
                        continue
                
                cursor.close()
                logger.debug(f"Stored {len(messages)} messages")
            
            self._execute_with_retry(store_operation)

    def store_messages(self, messages: List[Dict]):
        self._store_messages(messages)

    def get_last_message_id(self, channel_id: int) -> Optional[int]:
        """Get the ID of the last archived message for a channel."""
        def get_last_message_operation(conn):
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(message_id) FROM messages WHERE channel_id = ?
            """, (channel_id,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result and result[0] else None
            
        return self._execute_with_retry(get_last_message_operation)

    def search_messages(self, query: str, channel_id: Optional[int] = None) -> List[Dict]:
        """Search messages using FTS index."""
        def search_operation(conn):
            try:
                sql = """
                    SELECT m.*
                    FROM messages_fts fts
                    JOIN messages m ON fts.rowid = m.message_id
                    WHERE fts.content MATCH ?
                """
                params = [query]
                
                if channel_id:
                    sql += " AND m.channel_id = ?"
                    params.append(channel_id)
                    
                sql += " ORDER BY m.created_at DESC LIMIT 100"
                
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(sql, params)
                results = [dict(row) for row in cursor.fetchall()]
                cursor.close()
                return results
                
            except Exception as e:
                logger.error(f"Error searching messages: {e}")
                return []
            
        return self._execute_with_retry(search_operation)

    # Summary Methods
    def store_daily_summary(self, 
                          channel_id: int,
                          full_summary: Optional[str],
                          short_summary: Optional[str],
                          date: Optional[datetime] = None) -> bool:
        """Store daily channel summary."""
        def summary_operation(conn):
            if date is None:
                date_str = datetime.utcnow().date().isoformat()
            else:
                date_str = date.isoformat() if isinstance(date, datetime) else date
                
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO daily_summaries 
                (date, channel_id, full_summary, short_summary)
                VALUES (?, ?, ?, ?)
            """, (
                date_str,
                channel_id,
                full_summary,
                short_summary
            ))
            cursor.close()
            return True
            
        return self._execute_with_retry(summary_operation)

    # Thread Management Methods
    def get_summary_thread_id(self, channel_id: int) -> Optional[int]:
        """Get the summary thread ID for a channel."""
        def get_thread_operation(conn):
            cursor = conn.cursor()
            cursor.execute("""
                SELECT summary_thread_id 
                FROM channel_summary 
                WHERE channel_id = ?
            """, (channel_id,))
            result = cursor.fetchone()
            cursor.close()
            return result['summary_thread_id'] if result else None
            
        return self._execute_with_retry(get_thread_operation)

    def update_summary_thread(self, channel_id: int, thread_id: Optional[int]):
        """Update or delete the summary thread ID for a channel for the current month."""
        def update_thread_operation(conn):
            cursor = conn.cursor()
            try:
                # First, delete any existing entries for this channel for the current month
                cursor.execute(
                    """
                    DELETE FROM channel_summary 
                    WHERE channel_id = ? 
                    AND strftime('%Y-%m', created_at) = strftime('%Y-%m', CURRENT_TIMESTAMP)
                    """,
                    (channel_id,)
                )
                
                if thread_id is not None:
                    # Insert the new thread ID
                    cursor.execute(
                        """
                        INSERT INTO channel_summary 
                        (channel_id, summary_thread_id, created_at, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (channel_id, thread_id)
                    )
                conn.commit()
            except Exception as e:
                self.logger.error(f"Error updating summary thread: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
            return True
            
        return self._execute_with_retry(update_thread_operation)

    def get_all_message_ids(self, channel_id: int) -> List[int]:
        """Get all message IDs that have been archived for a channel."""
        def get_ids_operation(conn):
            cursor = conn.cursor()
            cursor.execute(
                "SELECT message_id FROM messages WHERE channel_id = ?",
                (channel_id,)
            )
            results = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return results
            
        return self._execute_with_retry(get_ids_operation)

    def get_message_date_range(self, channel_id: int) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the earliest and latest message dates for a channel."""
        def get_range_operation(conn):
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MIN(created_at), MAX(created_at)
                FROM messages 
                WHERE channel_id = ?
            """, (channel_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] and result[1]:
                return (
                    datetime.fromisoformat(result[0]),
                    datetime.fromisoformat(result[1])
                )
            return None, None
            
        return self._execute_with_retry(get_range_operation)

    def get_message_dates(self, channel_id: int) -> List[str]:
        """Get all message dates for a channel to check for gaps."""
        def get_dates_operation(conn):
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_at
                FROM messages 
                WHERE channel_id = ?
                ORDER BY created_at
            """, (channel_id,))
            results = [dict(row)['created_at'] for row in cursor.fetchall()]
            cursor.close()
            return results
            
        return self._execute_with_retry(get_dates_operation)

    def get_member(self, member_id: int) -> Optional[Dict]:
        """Get a member by their ID."""
        def get_member_operation(conn):
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT *
                FROM members
                WHERE member_id = ?
            """, (member_id,))
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
            
        return self._execute_with_retry(get_member_operation)

    def message_exists(self, message_id: int) -> bool:
        """Check if a message exists in the database."""
        def check_message_operation(conn):
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM messages WHERE message_id = ?
            """, (message_id,))
            result = cursor.fetchone()
            cursor.close()
            return bool(result)
            
        return self._execute_with_retry(check_message_operation)

    def update_message(self, message: Dict) -> bool:
        """Update an existing message with new data."""
        def update_operation(conn):
            cursor = conn.cursor()
            
            # Get and validate message ID
            message_id = message.get('message_id') or message.get('id')
            if message_id is None:
                raise ValueError("Message must have either 'message_id' or 'id' field")

            # Process attachments, embeds, and reactors
            attachments = message.get('attachments', [])
            embeds = message.get('embeds', [])
            reactors = message.get('reactors', [])
            
            # Convert to empty lists if None or 'null'
            attachments = [] if attachments is None or attachments == 'null' else attachments
            embeds = [] if embeds is None or embeds == 'null' else embeds
            reactors = [] if reactors is None or reactors == 'null' else reactors
            
            attachments_json = json.dumps(attachments if isinstance(attachments, (list, dict)) else [])
            embeds_json = json.dumps(embeds if isinstance(embeds, (list, dict)) else [])
            reactors_json = json.dumps(reactors if isinstance(reactors, (list, dict)) else [])
            
            edited_at = message.get('edited_at')
            if edited_at:
                edited_at = edited_at.isoformat() if hasattr(edited_at, 'isoformat') else str(edited_at)

            cursor.execute("""
                UPDATE messages 
                SET content = COALESCE(?, content),
                    attachments = COALESCE(?, attachments),
                    embeds = COALESCE(?, embeds),
                    reaction_count = COALESCE(?, reaction_count),
                    reactors = COALESCE(?, reactors),
                    edited_at = COALESCE(?, edited_at),
                    is_pinned = COALESCE(?, is_pinned),
                    flags = COALESCE(?, flags),
                    indexed_at = CURRENT_TIMESTAMP
                WHERE message_id = ?
            """, (
                message.get('content'),
                attachments_json,
                embeds_json,
                message.get('reaction_count', 0),
                reactors_json,
                edited_at,
                message.get('is_pinned'),
                message.get('flags'),
                message_id
            ))
            
            cursor.close()
            return True
            
        return self._execute_with_retry(update_operation)

    def create_or_update_member(self, member_id: int, username: str, display_name: Optional[str] = None, 
                              global_name: Optional[str] = None, avatar_url: Optional[str] = None,
                              discriminator: Optional[str] = None, bot: bool = False, 
                              system: bool = False, accent_color: Optional[int] = None,
                              banner_url: Optional[str] = None, discord_created_at: Optional[str] = None,
                              guild_join_date: Optional[str] = None, role_ids: Optional[str] = None) -> bool:
        """Create or update a member in the database."""
        def member_operation(conn):
            cursor = conn.cursor()
            
            # Check if member exists
            cursor.execute("SELECT * FROM members WHERE member_id = ?", (member_id,))
            existing_member = cursor.fetchone()
            
            if existing_member:
                # Update existing member if fields changed
                update_fields = []
                update_values = []
                
                # Build update query dynamically based on provided values
                if username is not None:
                    update_fields.append("username = ?")
                    update_values.append(username)
                if display_name is not None:
                    update_fields.append("server_nick = ?")
                    update_values.append(display_name)
                if global_name is not None:
                    update_fields.append("global_name = ?")
                    update_values.append(global_name)
                if avatar_url is not None:
                    update_fields.append("avatar_url = ?")
                    update_values.append(avatar_url)
                if discriminator is not None:
                    update_fields.append("discriminator = ?")
                    update_values.append(discriminator)
                if bot is not None:
                    update_fields.append("bot = ?")
                    update_values.append(bot)
                if system is not None:
                    update_fields.append("system = ?")
                    update_values.append(system)
                if accent_color is not None:
                    update_fields.append("accent_color = ?")
                    update_values.append(accent_color)
                if banner_url is not None:
                    update_fields.append("banner_url = ?")
                    update_values.append(banner_url)
                if discord_created_at is not None:
                    update_fields.append("discord_created_at = ?")
                    update_values.append(discord_created_at)
                if guild_join_date is not None:
                    update_fields.append("guild_join_date = ?")
                    update_values.append(guild_join_date)
                if role_ids is not None:
                    update_fields.append("role_ids = ?")
                    update_values.append(role_ids)
                
                if update_fields:
                    update_fields.append("updated_at = CURRENT_TIMESTAMP")
                    update_values.append(member_id)
                    
                    update_sql = f"""
                        UPDATE members
                        SET {', '.join(update_fields)}
                        WHERE member_id = ?
                    """
                    cursor.execute(update_sql, tuple(update_values))
            else:
                # Create new member
                cursor.execute("""
                    INSERT INTO members 
                    (member_id, username, server_nick, global_name, avatar_url, 
                     discriminator, bot, system, accent_color, banner_url, 
                     discord_created_at, guild_join_date, role_ids)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (member_id, username, display_name, global_name, avatar_url,
                      discriminator, bot, system, accent_color, banner_url,
                      discord_created_at, guild_join_date, role_ids))
            
            cursor.close()
            return True
            
        return self._execute_with_retry(member_operation)

    def get_channel(self, channel_id: int) -> Optional[Dict]:
        """Get a channel by its ID."""
        def get_channel_operation(conn):
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT channel_id, channel_name, description, suitable_posts, 
                       unsuitable_posts, rules, setup_complete, nsfw, enriched, category_id
                FROM channels
                WHERE channel_id = ?
            """, (channel_id,))
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
            
        return self._execute_with_retry(get_channel_operation)

    def create_or_update_channel(self, channel_id: int, channel_name: str, nsfw: bool = False, category_id: Optional[int] = None) -> bool:
        """Create or update a channel in the database."""
        def channel_operation(conn):
            cursor = conn.cursor()
            # Check if channel exists
            cursor.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,))
            existing_channel = cursor.fetchone()
            
            if existing_channel:
                cursor.execute("""
                    UPDATE channels
                    SET channel_name = ?, nsfw = ?, category_id = ?
                    WHERE channel_id = ?
                """, (channel_name, nsfw, category_id, channel_id))
            else:
                cursor.execute("""
                    INSERT INTO channels 
                    (channel_id, channel_name, description, suitable_posts, 
                     unsuitable_posts, rules, setup_complete, nsfw, enriched, category_id)
                    VALUES (?, ?, '', '', '', '', FALSE, ?, FALSE, ?)
                """, (channel_id, channel_name, nsfw, category_id))
            cursor.close()
            return True
        return self._execute_with_retry(channel_operation)

    def get_messages_after(self, date: datetime) -> List[Dict]:
        """Get all messages after a given date."""
        def get_messages_operation(conn):
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM messages 
                WHERE created_at > ?
                ORDER BY created_at ASC
            """, (date.isoformat(),))
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
            
        return self._execute_with_retry(get_messages_operation)

    def get_messages_by_ids(self, message_ids: List[int]) -> List[Dict]:
        """Get messages by their IDs."""
        def get_messages_operation(conn):
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(message_ids))
            cursor.execute(f"""
                SELECT * FROM messages 
                WHERE message_id IN ({placeholders})
            """, tuple(message_ids))
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
            
        return self._execute_with_retry(get_messages_operation)

    @property
    def conn(self):
        # Return an existing connection or create a new one if not available
        if not hasattr(self, '_conn') or self._conn is None:
            import sqlite3
            self._conn = sqlite3.connect(self.db_path)
            # Optionally, set row_factory if needed
            self._conn.row_factory = sqlite3.Row
        return self._conn
