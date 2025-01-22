import os
import sys
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.constants import get_database_path
from src.common.db_handler import DatabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('create_dev_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Starting create_dev_db.py script")
        
        # Get database paths
        prod_db_path = get_database_path(False)
        dev_db_path = get_database_path(True)
        
        logger.info(f"Production DB path: {prod_db_path}")
        logger.info(f"Dev DB path: {dev_db_path}")
        
        if not os.path.exists(prod_db_path):
            logger.error(f"Production database not found at: {prod_db_path}")
            sys.exit(1)
        
        if os.path.exists(dev_db_path):
            response = input(f"Dev database already exists at {dev_db_path}. Delete it? [y/N] ").lower()
            if response != 'y':
                logger.info("Aborting - existing database will not be modified")
                sys.exit(0)
            os.remove(dev_db_path)
            logger.info(f"Deleted existing database at: {dev_db_path}")
        
        # Initialize dev database
        dev_db = DatabaseHandler(dev_db_path, dev_mode=True)
        dev_db.close()
        
        # Connect to both databases
        src = sqlite3.connect(prod_db_path)
        dst = sqlite3.connect(dev_db_path)
        
        # Enable foreign keys
        src.execute("PRAGMA foreign_keys = ON")
        dst.execute("PRAGMA foreign_keys = ON")
        
        try:
            # Calculate cutoff date (24 hours ago)
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=1)
            logger.info(f"Will copy data after: {cutoff_date}")
            
            # Get messages from the last 24 hours
            messages = src.execute("""
                SELECT * FROM messages 
                WHERE created_at > ?
            """, (cutoff_date.isoformat(),)).fetchall()
            
            if not messages:
                logger.error("No messages found in the specified time range")
                sys.exit(1)
                
            logger.info(f"Found {len(messages)} messages to copy")
            
            # Get unique channel IDs and member IDs
            channel_ids = {msg[2] for msg in messages}  # channel_id is at index 2
            member_ids = {msg[3] for msg in messages}   # author_id is at index 3
            
            # Copy channels
            channels = src.execute(f"""
                SELECT * FROM channels 
                WHERE channel_id IN ({','.join('?' * len(channel_ids))})
            """, list(channel_ids)).fetchall()
            
            dst.executemany("""
                INSERT INTO channels VALUES (?,?,?,?,?,?,?,?,?)
            """, channels)
            logger.info(f"Copied {len(channels)} channels")
            
            # Copy members
            members = src.execute(f"""
                SELECT * FROM members 
                WHERE id IN ({','.join('?' * len(member_ids))})
            """, list(member_ids)).fetchall()
            
            dst.executemany("""
                INSERT INTO members VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, members)
            logger.info(f"Copied {len(members)} members")
            
            # Copy messages
            dst.executemany("""
                INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, messages)
            logger.info(f"Copied {len(messages)} messages")
            
            # Commit changes
            dst.commit()
            
            # Verify
            count = dst.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            logger.info(f"Verification: {count} messages in dev database")
            
            logger.info("Successfully created dev database!")
            
        except Exception as e:
            logger.error(f"Error copying data: {e}")
            dst.rollback()
            raise
        finally:
            src.close()
            dst.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 