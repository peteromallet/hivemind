import os
import sys
import logging
from pathlib import Path

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.db_handler import DatabaseHandler
from src.common.constants import get_database_path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def migrate_database(db_path):
    """Add category_id column to channels table if it doesn't exist."""
    try:
        db = DatabaseHandler(db_path)
        
        # Check if category_id column exists
        cursor = db.conn.cursor()
        cursor.execute("PRAGMA table_info(channels)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'category_id' not in columns:
            logger.info("Adding category_id column to channels table")
            cursor.execute("ALTER TABLE channels ADD COLUMN category_id BIGINT")
            db.conn.commit()
            logger.info("Migration successful")
        else:
            logger.info("category_id column already exists, no migration needed")
        
        db.close()
        return True
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

def main():
    """Main entry point for migration script."""
    load_dotenv()
    
    # Migrate production database
    prod_db_path = get_database_path(dev_mode=False)
    logger.info(f"Migrating production database at {prod_db_path}")
    migrate_database(prod_db_path)
    
    # Migrate development database
    dev_db_path = get_database_path(dev_mode=True)
    logger.info(f"Migrating development database at {dev_db_path}")
    migrate_database(dev_db_path)
    
    logger.info("Migration complete")

if __name__ == "__main__":
    main() 