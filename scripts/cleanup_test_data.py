import discord
from discord.ext import commands
import asyncio
import logging
from dotenv import load_dotenv, set_key
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable discord.py debug logging
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db_handler import DatabaseHandler

class ChannelCleaner(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guild_messages = True
        intents.voice_states = False  # Explicitly disable voice states
        
        super().__init__(command_prefix="!", intents=intents)
        self.db = DatabaseHandler()
        
    async def delete_database_entries(self, channel_id: int):
        """Delete all database entries for a channel."""
        try:
            # Get counts before deletion - handle case where query returns None
            result = self.db.execute_query(
                "SELECT COUNT(*) FROM daily_summaries WHERE channel_id = ?",
                (channel_id,)
            )
            before_daily = result[0][0] if result else 0
            
            result = self.db.execute_query(
                "SELECT COUNT(*) FROM channel_summary WHERE channel_id = ?",
                (channel_id,)
            )
            before_summary = result[0][0] if result else 0
            
            # Delete from daily_summaries
            self.db.execute_query(
                "DELETE FROM daily_summaries WHERE channel_id = ?",
                (channel_id,)
            )
            logger.info(f"Deleted {before_daily} entries from daily_summaries for channel {channel_id}")
            
            # Delete from channel_summary
            self.db.execute_query(
                "DELETE FROM channel_summary WHERE channel_id = ?",
                (channel_id,)
            )
            logger.info(f"Deleted {before_summary} entries from channel_summary for channel {channel_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting database entries for channel {channel_id}: {e}")
            return False

    async def delete_messages(self, channel):
        """Delete all messages from a channel or thread."""
        deleted_count = 0
        
        try:
            # Add logging for initial message check
            logger.info(f"Starting message deletion in channel: #{channel.name}")
            
            # First check if there are any deletable messages
            has_deletable_messages = False
            message_count = 0
            bot_message_count = 0
            async for message in channel.history(limit=None):
                message_count += 1
                if message.author.id == self.user.id:
                    has_deletable_messages = True
                    bot_message_count += 1
            
            logger.info(f"Channel #{channel.name} has {message_count} total messages, {bot_message_count} from bot")
            
            if not has_deletable_messages:
                if isinstance(channel, discord.Thread):
                    logger.info(f"Skipping thread (no deletable messages): #{channel.parent.name}/{channel.name}")
                else:
                    logger.info(f"Skipping channel (no deletable messages): #{channel.name}")
                return 0
                
            while True:
                messages = []
                async for message in channel.history(limit=None):
                    if message.author.id == self.user.id:
                        messages.append(message)
                
                if not messages:
                    break
                
                # Process in chunks of 100
                chunks = [messages[i:i + 100] for i in range(0, len(messages), 100)]
                
                for chunk in chunks:
                    if len(chunk) > 1:
                        await channel.delete_messages(chunk)
                        deleted_count += len(chunk)
                        if isinstance(channel, discord.Thread):
                            logger.info(f"Deleted {len(chunk)} messages from thread: #{channel.parent.name}/{channel.name}")
                        else:
                            logger.info(f"Deleted {len(chunk)} messages from channel: #{channel.name}")
                    else:
                        await chunk[0].delete()
                        deleted_count += 1
                        if isinstance(channel, discord.Thread):
                            logger.info(f"Deleted 1 message from thread: #{channel.parent.name}/{channel.name}")
                        else:
                            logger.info(f"Deleted 1 message from channel: #{channel.name}")
                    
                    await asyncio.sleep(1)  # Rate limiting
                
                if len(chunks) == 0:
                    break
                
        except Exception as e:
            if isinstance(channel, discord.Thread):
                logger.error(f"Error cleaning thread #{channel.parent.name}/{channel.name}: {e}")
            else:
                logger.error(f"Error cleaning channel #{channel.name}: {e}")
        
        return deleted_count

    async def clean_channel_and_threads(self, channel):
        """Clean a channel and all its threads."""
        total_deleted = 0
        
        # If it's a category, process all channels in it
        if isinstance(channel, discord.CategoryChannel):
            logger.info(f"Processing category: {channel.name}")
            for subchannel in channel.channels:
                deleted = await self.clean_channel_and_threads(subchannel)
                total_deleted += deleted
            return total_deleted
        
        # For text channels and threads
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            # Clean main channel messages
            deleted = await self.delete_messages(channel)
            total_deleted += deleted
            
            # Clean database entries
            db_deleted = await self.delete_database_entries(channel.id)
            if db_deleted:
                logger.info(f"Deleted database entries for channel #{channel.name}")
            
            # Only process threads if it's a text channel (not a thread)
            if isinstance(channel, discord.TextChannel):
                # Clean all threads in the channel
                threads = channel.threads
                logger.info(f"Found {len(threads)} active threads in #{channel.name}")
                
                # Get archived threads
                archived_threads = []
                async for thread in channel.archived_threads():
                    archived_threads.append(thread)
                
                all_threads = threads + archived_threads
                logger.info(f"Processing {len(all_threads)} total threads in #{channel.name}")
                
                for thread in all_threads:
                    try:
                        # Only delete threads created by the bot
                        if thread.owner_id == self.user.id:
                            await thread.delete()
                            logger.info(f"Deleted thread: {thread.name} in #{channel.name}")
                        else:
                            logger.info(f"Skipping thread not owned by bot: {thread.name} in #{channel.name}")
                    except Exception as e:
                        logger.error(f"Could not delete thread {thread.name}: {e}")
        
        return total_deleted

    async def close(self):
        """Cleanup when bot is shutting down."""
        self.db.conn.close()  # Remove await - sqlite connection doesn't use async
        await super().close()

async def main():
    # Load environment variables
    load_dotenv()
    
    # Hardcode channels for testing
    dev_channels = ['1320120559425818655', '1320122436703752252']
    logger.info(f"Using hardcoded channel IDs: {dev_channels}")
    
    bot_token = os.getenv('DISCORD_BOT_TOKEN')
    summary_channel_id = int(os.getenv('DEV_SUMMARY_CHANNEL_ID'))
    
    # Remove the environment variable parsing since we're using hardcoded values
    
    @bot.event
    async def on_ready():
        logger.info(f"Logged in as {bot.user}")
        logger.info(f"Starting channel processing. Number of channels to process: {len(dev_channels)}")
        logger.info(f"Channels to process: {dev_channels}")
        total_deleted = 0
        
        # First clean the summary channel
        try:
            summary_channel = bot.get_channel(summary_channel_id)
            if summary_channel:
                logger.info(f"Processing summary channel: #{summary_channel.name}")
                deleted = await bot.clean_channel_and_threads(summary_channel)
                total_deleted += deleted
                logger.info(f"Finished cleaning summary channel #{summary_channel.name}: {deleted} messages deleted")
            else:
                logger.error(f"Could not find summary channel with ID: {summary_channel_id}")
        except Exception as e:
            logger.error(f"Error processing summary channel: {e}")
        
        # Process each channel/category
        for i, channel_id in enumerate(dev_channels, 1):
            logger.info(f"Processing channel {i} of {len(dev_channels)}")
            if not channel_id:  # Skip empty strings
                logger.warning(f"Skipping empty channel ID at position {i}")
                continue
                
            try:
                channel_id_int = int(channel_id)
                logger.info(f"Converting channel ID to int: {channel_id} -> {channel_id_int}")
                channel = bot.get_channel(channel_id_int)
                logger.info(f"Attempting to clean channel ID: {channel_id_int}")
                
                if not channel:
                    logger.error(f"Could not find channel with ID: {channel_id_int}")
                    continue
                
                # Add permission check
                permissions = channel.permissions_for(channel.guild.me)
                logger.info(f"Bot permissions in channel #{channel.name}: manage_messages={permissions.manage_messages}")
                if not permissions.manage_messages:
                    logger.error(f"Bot lacks manage_messages permission in channel: #{channel.name}")
                    continue
                    
                logger.info(f"Found channel: #{channel.name} (Type: {type(channel)})")
                deleted = await bot.clean_channel_and_threads(channel)
                total_deleted += deleted
                logger.info(f"Finished processing channel {i} of {len(dev_channels)}: #{channel.name}")
                
            except ValueError as e:
                logger.error(f"Invalid channel ID format at position {i}: '{channel_id}'")
            except Exception as e:
                logger.error(f"Error processing channel at position {i} ({channel_id}): {e}")
        
        logger.info(f"Channel processing complete. Total messages deleted: {total_deleted}")
        
        # Properly close the session before shutting down
        await bot.http.close()
        await bot.close()
    
    await bot.start(bot_token)

if __name__ == "__main__":
    # Create bot instance outside of main
    bot = ChannelCleaner()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    finally:
        # Ensure everything is cleaned up
        if not bot.is_closed():
            asyncio.run(bot.close())