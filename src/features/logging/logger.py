import discord
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import json
import traceback  # Add at top of file with other imports

from src.common.db_handler import DatabaseHandler
from dotenv import load_dotenv
from src.common.base_bot import BaseDiscordBot
from src.common.error_handler import handle_errors

logger = logging.getLogger('MessageLogger')

class MessageLogger(BaseDiscordBot):
    def __init__(self, dev_mode=False):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        intents.members = True
        intents.reactions = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            heartbeat_timeout=120.0,
            guild_ready_timeout=30.0,
            gateway_queue_size=512
        )
        
        # Initialize database connection with dev mode
        self.db = DatabaseHandler(dev_mode=dev_mode)
        
        # Set dev mode
        self.dev_mode = dev_mode
        
        # Load environment variables
        load_dotenv()
        
        # Load bot user ID
        self.bot_user_id = int(os.getenv('BOT_USER_ID'))
        
        # Load monitored channels based on dev mode
        if dev_mode:
            dev_channels = os.getenv('DEV_CHANNELS_TO_MONITOR', '').strip()
            self.skip_channels = {int(id) for id in dev_channels.split(',') if id}
            self.skip_channels.add(1076117621407223832)  # Welcome channel
            logger.info(f"Dev mode: Monitoring all channels EXCEPT {self.skip_channels}")
        else:
            # In production, monitor all channels except welcome
            self.skip_channels = {1076117621407223832}  # Welcome channel
            logger.info("Production mode: Monitoring all channels except welcome")
        
        # Set up logging
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
        
    async def setup_hook(self):
        """Setup hook to initialize any necessary resources."""
        logger.info("Message logger initialized and ready")
        
    async def _prepare_message_data(self, message: discord.Message) -> Dict[str, Any]:
        """Convert a discord message into a format suitable for database storage."""
        try:
            # Calculate total reaction count
            reaction_count = sum(reaction.count for reaction in message.reactions) if message.reactions else 0
            
            # Get list of unique reactors
            reactors = []
            if message.reactions:
                for reaction in message.reactions:
                    async for user in reaction.users():
                        if user.id not in reactors and user.id != self.bot_user_id:
                            reactors.append(user.id)
            
            # More defensive thread_id handling with logging
            thread_id = None
            try:
                if hasattr(message, 'thread') and message.thread:
                    thread_id = message.thread.id
                    logger.debug(f"Found thread_id {thread_id} for message {message.id}")
                elif message.channel and isinstance(message.channel, discord.Thread):
                    thread_id = message.channel.id
                    logger.debug(f"Message {message.id} is in thread {thread_id}")
            except Exception as e:
                logger.debug(f"Error getting thread_id for message {message.id}: {e}")
            
            # Get guild display name (nickname) if available
            display_name = None  # Only set if there's a server nickname
            global_name = message.author.global_name
            try:
                if hasattr(message, 'guild') and message.guild:
                    member = message.guild.get_member(message.author.id)
                    if member:
                        display_name = member.nick  # Only use the server nickname
            except Exception as e:
                logger.debug(f"Error getting display name for user {message.author.id}: {e}")
            
            # Get category ID if available
            category_id = None
            if hasattr(message.channel, 'category') and message.channel.category:
                category_id = message.channel.category.id
            
            return {
                'id': message.id,
                'message_id': message.id,
                'channel_id': message.channel.id,
                'channel_name': message.channel.name,
                'author_id': message.author.id,
                'author_name': message.author.name,
                'author_discriminator': message.author.discriminator,
                'author_avatar_url': str(message.author.avatar.url) if message.author.avatar else None,
                'content': message.content,
                'created_at': message.created_at.isoformat(),
                'attachments': [
                    {
                        'url': attachment.url,
                        'filename': attachment.filename
                    } for attachment in message.attachments
                ],
                'embeds': [embed.to_dict() for embed in message.embeds],
                'reaction_count': reaction_count,
                'reactors': json.dumps(reactors),  # Store reactors as JSON array
                'reference_id': message.reference.message_id if message.reference else None,
                'edited_at': message.edited_at.isoformat() if message.edited_at else None,
                'is_pinned': message.pinned,
                'thread_id': thread_id,
                'message_type': str(message.type),
                'flags': message.flags.value,
                'jump_url': message.jump_url,
                'is_deleted': False,  # Messages are not deleted when first created
                'display_name': display_name,  # Server nickname or display name
                'global_name': global_name,  # Global display name
                'category_id': category_id
            }
        except Exception as e:
            logger.error(f"Error preparing message data: {e}")
            raise

    async def on_ready(self):
        """Called when the client is ready."""
        logger.info(f"Logged in as {self.user.name} ({self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guilds")

    @handle_errors("on_message")
    async def on_message(self, message: discord.Message):
        """Called when a message is sent in any channel the bot can see."""
        try:
            # Ignore messages from the bot itself or the configured bot user
            if message.author == self.user or message.author.id == self.bot_user_id:
                return
                
            # Skip configured channels
            if message.channel.id in self.skip_channels:
                return
                
            # Prepare and store the message
            message_data = await self._prepare_message_data(message)
            self.db.store_messages([message_data])
            
            logger.debug(f"Logged message {message.id} from {message.author.name} in #{message.channel.name}")
            
        except Exception as e:
            logger.error(f"Error logging message: {e}")

    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """Called when a message is edited."""
        try:
            # Ignore edits from the bot itself or the configured bot user
            if after.author == self.user or after.author.id == self.bot_user_id:
                return
                
            # Skip configured channels
            if after.channel.id in self.skip_channels:
                return
                
            # Prepare and store the edited message
            message_data = await self._prepare_message_data(after)
            self.db.store_messages([message_data])
            
            logger.debug(f"Logged edited message {after.id} from {after.author.name} in #{after.channel.name}")
            
        except Exception as e:
            logger.error(f"Error logging edited message: {e}")

    async def on_message_delete(self, message: discord.Message):
        """Called when a message is deleted."""
        try:
            # Skip configured channels
            if message.channel.id in self.skip_channels:
                return

            # Check if message exists first
            result = self.db.execute_query("""
                SELECT 1 FROM messages WHERE message_id = ?
            """, (message.id,))

            if result:
                # Update the message in the database to mark it as deleted
                self.db.execute_query("""
                    UPDATE messages 
                    SET is_deleted = TRUE 
                    WHERE message_id = ?
                """, (message.id,))
                logger.debug(f"Message {message.id} marked as deleted")
            
        except Exception as e:
            logger.error(f"Error handling message deletion: {e}")

    @handle_errors("on_reaction_add")
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.User):
        """Called when a reaction is added to a message."""
        try:
            # Ignore reactions from the bot itself
            if user == self.user or user.id == self.bot_user_id:
                return

            # Skip configured channels
            if reaction.message.channel.id in self.skip_channels:
                return

            # Get current message data from database
            try:
                results = self.db.execute_query("""
                    SELECT reaction_count, reactors
                    FROM messages
                    WHERE message_id = ?
                """, (reaction.message.id,))

                if not results:
                    logger.warning(f"Message {reaction.message.id} not found in database for reaction update")
                    # Check if message exists in Discord but not in DB
                    try:
                        msg = await reaction.message.channel.fetch_message(reaction.message.id)
                        if msg:
                            logger.error(f"Message exists in Discord but not in database:")
                            logger.error(f"  Content: {msg.content[:100]}...")
                            logger.error(f"  Author: {msg.author.name} ({msg.author.id})")
                            logger.error(f"  Created at: {msg.created_at}")
                    except Exception as fetch_err:
                        logger.error(f"Error fetching message from Discord: {fetch_err}")
                    return

                current_count = results[0].get('reaction_count', 0) or 0  # Default to 0 if None
                current_reactors_json = results[0].get('reactors')
                current_reactors = json.loads(current_reactors_json) if current_reactors_json else []

                # Add new reactor if not already in list
                if user.id not in current_reactors:
                    current_reactors.append(user.id)

                # Update database with new count and reactors
                try:
                    self.db.execute_query("""
                        UPDATE messages
                        SET reaction_count = ?, reactors = ?
                        WHERE message_id = ?
                    """, (current_count + 1, json.dumps(current_reactors), reaction.message.id))
                except Exception as db_error:
                    logger.error(f"Database error updating reaction: {db_error}")
                    logger.error(f"Message ID: {reaction.message.id}")
                    logger.error(f"Channel ID: {reaction.message.channel.id}")
                    logger.error(f"Channel Name: {reaction.message.channel.name}")
                    logger.error(f"Reactor ID: {user.id}")
                    logger.error(f"Reactor Name: {user.name}")
                    logger.error(f"Reaction: {reaction.emoji}")
                    logger.error(f"Current Count: {current_count}")
                    logger.error(f"Current Reactors: {current_reactors}")
                    logger.error(f"Database Error Traceback:\n{traceback.format_exc()}")
                    raise

                logger.debug(f"Added reaction {reaction.emoji} from {user.name} to message {reaction.message.id}")

            except Exception as query_error:
                logger.error(f"Database query error: {str(query_error)}")
                logger.error(f"Query Error Traceback:\n{traceback.format_exc()}")
                raise

        except Exception as e:
            logger.error(f"Error handling reaction add: {str(e)}")
            logger.error(f"Full Error Traceback:\n{traceback.format_exc()}")
            logger.error(f"Message ID: {getattr(reaction, 'message', None) and reaction.message.id}")
            logger.error(f"Channel ID: {getattr(reaction, 'message', None) and reaction.message.channel.id}")
            logger.error(f"Channel Name: {getattr(reaction, 'message', None) and reaction.message.channel.name}")
            logger.error(f"Reactor ID: {user.id}")
            logger.error(f"Reactor Name: {user.name}")
            logger.error(f"Reaction: {getattr(reaction, 'emoji', 'Unknown')}")
            
            # Check database state
            try:
                msg_check = self.db.execute_query("""
                    SELECT message_id, channel_id, author_id, content, created_at, 
                           reaction_count, reactors
                    FROM messages 
                    WHERE message_id = ?
                """, (reaction.message.id,))
                if msg_check:
                    logger.error("Current database state for message:")
                    logger.error(f"  Message exists in DB: {bool(msg_check)}")
                    logger.error(f"  Stored data: {json.dumps(msg_check[0], indent=2)}")
                else:
                    logger.error("Message does not exist in database")
            except Exception as db_check_err:
                logger.error(f"Error checking database state: {db_check_err}")

    async def on_reaction_remove(self, reaction: discord.Reaction, user: discord.User):
        """Called when a reaction is removed from a message."""
        try:
            # Ignore reactions from the bot itself
            if user == self.user or user.id == self.bot_user_id:
                return

            # Get current message data from database
            results = self.db.execute_query("""
                SELECT reaction_count, reactors
                FROM messages
                WHERE message_id = ?
            """, (reaction.message.id,))

            if not results:
                logger.warning(f"Message {reaction.message.id} not found in database for reaction update")
                return

            current_count, current_reactors_json = results[0]
            current_reactors = json.loads(current_reactors_json) if current_reactors_json else []

            # Remove reactor from list if present
            if user.id in current_reactors:
                current_reactors.remove(user.id)

            # Update database with new count and reactors
            self.db.execute_query("""
                UPDATE messages
                SET reaction_count = ?, reactors = ?
                WHERE message_id = ?
            """, (max(0, current_count - 1), json.dumps(current_reactors), reaction.message.id))

            logger.debug(f"Removed reaction from {user.name} on message {reaction.message.id}")

        except Exception as e:
            logger.error(f"Error handling reaction remove: {e}")

    def run_logger(self):
        """Run the message logger."""
        try:
            token = os.getenv('DISCORD_BOT_TOKEN')
            if not token:
                raise ValueError("DISCORD_BOT_TOKEN not found in environment variables")
                
            logger.info("Starting message logger...")
            super().run(token)
            
        except Exception as e:
            logger.error(f"Error running message logger: {e}")
            raise
        finally:
            if hasattr(self, 'db'):
                self.db.close()

def main():
    """Main entry point for running the message logger."""
    try:
        message_logger = MessageLogger()
        message_logger.run_logger()
    except Exception as e:
        logger.error(f"Failed to start message logger: {e}")
        raise

if __name__ == "__main__":
    main()
