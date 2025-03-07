import os
import sys
import argparse
# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import discord
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta, timezone
import logging
from dotenv import load_dotenv
from typing import Optional, List, Dict
import json
from src.common.db_handler import DatabaseHandler
from src.common.constants import get_database_path
from src.common.base_bot import BaseDiscordBot
from src.common.rate_limiter import RateLimiter
import threading
import queue
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('archive_discord.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Thread-local storage for database connections
thread_local = threading.local()

def get_db(db_path):
    """Get thread-local database connection."""
    if not hasattr(thread_local, "db"):
        thread_local.db = DatabaseHandler(db_path)
        thread_local.db._init_db()
    return thread_local.db

class MessageArchiver(BaseDiscordBot):
    def __init__(self, dev_mode=False, order="newest", days=None, batch_size=500, in_depth=False, channel_id=None):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        intents.messages = True
        intents.reactions = True
        super().__init__(
            command_prefix="!",
            intents=intents,
            heartbeat_timeout=120.0,
            guild_ready_timeout=30.0,
            gateway_queue_size=512,
            logger=logger
        )
        
        # Load environment variables
        load_dotenv()
        
        # Set database path based on mode
        self.db_path = get_database_path(dev_mode)
        
        # Create a queue for database operations
        self.db_queue = queue.Queue()
        
        # Track total messages archived
        self.total_messages_archived = 0
        
        # Check if token exists
        if not os.getenv('DISCORD_BOT_TOKEN'):
            raise ValueError("DISCORD_BOT_TOKEN not found in environment variables")
        
        # Add reconnect settings
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Get bot user ID from env
        self.bot_user_id = int(os.getenv('BOT_USER_ID'))
        
        # Get guild ID based on mode
        self.guild_id = int(os.getenv('DEV_GUILD_ID' if dev_mode else 'GUILD_ID'))
        
        # Channels to skip
        self.skip_channels = {1076117621407223832}  # Welcome channel
        
        # Default config for all channels
        self.default_config = {
            'batch_size': batch_size,
            'delay': 0.25
        }
        
        # Set message ordering
        self.oldest_first = order.lower() == "oldest"
        logger.info(f"Message ordering: {'oldest to newest' if self.oldest_first else 'newest to oldest'}")
        
        # Set days limit
        self.days_limit = days
        if days:
            logger.info(f"Will fetch messages from the last {days} days")
        else:
            logger.info("Will fetch all available messages")
            
        # Set in-depth mode
        self.in_depth = in_depth
        if in_depth:
            logger.info("Running in in-depth mode - will perform thorough message checks")
            
        # Set specific channel to archive
        self.target_channel_id = channel_id
        if channel_id:
            logger.info(f"Will only archive channel with ID: {channel_id}")
        
        # Add rate limiting tracking
        self.last_api_call = datetime.now()
        self.api_call_count = 0
        self.rate_limit_reset = datetime.now()
        self.rate_limit_remaining = 50
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter()
        
        # Add member update cache
        self.member_update_cache = {}
        self.member_update_cache_timeout = 300  # 5 minutes
        
        # Start database worker thread
        self.db_thread = threading.Thread(target=self._db_worker, daemon=True)
        self.db_thread.start()

    def _db_worker(self):
        """Worker thread for database operations."""
        db = get_db(self.db_path)
        while True:
            try:
                # Get the next operation from the queue
                operation = self.db_queue.get()
                if operation is None:
                    break
                
                # Execute the operation
                func, args, kwargs, future = operation
                try:
                    result = func(db, *args, **kwargs)
                    # Only try to set result if the future is not done
                    if not future.done():
                        try:
                            # Create a callback to set the result in the event loop
                            def set_result_callback():
                                if not future.done():
                                    future.set_result(result)
                            self.loop.call_soon_threadsafe(set_result_callback)
                        except Exception as e:
                            logger.error(f"Error setting future result: {e}")
                except Exception as exception:
                    # Only try to set exception if the future is not done
                    if not future.done():
                        try:
                            # Create a callback to set the exception in the event loop
                            # Capture the exception in the closure
                            def set_exception_callback(e=exception):
                                if not future.done():
                                    future.set_exception(e)
                            self.loop.call_soon_threadsafe(set_exception_callback)
                        except Exception as e:
                            logger.error(f"Error setting future exception: {e}")
                
                self.db_queue.task_done()
            except Exception as e:
                logger.error(f"Error in database worker: {e}")
                continue

    async def _db_operation(self, func, *args, **kwargs):
        """Execute a database operation in the worker thread."""
        # Make sure we have an event loop
        if not self.loop or self.loop.is_closed():
            self.loop = asyncio.get_event_loop()
        
        future = self.loop.create_future()
        self.db_queue.put((func, args, kwargs, future))
        try:
            return await future
        except Exception as e:
            logger.error(f"Error in database operation: {e}")
            raise

    async def setup_hook(self):
        """Setup hook to initialize database and start archiving."""
        try:
            # Initialize database in the worker thread
            await self._db_operation(lambda db: None)
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def _process_message(self, message, channel_id):
        """Process a single message and store it in the database."""
        try:
            message_start_time = datetime.now()
            
            # Calculate total reaction count and get reactors
            reaction_count = sum(reaction.count for reaction in message.reactions) if message.reactions else 0
            reactors = []
            
            if reaction_count > 0 and message.reactions:
                reaction_start_time = datetime.now()
                reactor_ids = set()
                try:
                    message_exists = await self._db_operation(
                        lambda db: db.message_exists(message.id)
                    )
                    if self.in_depth or not message_exists:
                        logger.info(f"Processing reactions for message {message.id}: {len(message.reactions)} types, {reaction_count} total reactions")
                        
                        guild = self.get_guild(self.guild_id)
                        
                        for reaction in message.reactions:
                            reaction_process_start = datetime.now()
                            try:
                                async def fetch_users():
                                    async for user in reaction.users(limit=50):
                                        reactor_ids.add(user.id)
                                        # Check if we've recently updated this member
                                        cache_key = f"{user.id}_{user.name}"
                                        cache_time = self.member_update_cache.get(cache_key, 0)
                                        current_time = time.time()
                                        
                                        if current_time - cache_time > self.member_update_cache_timeout:
                                            member = guild.get_member(user.id) if guild else None
                                            role_ids = json.dumps([role.id for role in member.roles]) if member and member.roles else None
                                            guild_join_date = member.joined_at.isoformat() if member and member.joined_at else None
                                            
                                            await self._db_operation(
                                                lambda db: db.create_or_update_member(
                                                    user.id,
                                                    user.name,
                                                    getattr(user, 'display_name', None),
                                                    getattr(user, 'global_name', None),
                                                    str(user.avatar.url) if user.avatar else None,
                                                    getattr(user, 'discriminator', None),
                                                    getattr(user, 'bot', False),
                                                    getattr(user, 'system', False),
                                                    getattr(user, 'accent_color', None),
                                                    str(user.banner.url) if getattr(user, 'banner', None) else None,
                                                    user.created_at.isoformat() if hasattr(user, 'created_at') else None,
                                                    guild_join_date,
                                                    role_ids
                                                )
                                            )
                                            # Update cache
                                            self.member_update_cache[cache_key] = current_time
                                
                                await self.rate_limiter.execute(f"reaction_{message.id}_{reaction}", fetch_users)
                                
                            except Exception as e:
                                logger.warning(f"Error fetching users for reaction {reaction} on message {message.id}: {e}")
                                continue
                        
                        if reactor_ids:
                            reactors = list(reactor_ids)
                except Exception as e:
                    logger.warning(f"Could not fetch reactors for message {message.id}: {e}")
            
            # Process the message author with caching
            if hasattr(message.author, 'id'):
                cache_key = f"{message.author.id}_{message.author.name}"
                cache_time = self.member_update_cache.get(cache_key, 0)
                current_time = time.time()
                
                if current_time - cache_time > self.member_update_cache_timeout:
                    guild = self.get_guild(self.guild_id)
                    member = guild.get_member(message.author.id) if guild else None
                    role_ids = json.dumps([role.id for role in member.roles]) if member and member.roles else None
                    guild_join_date = member.joined_at.isoformat() if member and member.joined_at else None
                    
                    await self._db_operation(
                        lambda db: db.create_or_update_member(
                            message.author.id,
                            message.author.name,
                            getattr(message.author, 'display_name', None),
                            getattr(message.author, 'global_name', None),
                            str(message.author.avatar.url) if message.author.avatar else None,
                            getattr(message.author, 'discriminator', None),
                            getattr(message.author, 'bot', False),
                            getattr(message.author, 'system', False),
                            getattr(message.author, 'accent_color', None),
                            str(message.author.banner.url) if getattr(message.author, 'banner', None) else None,
                            message.author.created_at.isoformat() if hasattr(message.author, 'created_at') else None,
                            guild_join_date,
                            role_ids
                        )
                    )
                    # Update cache
                    self.member_update_cache[cache_key] = current_time

            # Get the actual channel ID and name (use parent forum for threads)
            actual_channel = message.channel
            thread_id = None
            
            if hasattr(message.channel, 'parent') and message.channel.parent:
                actual_channel = message.channel.parent
                if isinstance(message.channel, discord.Thread) and not hasattr(message.channel, 'thread_type'):
                    thread_id = message.channel.id
                elif hasattr(message.channel, 'thread_type'):
                    actual_channel = message.channel

            # Create or update the channel
            category_id = None
            if hasattr(actual_channel, 'category') and actual_channel.category:
                category_id = actual_channel.category.id

            await self._db_operation(
                lambda db: db.create_or_update_channel(
                    channel_id=actual_channel.id,
                    channel_name=actual_channel.name,
                    nsfw=getattr(actual_channel, 'nsfw', False),
                    category_id=category_id
                )
            )
            
            processed_message = {
                'message_id': message.id,
                'channel_id': actual_channel.id,
                'author_id': message.author.id,
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
                'reactors': reactors,
                'reference_id': message.reference.message_id if message.reference else None,
                'edited_at': message.edited_at.isoformat() if message.edited_at else None,
                'is_pinned': message.pinned,
                'thread_id': thread_id,
                'message_type': str(message.type),
                'flags': message.flags.value,
                'jump_url': message.jump_url
            }
            
            return processed_message
            
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")
            return None

    async def close(self):
        """Properly close the bot and database connection."""
        try:
            # Signal database worker to stop
            self.db_queue.put(None)
            self.db_thread.join()
            await super().close()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    async def on_ready(self):
        """Called when bot is ready."""
        try:
            logger.info(f"Logged in as {self.user}")
            
            # Get the guild
            guild = self.get_guild(self.guild_id)
            if not guild:
                logger.error(f"Could not find guild with ID {self.guild_id}")
                await self.close()
                return
            
            # If a specific channel was requested, only archive that one
            if self.target_channel_id:
                channel = self.get_channel(self.target_channel_id)
                if not channel:
                    logger.error(f"Could not find channel with ID {self.target_channel_id}")
                    await self.close()
                    return
                
                logger.info(f"Starting archive of channel #{channel.name}")
                await self.archive_channel(self.target_channel_id)
                
                # Also archive any threads in this channel if it's a text channel
                if isinstance(channel, discord.TextChannel):
                    logger.info(f"Checking for threads in #{channel.name}")
                    # Archive archived threads
                    async for thread in channel.archived_threads():
                        logger.info(f"Starting archive of thread #{thread.name} in {channel.name}")
                        await self.archive_channel(thread.id)
                    # Archive active threads
                    for thread in channel.threads:
                        logger.info(f"Starting archive of active thread #{thread.name} in {channel.name}")
                        await self.archive_channel(thread.id)
            else:
                # Archive all text channels in the guild
                for channel in guild.text_channels:
                    if channel.id not in self.skip_channels:
                        logger.info(f"Starting archive of text channel #{channel.name}")
                        await self.archive_channel(channel.id)
                
                # Debug logging for forum channels
                logger.info(f"Found {len(guild.forums)} forum channels")
                for forum in guild.forums:
                    logger.info(f"Found forum channel: #{forum.name} (ID: {forum.id})")
                
                # Archive all forum channels and their threads
                for forum in guild.forums:
                    if forum.id not in self.skip_channels:
                        logger.info(f"Starting archive of forum channel #{forum.name}")
                        # Archive the forum posts (threads)
                        thread_count = 0
                        async for thread in forum.archived_threads():
                            thread_count += 1
                            logger.info(f"Starting archive of forum thread #{thread.name} in {forum.name}")
                            await self.archive_channel(thread.id)
                        # Archive active threads
                        for thread in forum.threads:
                            thread_count += 1
                            logger.info(f"Starting archive of active forum thread #{thread.name} in {forum.name}")
                            await self.archive_channel(thread.id)
                        logger.info(f"Processed {thread_count} total threads in forum #{forum.name}")
                
                # Archive threads in text channels
                for channel in guild.text_channels:
                    if channel.id not in self.skip_channels:
                        logger.info(f"Checking for threads in #{channel.name}")
                        # Archive archived threads
                        async for thread in channel.archived_threads():
                            logger.info(f"Starting archive of thread #{thread.name} in {channel.name}")
                            await self.archive_channel(thread.id)
                        # Archive active threads
                        for thread in channel.threads:
                            logger.info(f"Starting archive of active thread #{thread.name} in {channel.name}")
                            await self.archive_channel(thread.id)
            
            logger.info("Archiving complete, shutting down bot")
            logger.info(f"Total new messages archived across all channels: {self.total_messages_archived}")
            # Close the bot after archiving
            await self.close()
        except Exception as e:
            logger.error(f"Error in on_ready: {e}")
            await self.close()

    async def _wait_for_rate_limit(self):
        """Handles rate limiting for Discord API calls."""
        now = datetime.now()
        self.api_call_count += 1
        
        time_since_last = (now - self.last_api_call).total_seconds()
        
        # Basic throttling - ensure at least 0.1s between calls
        if time_since_last < 0.1:
            await asyncio.sleep(0.1 - time_since_last)
            logger.debug(f"Basic throttle - {time_since_last:.3f}s since last call")
        
        # Only enforce rate limits if we're actually approaching them
        if self.api_call_count >= 45:  # Conservative buffer before hitting 50
            wait_time = 1.0  # Start with a 1s pause
            logger.info(f"Rate limit approaching - Current count: {self.api_call_count}, Remaining: {self.rate_limit_remaining}")
            await asyncio.sleep(wait_time)
            self.api_call_count = 0
            self.rate_limit_reset = datetime.now() + timedelta(seconds=60)
            self.rate_limit_remaining = 50
            logger.info(f"Rate limit reset - New remaining: {self.rate_limit_remaining}, Next reset: {self.rate_limit_reset}")
        
        self.last_api_call = now

    async def archive_channel(self, channel_id: int) -> None:
        """Archive all messages from a channel."""
        channel_start_time = datetime.now()
        try:
            # Skip welcome channel
            if channel_id in self.skip_channels:
                logger.info(f"Skipping welcome channel {channel_id}")
                return
            
            channel = self.get_channel(channel_id)
            if not channel:
                logger.error(f"Could not find channel {channel_id}")
                return
            
            # Get the actual channel (parent forum if this is a thread)
            actual_channel = channel
            if hasattr(channel, 'parent') and channel.parent:
                actual_channel = channel.parent
                logger.debug(f"Using parent forum #{actual_channel.name} (ID: {actual_channel.id}) for thread #{channel.name}")
                # Update channel_id to use parent forum's ID
                channel_id = actual_channel.id
            
            logger.info(f"Starting archive of #{channel.name} at {channel_start_time}")
            
            # Initialize counters here so they're available in all branches
            message_counter = 0
            new_message_count = 0
            batch = []
            
            # Calculate the cutoff date if days limit is set
            cutoff_date = None
            if self.days_limit:
                # Make sure to create timezone-aware datetime
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.days_limit)
                logger.info(f"Will only fetch messages after {cutoff_date}")
            
            # Initialize dates as None
            earliest_date = None
            latest_date = None
            
            try:
                # Get date range of archived messages
                earliest_date, latest_date = await self._db_operation(
                    lambda db: db.get_message_date_range(channel_id)
                )
                # Make sure dates are timezone-aware
                if earliest_date:
                    earliest_date = earliest_date.replace(tzinfo=timezone.utc)
                    logger.info(f"Earliest message in DB for #{channel.name}: {earliest_date}")
                if latest_date:
                    latest_date = latest_date.replace(tzinfo=timezone.utc)
                    logger.info(f"Latest message in DB for #{channel.name}: {latest_date}")
            except Exception as e:
                logger.warning(f"Could not get message date range, will fetch all messages: {e}")
            
            # If no archived messages exist or we're in in-depth mode, get all messages in the time range
            if not earliest_date or not latest_date or self.in_depth:
                if self.in_depth:
                    logger.info(f"In-depth mode: Re-checking all messages in time range for #{channel.name}")
                else:
                    logger.info(f"No existing archives found for #{channel.name}. Getting all messages...")
                logger.info(f"Starting message fetch for #{channel.name} from {'oldest to newest' if self.oldest_first else 'newest to oldest'}...")
                try:
                    # We'll paginate through messages using before/after
                    last_message = None
                    while True:
                        history_kwargs = {
                            'limit': None,  # No limit, we'll control the flow ourselves
                            'oldest_first': self.oldest_first,
                            'before': last_message.created_at if last_message else None,
                            'after': cutoff_date if cutoff_date else None
                        }
                        
                        logger.info(f"Fetching messages for #{channel.name} with kwargs: {history_kwargs}")
                        current_batch = []
                        
                        try:
                            got_messages = False
                            async for message in channel.history(**{k: v for k, v in history_kwargs.items() if v is not None}):
                                got_messages = True
                                last_message = message
                                
                                # Process message
                                message_counter += 1
                                if message_counter % 25 == 0:
                                    logger.info(f"Fetched {message_counter} messages so far from #{channel.name}, last message from {message.created_at}")
                                
                                try:
                                    # Skip messages from the bot
                                    if message.author.id == self.bot_user_id:
                                        continue
                                    
                                    # In in-depth mode, always process the message
                                    # In normal mode, only process if not already in DB
                                    message_exists = await self._db_operation(
                                        lambda db: db.message_exists(message.id)
                                    )
                                    if self.in_depth or not message_exists:
                                        current_batch.append(message)
                                    
                                    # Store batch when it reaches the threshold
                                    if len(current_batch) >= 100:
                                        try:
                                            processed_messages = []
                                            for msg in current_batch:
                                                processed_msg = await self._process_message(msg, channel_id)
                                                if processed_msg:
                                                    processed_messages.append(processed_msg)
                                            
                                            if processed_messages:
                                                # Only increment counter for messages that didn't exist before
                                                pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                                    lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                                ))
                                                new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                                new_message_count += len(new_messages)
                                                
                                                logger.info(f"Storing batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                                                await self._db_operation(
                                                    lambda db: db.store_messages(processed_messages)
                                                )
                                            
                                            current_batch = []
                                            await asyncio.sleep(0.1)
                                        except Exception as e:
                                            logger.error(f"Failed to store batch: {e}")
                                            logger.error(f"Error details: {str(e)}")
                                            
                                except Exception as e:
                                    logger.error(f"Error processing message {message.id}: {e}")
                                    logger.error(f"Error details: {str(e)}")
                                    continue
                                
                        except discord.Forbidden:
                            logger.warning(f"Missing permissions to read messages in #{channel.name}")
                            break
                        except Exception as e:
                            logger.error(f"Error fetching messages: {e}")
                            break
                        
                        # Process any remaining messages in the current batch
                        if current_batch:
                            try:
                                processed_messages = []
                                for msg in current_batch:
                                    processed_msg = await self._process_message(msg, channel_id)
                                    if processed_msg:
                                        processed_messages.append(processed_msg)
                                
                                if processed_messages:
                                    # Only increment counter for messages that didn't exist before
                                    pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                        lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                    ))
                                    new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                    new_message_count += len(new_messages)
                                    
                                    logger.info(f"Storing final batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                                    await self._db_operation(
                                        lambda db: db.store_messages(processed_messages)
                                    )
                            except Exception as e:
                                logger.error(f"Failed to store final batch: {e}")
                                logger.error(f"Error details: {str(e)}")
                        
                        # If we didn't get any messages in this fetch, break the loop
                        if not got_messages:
                            logger.info(f"No more messages found in #{channel.name} for the current time range")
                            break
                            
                        await asyncio.sleep(0.1)
                    
                    logger.info(f"Finished initial fetch for #{channel.name}: {message_counter} messages fetched, last message from {last_message.created_at if last_message else 'N/A'}")
                except Exception as e:
                    logger.error(f"Error fetching message history: {e}")
                    logger.error(f"Error details: {str(e)}")
                    raise

            # Still check before earliest and after latest, respecting days limit
            if latest_date:
                logger.info(f"Searching for newer messages in #{channel.name} (after {latest_date})...")
                current_batch = []
                messages_found = 0
                async for message in channel.history(limit=None, after=latest_date, oldest_first=self.oldest_first):
                    messages_found += 1
                    if messages_found % 100 == 0:
                        logger.info(f"Found {messages_found} newer messages in #{channel.name}")
                    if cutoff_date and message.created_at < cutoff_date:
                        logger.debug(f"Reached cutoff date {cutoff_date}, stopping newer message search")
                        break
                    
                    # Skip messages from the bot
                    if message.author.id == self.bot_user_id:
                        continue
                        
                    current_batch.append(message)
                    message_counter += 1
                    
                    # Store batch when it reaches the threshold
                    if len(current_batch) >= 100:
                        try:
                            processed_messages = []
                            for msg in current_batch:
                                processed_msg = await self._process_message(msg, channel_id)
                                if processed_msg:
                                    processed_messages.append(processed_msg)
                            
                            if processed_messages:
                                # Only increment counter for messages that didn't exist before
                                pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                    lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                ))
                                new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                new_message_count += len(new_messages)
                                
                                logger.info(f"Storing batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                                await self._db_operation(
                                    lambda db: db.store_messages(processed_messages)
                                )
                            current_batch = []
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Failed to store batch: {e}")
                            logger.error(f"Error details: {str(e)}")
                
                # Process any remaining messages
                if current_batch:
                    try:
                        processed_messages = []
                        for msg in current_batch:
                            processed_msg = await self._process_message(msg, channel_id)
                            if processed_msg:
                                processed_messages.append(processed_msg)
                        
                        if processed_messages:
                            # Only increment counter for messages that didn't exist before
                            pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                            ))
                            new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                            new_message_count += len(new_messages)
                            
                            logger.info(f"Storing batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                            await self._db_operation(
                                lambda db: db.store_messages(processed_messages)
                            )
                    except Exception as e:
                        logger.error(f"Failed to store batch: {e}")
                        logger.error(f"Error details: {str(e)}")
            
            # Only search for older messages if we're not using --days flag
            if not self.days_limit and earliest_date:
                logger.info(f"Searching for older messages in #{channel.name} (before {earliest_date})...")
                current_batch = []
                messages_found = 0
                async for message in channel.history(limit=None, before=earliest_date, oldest_first=self.oldest_first):
                    messages_found += 1
                    if messages_found % 100 == 0:
                        logger.info(f"Found {messages_found} older messages in #{channel.name}")
                    if cutoff_date and message.created_at < cutoff_date:
                        continue
                        
                    # Skip messages from the bot
                    if message.author.id == self.bot_user_id:
                        continue
                        
                    current_batch.append(message)
                    message_counter += 1
                    
                    # Store batch when it reaches the threshold
                    if len(current_batch) >= 100:
                        try:
                            processed_messages = []
                            for msg in current_batch:
                                processed_msg = await self._process_message(msg, channel_id)
                                if processed_msg:
                                    processed_messages.append(processed_msg)
                            
                            if processed_messages:
                                # Only increment counter for messages that didn't exist before
                                pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                    lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                ))
                                new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                new_message_count += len(new_messages)
                                
                                logger.info(f"Storing batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                                await self._db_operation(
                                    lambda db: db.store_messages(processed_messages)
                                )
                            current_batch = []
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Failed to store batch: {e}")
                            logger.error(f"Error details: {str(e)}")
                
                # Process any remaining messages
                if current_batch:
                    try:
                        processed_messages = []
                        for msg in current_batch:
                            processed_msg = await self._process_message(msg, channel_id)
                            if processed_msg:
                                processed_messages.append(processed_msg)
                        
                        if processed_messages:
                            # Only increment counter for messages that didn't exist before
                            pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                            ))
                            new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                            new_message_count += len(new_messages)
                            
                            logger.info(f"Storing batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                            await self._db_operation(
                                lambda db: db.store_messages(processed_messages)
                            )
                    except Exception as e:
                        logger.error(f"Failed to store batch: {e}")
                        logger.error(f"Error details: {str(e)}")

            # Get all message dates to check for gaps
            message_dates = await self._db_operation(
                lambda db: db.get_message_dates(channel_id)
            )
            if message_dates:
                # Filter dates based on cutoff if set
                if cutoff_date:
                    message_dates = [d for d in message_dates if datetime.fromisoformat(d) >= cutoff_date]
                
                # Sort dates based on order setting
                message_dates.sort(reverse=not self.oldest_first)
                gaps = []
                for i in range(len(message_dates) - 1):
                    current = datetime.fromisoformat(message_dates[i])
                    next_date = datetime.fromisoformat(message_dates[i + 1])
                    # Compare dates based on order
                    date_diff = (next_date - current).days if self.oldest_first else (current - next_date).days
                    if date_diff > 7:
                        gaps.append((current, next_date) if self.oldest_first else (next_date, current))
                
                if gaps:
                    logger.info(f"Found {len(gaps)} gaps (>1 week) in message history for #{channel.name}")
                    for start, end in gaps:
                        gap_message_count = 0
                        current_batch = []
                        logger.info(f"Searching for messages in #{channel.name} between {start} and {end} (gap of {abs((end - start).days)} days)")
                        async for message in channel.history(limit=None, after=start, before=end, oldest_first=self.oldest_first):
                            # Skip messages from the bot
                            if message.author.id == self.bot_user_id:
                                continue
                                
                            current_batch.append(message)
                            gap_message_count += 1
                            
                            # Store batch when it reaches the threshold
                            if len(current_batch) >= 100:
                                try:
                                    processed_messages = []
                                    for msg in current_batch:
                                        processed_msg = await self._process_message(msg, channel_id)
                                        if processed_msg:
                                            processed_messages.append(processed_msg)
                                    
                                    if processed_messages:
                                        # Only increment counter for messages that didn't exist before
                                        pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                            lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                        ))
                                        new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                        new_message_count += len(new_messages)
                                        
                                        logger.info(f"Storing batch of {len(processed_messages)} messages from gap in #{channel.name} ({len(new_messages)} new)")
                                        await self._db_operation(
                                            lambda db: db.store_messages(processed_messages)
                                        )
                                        if gap_message_count % 100 == 0:
                                            logger.info(f"Found {gap_message_count} messages in current gap for #{channel.name}")
                                    
                                    current_batch = []
                                    await asyncio.sleep(0.1)
                                except Exception as e:
                                    logger.error(f"Failed to store batch: {e}")
                                    logger.error(f"Error details: {str(e)}")
                        
                        # Process any remaining messages from the gap
                        if current_batch:
                            try:
                                processed_messages = []
                                for msg in current_batch:
                                    processed_msg = await self._process_message(msg, channel_id)
                                    if processed_msg:
                                        processed_messages.append(processed_msg)
                                
                                if processed_messages:
                                    # Only increment counter for messages that didn't exist before
                                    pre_existing = set(msg['message_id'] for msg in await self._db_operation(
                                        lambda db: db.get_messages_by_ids([msg['message_id'] for msg in processed_messages])
                                    ))
                                    new_messages = [msg for msg in processed_messages if msg['message_id'] not in pre_existing]
                                    new_message_count += len(new_messages)
                                    
                                    logger.info(f"Storing final gap batch of {len(processed_messages)} messages from #{channel.name} ({len(new_messages)} new)")
                                    await self._db_operation(
                                        lambda db: db.store_messages(processed_messages)
                                    )
                            except Exception as e:
                                logger.error(f"Failed to store batch: {e}")
                                logger.error(f"Error details: {str(e)}")
                        
                        logger.info(f"Finished gap search in #{channel.name}, found {gap_message_count} messages")
            
            logger.info(f"Found {new_message_count} new messages to archive in #{channel.name}")
            logger.info(f"Archive complete - processed {new_message_count} new messages")
            self.total_messages_archived += new_message_count
            
            channel_duration = (datetime.now() - channel_start_time).total_seconds()
            logger.info(f"Finished archive of #{channel.name} in {channel_duration:.2f}s")
            
        except discord.HTTPException as e:
            if e.code == 429:  # Rate limit error
                logger.warning(f"Hit rate limit while processing #{channel.name}: {e}")
                retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
                logger.info(f"Waiting {retry_after}s before continuing")
                await asyncio.sleep(retry_after)
            else:
                logger.error(f"HTTP error in channel {channel.name}: {e}")
        except Exception as e:
            logger.error(f"Error archiving channel {channel.name}: {e}")
        finally:
            # Don't close the connection here as it's reused across channels
            pass

    def _ensure_db_connection(self):
        """Ensure database connection is alive and reconnect if needed."""
        try:
            # Test the connection
            if not self.db or not self.db.conn:
                logger.info("Database connection lost, reconnecting...")
                self.db = DatabaseHandler(self.db_path)
                self.db._init_db()
                logger.info("Successfully reconnected to database")
            else:
                # Test if connection is actually working
                self.db.cursor.execute("SELECT 1")
        except Exception as e:
            logger.warning(f"Database connection test failed, reconnecting: {e}")
            try:
                if self.db:
                    self.db.close()
                self.db = DatabaseHandler(self.db_path)
                self.db._init_db()
                logger.info("Successfully reconnected to database")
            except Exception as e:
                logger.error(f"Failed to reconnect to database: {e}")
                raise

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Archive Discord messages')
    parser.add_argument('--dev', action='store_true', help='Run in development mode')
    parser.add_argument('--order', choices=['newest', 'oldest'], default='newest',
                      help='Order to process messages (default: newest)')
    parser.add_argument('--days', type=int, help='Number of days of history to fetch (default: all)')
    parser.add_argument('--batch-size', type=int, default=100,
                      help='Number of messages to process in each batch (default: 100)')
    parser.add_argument('--in-depth', action='store_true',
                      help='Perform thorough message checks, re-processing all messages in the time range')
    parser.add_argument('--channel', type=int,
                      help='ID of a specific channel to archive')
    args = parser.parse_args()
    
    if args.dev:
        logger.info("Running in development mode")
    
    bot = None
    try:
        # Create new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        bot = MessageArchiver(dev_mode=args.dev, order=args.order, days=args.days, 
                            batch_size=args.batch_size, in_depth=args.in_depth,
                            channel_id=args.channel)
        
        # Start the bot and keep it running until archiving is complete
        async def runner():
            await bot.start(os.getenv('DISCORD_BOT_TOKEN'))
            # Wait for the bot to be ready and complete archiving
            while not bot.is_closed():
                await asyncio.sleep(1)
        
        # Run the bot until it completes
        loop.run_until_complete(runner())
        
    except discord.LoginFailure:
        logger.error("Failed to login. Please check your Discord token.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Ensure everything is cleaned up properly
        if bot:
            if not loop.is_closed():
                loop.run_until_complete(bot.close())

        # Clean up the event loop
        try:
            if not loop.is_closed():
                loop.run_until_complete(loop.shutdown_asyncgens())
                remaining_tasks = asyncio.all_tasks(loop)
                if remaining_tasks:
                    loop.run_until_complete(asyncio.gather(*remaining_tasks))
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot shutdown initiated by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}") 