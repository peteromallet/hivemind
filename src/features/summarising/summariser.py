# Standard library imports
import asyncio

import io
import json
import logging
import os
import re

import traceback
from datetime import datetime, timedelta
from typing import List, Tuple, Set, Dict, Optional, Any, Union
import sqlite3

import time

# Third-party imports
import aiohttp
import discord
from discord.ext import commands
from dotenv import load_dotenv

# Local imports
from src.common.db_handler import DatabaseHandler
from src.common.errors import *
from src.common.error_handler import ErrorHandler, handle_errors
from src.common.rate_limiter import RateLimiter
from src.common.log_handler import LogHandler
from src.common.base_bot import BaseDiscordBot

# Import the new summarizer that handles queries/Claude calls
from src.features.summarising.subfeatures.news_summary import NewsSummarizer
from src.features.summarising.subfeatures.top_generations import TopGenerations
from src.features.summarising.subfeatures.top_art_sharing import TopArtSharing


# Optional imports for media processing
try:
    from PIL import Image
    import moviepy.editor as mp
    MEDIA_PROCESSING_AVAILABLE = True
except ImportError:
    MEDIA_PROCESSING_AVAILABLE = False

################################################################################
# You may already have a scheduling function somewhere, but here is a simple stub:
################################################################################
async def schedule_daily_summary(bot):
    """
    Example stub for daily scheduled runs. 
    Adjust logic and scheduling library as appropriate to your environment.
    """
    while not bot._shutdown_flag:
        now_utc = datetime.utcnow()
        # Suppose we run at 10:00 UTC daily
        run_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
        if run_time < now_utc:
            run_time += timedelta(days=1)
        await asyncio.sleep((run_time - now_utc).total_seconds())
        
        if bot._shutdown_flag:
            break
        
        try:
            await bot.generate_summary()
        except Exception as e:
            bot.logger.error(f"Scheduled summary run failed: {e}")
        
        # Sleep 24h until next scheduled run:
        await asyncio.sleep(86400)

################################################################################

class ChannelSummarizerError(Exception):
    """Base exception class for ChannelSummarizer"""
    pass

class APIError(ChannelSummarizerError):
    """Raised when API calls fail"""
    pass

class DiscordError(ChannelSummarizerError):
    """Raised when Discord operations fail"""
    pass

class SummaryError(ChannelSummarizerError):
    """Raised when summary generation fails"""
    pass

class Attachment:
    def __init__(self, filename: str, data: bytes, content_type: str, reaction_count: int, username: str, content: str = "", jump_url: str = ""):
        self.filename = filename
        self.data = data
        self.content_type = content_type
        self.reaction_count = reaction_count
        self.username = username
        self.content = content
        self.jump_url = jump_url  # Add jump_url field

class AttachmentHandler:
    def __init__(self, max_size: int = 25 * 1024 * 1024):
        self.max_size = max_size
        self.attachment_cache: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger('ChannelSummarizer')
        
    def clear_cache(self):
        """Clear the attachment cache"""
        self.attachment_cache.clear()
        
    async def process_attachment(self, attachment: discord.Attachment, message: discord.Message, session: aiohttp.ClientSession, original_jump_url: str = None) -> Optional[Attachment]:
        """Process a single attachment with size and type validation."""
        try:
            cache_key = f"{message.channel.id}:{message.id}"
            
            # Use original_jump_url if provided (dev mode), otherwise use message.jump_url
            jump_url = original_jump_url if original_jump_url else message.jump_url

            async with session.get(attachment.url, timeout=300) as response:
                if response.status != 200:
                    raise APIError(f"Failed to download attachment: HTTP {response.status}")

                file_data = await response.read()
                if len(file_data) > self.max_size:
                    self.logger.warning(f"Skipping large file {attachment.filename} ({len(file_data)/1024/1024:.2f}MB)")
                    return None

                total_reactions = sum(reaction.count for reaction in message.reactions) if message.reactions else 0
                
                # Get guild display name (nickname) if available, otherwise use display name
                author_name = message.author.display_name
                if hasattr(message.author, 'guild'):
                    member = message.guild.get_member(message.author.id)
                    if member:
                        author_name = member.nick or member.display_name

                processed_attachment = Attachment(
                    filename=attachment.filename,
                    data=file_data,
                    content_type=attachment.content_type,
                    reaction_count=total_reactions,
                    username=author_name,  # Use the determined name
                    content=message.content,
                    jump_url=jump_url  # Use the correct jump URL
                )

                # Ensure the cache key structure is consistent
                if cache_key not in self.attachment_cache:
                    self.attachment_cache[cache_key] = {
                        'attachments': [],
                        'reaction_count': total_reactions,
                        'username': author_name,
                        'channel_id': str(message.channel.id)
                    }
                self.attachment_cache[cache_key]['attachments'].append(processed_attachment)

                return processed_attachment

        except Exception as e:
            self.logger.error(f"Failed to process attachment {attachment.filename}: {e}")
            self.logger.debug(traceback.format_exc())
            return None

    async def prepare_files(self, message_ids: List[str], channel_id: str) -> List[Tuple[discord.File, int, str, str]]:
        """Prepare Discord files from cached attachments."""
        files = []
        for message_id in message_ids:
            # Use composite key to look up attachments
            cache_key = f"{channel_id}:{message_id}"
            if cache_key in self.attachment_cache:
                for attachment in self.attachment_cache[cache_key]['attachments']:
                    try:
                        file = discord.File(
                            io.BytesIO(attachment.data),
                            filename=attachment.filename,
                            description=f"From message ID: {message_id} (🔥 {attachment.reaction_count} reactions)"
                        )
                        files.append((
                            file,
                            attachment.reaction_count,
                            message_id,
                            attachment.username
                        ))
                    except Exception as e:
                        self.logger.error(f"Failed to prepare file {attachment.filename}: {e}")
                        continue

        return sorted(files, key=lambda x: x[1], reverse=True)[:10]

    def get_all_files_sorted(self) -> List[Attachment]:
        """
        Retrieve all attachments sorted by reaction count in descending order.
        """
        all_attachments = []
        for channel_data in self.attachment_cache.values():
            all_attachments.extend(channel_data['attachments'])
        
        # Sort attachments by reaction_count in descending order
        sorted_attachments = sorted(all_attachments, key=lambda x: x.reaction_count, reverse=True)
        return sorted_attachments

class MessageFormatter:
    @staticmethod
    def format_usernames(usernames: List[str]) -> str:
        """Format a list of usernames with proper grammar and bold formatting."""
        unique_usernames = list(dict.fromkeys(usernames))
        if not unique_usernames:
            return ""
        
        formatted_usernames = []
        for username in unique_usernames:
            if not username.startswith('**'):
                username = f"**{username}**"
            formatted_usernames.append(username)
        
        if len(formatted_usernames) == 1:
            return formatted_usernames[0]
        
        return f"{', '.join(formatted_usernames[:-1])} and {formatted_usernames[-1]}"

    @staticmethod
    def chunk_content(content: str, max_length: int = 1900) -> List[Tuple[str, Set[str]]]:
        """Split content into chunks while preserving message links."""
        chunks = []
        current_chunk = ""
        current_chunk_links = set()

        for line in content.split('\n'):
            message_links = set(re.findall(r'https://discord\.com/channels/\d+/\d+/(\d+)', line))
            
            # Start new chunk if we hit an emoji or length limit
            if (any(line.startswith(emoji) for emoji in ['🎥', '💻', '🎬', '🤖', '📱', '🔧', '🎨', '📊']) and 
                current_chunk):
                if current_chunk:
                    chunks.append((current_chunk, current_chunk_links))
                current_chunk = ""
                current_chunk_links = set()
                current_chunk += '\n---\n\n'

            if len(current_chunk) + len(line) + 2 <= max_length:
                current_chunk += line + '\n'
                current_chunk_links.update(message_links)
            else:
                if current_chunk:
                    chunks.append((current_chunk, current_chunk_links))
                current_chunk = line + '\n'
                current_chunk_links = set(message_links)

        if current_chunk:
            chunks.append((current_chunk, current_chunk_links))

        return chunks

    def chunk_long_content(self, content: str, max_length: int = 1900) -> List[str]:
        """Split content into chunks that respect Discord's length limits."""
        chunks = []
        current_chunk = ""
        
        lines = content.split('\n')
        
        for line in lines:
            if len(current_chunk) + len(line) + 1 <= max_length:
                current_chunk += line + '\n'
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = line + '\n'
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

class ChannelSummarizer(BaseDiscordBot):
    def __init__(self, logger=None, dev_mode=False, command_prefix="!"):
        # Initialize intents first
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        intents.members = True
        intents.presences = True
        
        # Pass command_prefix to the base constructor
        super().__init__(command_prefix=command_prefix, intents=intents)
        
        self.logger = logger or logging.getLogger(__name__)
        self._dev_mode = dev_mode
        
        try:
            # Initialize handlers
            self.rate_limiter = RateLimiter()
            self.attachment_handler = AttachmentHandler()
            self.message_formatter = MessageFormatter()
            self.db = DatabaseHandler(dev_mode=dev_mode)
            self.error_handler = ErrorHandler(self)
            self.log_handler = LogHandler()
            
            # --- New: Instantiate the summarizer for queries to Claude, etc. ---
            # We remove direct Anthropic usage from here. 
            self.news_summarizer = NewsSummarizer(dev_mode=dev_mode)
            self.top_art_sharing = TopArtSharing(self)
            self.top_generations = TopGenerations(self)
            # ---------------------------------------------------------------
            
            # Initialize state variables
            self.guild_id = None
            self.summary_channel_id = None
            self.channels_to_monitor = []
            self.dev_channels_to_monitor = []
            self.first_message = None
            self._summary_lock = asyncio.Lock()
            self._cleanup_lock = asyncio.Lock()
            self._shutdown_flag = False
            self.current_summary_attachments = []
            self.approved_channels = []
            self.original_urls = {}
            
            self._channel_cache = {}
            self._last_cache_refresh = None
            self._cache_ttl = 300  # 5 minutes
            self._max_retries = 3
            self._retry_delay = 5  # seconds
            
            # Set initial dev mode (this triggers the setter that loads IDs, etc.)
            self.dev_mode = dev_mode
            
            self.register_events()
            
        except Exception as e:
            self.logger.error(f"Error during ChannelSummarizer initialization: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    def setup_logger(self, dev_mode):
        """Initialize or update logger configuration"""
        self.logger = self.log_handler.setup_logging(dev_mode)
        
        if self.logger:
            self.logger.info("Bot initializing...")
            if dev_mode:
                self.logger.debug("Development mode enabled")

    @property
    def dev_mode(self):
        return self._dev_mode

    @dev_mode.setter
    def dev_mode(self, value):
        """Set development mode and reconfigure logger"""
        if self._dev_mode != value:
            self._dev_mode = value
            self.setup_logger(value)

    def load_config(self):
        """Load configuration based on mode"""
        self.logger.debug("Loading configuration...")
        self.logger.debug(f"Current TEST_DATA_CHANNEL: {os.getenv('TEST_DATA_CHANNEL')}")
        
        load_dotenv(override=True)
        self.logger.debug(f"After reload TEST_DATA_CHANNEL: {os.getenv('TEST_DATA_CHANNEL')}")
        
        self.logger.debug("All channel-related environment variables:")
        for key, value in os.environ.items():
            if 'CHANNEL' in key:
                self.logger.debug(f"{key}: {value}")
        
        try:
            if self.dev_mode:
                self.logger.info("Loading development configuration")
                self.guild_id = int(os.getenv('DEV_GUILD_ID'))
                self.summary_channel_id = int(os.getenv('DEV_SUMMARY_CHANNEL_ID'))
                channels_str = os.getenv('DEV_CHANNELS_TO_MONITOR')
                if not channels_str:
                    raise ConfigurationError("DEV_CHANNELS_TO_MONITOR not found in environment")
                try:
                    self.dev_channels_to_monitor = [int(chan.strip()) for chan in channels_str.split(',') if chan.strip()]
                    self.logger.info(f"DEV_CHANNELS_TO_MONITOR: {self.dev_channels_to_monitor}")
                except ValueError as e:
                    raise ConfigurationError(f"Invalid channel ID in DEV_CHANNELS_TO_MONITOR: {e}")
            else:
                self.logger.info("Loading production configuration")
                self.guild_id = int(os.getenv('GUILD_ID'))
                self.summary_channel_id = int(os.getenv('PRODUCTION_SUMMARY_CHANNEL_ID'))
                channels_str = os.getenv('CHANNELS_TO_MONITOR')
                if not channels_str:
                    raise ConfigurationError("CHANNELS_TO_MONITOR not found in environment")
                try:
                    self.channels_to_monitor = [int(chan.strip()) for chan in channels_str.split(',') if chan.strip()]
                    self.logger.info(f"CHANNELS_TO_MONITOR: {self.channels_to_monitor}")
                except ValueError as e:
                    raise ConfigurationError(f"Invalid ID in CHANNELS_TO_MONITOR: {e}")
            
            self.logger.info(
                f"Configured with guild_id={self.guild_id}, "
                f"summary_channel={self.summary_channel_id}, "
                f"channels={self.channels_to_monitor if not self.dev_mode else self.dev_channels_to_monitor}"
            )
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise ConfigurationError(f"Failed to load configuration: {e}")

    def load_test_data(self) -> List[Dict[str, Any]]:
        """Load test data from test.json (not heavily used in current approach)."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            test_data_path = os.path.join(script_dir, 'test.json')
            
            if not os.path.exists(test_data_path):
                test_data = {
                    "messages": [
                        {
                            "content": "Test message 1",
                            "author": "test_user",
                            "timestamp": datetime.utcnow().isoformat(),
                            "attachments": [],
                            "reactions": 0,
                            "id": "1234567890"
                        }
                    ]
                }
                with open(test_data_path, 'w') as f:
                    json.dump(test_data, f, indent=2)
                self.logger.info(f"Created default test data at {test_data_path}")
                return test_data["messages"]
                
            with open(test_data_path, 'r') as f:
                data = json.load(f)
                return data.get("messages", [])
                
        except Exception as e:
            self.logger.error(f"Failed to load test data: {e}")
            return []

    async def setup_hook(self):
        """Called when the bot is starting up."""
        try:
            pass
        except Exception as e:
            raise ConfigurationError("Failed to initialize bot", e)

    async def on_ready(self):
        """Called when the bot is fully connected."""
        try:
            await super().on_ready()
            
            notification_channel = self.get_channel(self.summary_channel_id)
            if not notification_channel:
                self.logger.error(f"Could not find summary channel with ID {self.summary_channel_id}")
                self.logger.info("Available channels:")
                for guild in self.guilds:
                    for channel in guild.channels:
                        self.logger.info(f"- {channel.name} (ID: {channel.id})")
                return
            
            admin_user = await self.fetch_user(int(os.getenv('ADMIN_USER_ID')))
            self.error_handler = ErrorHandler(self)
            self.logger.info(f"Successfully initialized with summary channel: {notification_channel.name}")
            
        except Exception as e:
            self.logger.error(f"Error in on_ready: {e}")
            self.logger.debug(traceback.format_exc())

    async def get_channel_history(self, channel_id: int, db_handler: Optional[DatabaseHandler] = None) -> List[dict]:
        """Get message history for a channel from the database (past 24h)."""
        self.logger.info(f"Getting message history for channel {channel_id} from database")
        
        try:
            yesterday = datetime.utcnow() - timedelta(hours=24)
            
            def get_messages():
                # Create a new connection for this thread
                thread_local_db = DatabaseHandler(dev_mode=self.dev_mode)
                try:
                    # Get total message count
                    cursor = thread_local_db.conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM messages 
                        WHERE channel_id = ? 
                        AND created_at > ?
                    """, (channel_id, yesterday.isoformat()))
                    total_count = cursor.fetchone()[0]
                    cursor.close()

                    self.logger.info(f"Found {total_count} messages in past 24h for channel {channel_id}")

                    if total_count == 0:
                        return []

                    # Get all messages
                    thread_local_db.conn.row_factory = sqlite3.Row
                    cursor = thread_local_db.conn.cursor()
                    cursor.execute("""
                        SELECT m.*, 
                               COALESCE(mem.username, 'Unknown') as username,
                               COALESCE(mem.server_nick, mem.global_name, mem.username, 'Unknown') as display_name
                        FROM messages m
                        LEFT JOIN members mem ON m.author_id = mem.member_id
                        WHERE m.channel_id = ?
                        AND m.created_at > ?
                        AND (m.is_deleted IS NULL OR m.is_deleted = FALSE)
                        ORDER BY m.created_at DESC
                    """, (channel_id, yesterday.isoformat()))
                    
                    messages = [dict(row) for row in cursor.fetchall()]
                    cursor.close()
                    
                    formatted_messages = []
                    for msg in messages:
                        try:
                            attachments = json.loads(msg['attachments']) if msg['attachments'] else []
                            reactors = json.loads(msg['reactors']) if msg['reactors'] else []
                            
                            formatted_msg = {
                                'message_id': msg['message_id'],
                                'channel_id': msg['channel_id'],
                                'author_id': msg['author_id'],
                                'content': msg['content'],
                                'created_at': msg['created_at'],
                                'attachments': attachments,
                                'reaction_count': msg['reaction_count'],
                                'reactors': reactors,
                                'reference_id': msg['reference_id'],
                                'thread_id': msg['thread_id'],
                                'jump_url': msg['jump_url'],
                                'author_name': msg['display_name']
                            }
                            formatted_messages.append(formatted_msg)
                        except Exception as e:
                            self.logger.error(f"Error formatting message {msg.get('message_id')}: {e}")
                            self.logger.debug(traceback.format_exc())
                            continue

                    return formatted_messages
                finally:
                    thread_local_db.close()

            return await asyncio.get_event_loop().run_in_executor(None, get_messages)

        except Exception as e:
            self.logger.error(f"Error retrieving message history: {e}")
            self.logger.debug(traceback.format_exc())
            return []

    async def safe_send_message(self, channel, content=None, embed=None, file=None, files=None, reference=None):
        """Safely send a message with concurrency-limited retry logic."""
        try:
            send_task = self.rate_limiter.execute(
                f"channel_{channel.id}",
                channel.send(
                    content=content,
                    embed=embed,
                    file=file,
                    files=files,
                    reference=reference
                )
            )
            return await asyncio.wait_for(send_task, timeout=10)
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout sending message to channel {channel.id}")
            raise
        except discord.HTTPException as e:
            self.logger.error(f"HTTP error sending message: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            raise

    async def create_media_content(self, files: List[Tuple[discord.File, int, str, str]], max_media: int = 4) -> Optional[discord.File]:
        """Create a collage of images or a combined video, depending on attachments."""
        try:
            if not MEDIA_PROCESSING_AVAILABLE:
                self.logger.error("Media processing libraries are not available")
                return None
            
            self.logger.info(f"Starting media content creation with {len(files)} files")
            
            images = []
            videos = []
            has_audio = False
            
            for file_tuple, _, _, _ in files[:max_media]:
                file_tuple.fp.seek(0)
                data = file_tuple.fp.read()
                
                if file_tuple.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
                    self.logger.debug(f"Processing image: {file_tuple.filename}")
                    img = Image.open(io.BytesIO(data))
                    images.append(img)
                elif file_tuple.filename.lower().endswith(('.mp4', '.mov', '.webm')):
                    self.logger.debug(f"Processing video: {file_tuple.filename}")
                    temp_path = f'temp_{len(videos)}.mp4'
                    with open(temp_path, 'wb') as f:
                        f.write(data)
                    video = mp.VideoFileClip(temp_path)
                    if video.audio is not None:
                        has_audio = True
                        self.logger.debug(f"Video {file_tuple.filename} has audio")
                    videos.append(video)
            
            self.logger.info(f"Processed {len(images)} images and {len(videos)} videos. Has audio: {has_audio}")
                
            if videos and has_audio:
                self.logger.info("Creating combined video with audio")
                final_video = mp.concatenate_videoclips(videos)
                output_path = 'combined_video.mp4'
                final_video.write_videofile(output_path)
                
                for video in videos:
                    video.close()
                final_video.close()
                
                self.logger.info("Video combination complete")
                
                with open(output_path, 'rb') as f:
                    return discord.File(f, filename='combined_video.mp4')
                
            elif images or (videos and not has_audio):
                self.logger.info("Creating image/GIF collage")
                
                # Convert silent videos to GIF
                for i, video in enumerate(videos):
                    self.logger.debug(f"Converting silent video {i+1} to GIF")
                    gif_path = f'temp_gif_{len(images)}.gif'
                    video.write_gif(gif_path)
                    gif_img = Image.open(gif_path)
                    images.append(gif_img)
                    video.close()
                
                if not images:
                    self.logger.warning("No images available for collage")
                    return None
                
                n = len(images)
                if n == 1:
                    cols, rows = 1, 1
                elif n == 2:
                    cols, rows = 2, 1
                else:
                    cols, rows = 2, 2
                
                self.logger.debug(f"Creating {cols}x{rows} collage for {n} images")
                
                target_size = (800 // cols, 800 // rows)
                resized_images = []
                for i, img in enumerate(images):
                    self.logger.debug(f"Resizing image {i+1}/{len(images)} to {target_size}")
                    img = img.convert('RGB')
                    img.thumbnail(target_size)
                    resized_images.append(img)
                
                collage = Image.new('RGB', (800, 800))
                
                for idx, img in enumerate(resized_images):
                    x = (idx % cols) * (800 // cols)
                    y = (idx // cols) * (800 // rows)
                    collage.paste(img, (x, y))
                
                self.logger.info("Collage creation complete")
                
                buffer = io.BytesIO()
                collage.save(buffer, format='JPEG')
                buffer.seek(0)
                return discord.File(buffer, filename='collage.jpg')
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating media content: {e}")
            self.logger.debug(traceback.format_exc())
            return None
        finally:
            # Cleanup
            import os
            self.logger.debug("Cleaning up temporary files")
            for f in os.listdir():
                if f.startswith('temp_'):
                    try:
                        os.remove(f)
                        self.logger.debug(f"Removed temporary file: {f}")
                    except Exception as ex:
                        self.logger.warning(f"Failed to remove temporary file {f}: {ex}")

    async def create_summary_thread(self, message, thread_name, is_top_generations=False):
        try:
            self.logger.info(f"Attempting to create thread '{thread_name}' for message {message.id}")
            # If it's already a Thread object
            if isinstance(message, discord.Thread):
                self.logger.warning(f"Message is already a Thread object with ID {message.id}. Returning it directly.")
                return message

            if not message.guild:
                self.logger.error("Cannot create thread: message is not in a guild")
                return None

            bot_member = message.guild.get_member(self.user.id)
            if not bot_member:
                self.logger.error("Cannot find bot member in guild")
                return None

            self.logger.debug(f"Using channel: {message.channel} (ID: {message.channel.id}) for thread creation")
            required_permissions = ['create_public_threads', 'send_messages_in_threads', 'manage_messages']
            missing_permissions = [perm for perm in required_permissions if not getattr(message.channel.permissions_for(bot_member), perm, False)]
            if missing_permissions:
                self.logger.error(f"Missing required permissions in channel {message.channel.id}: {', '.join(missing_permissions)}")
                return None
            
            thread = await message.create_thread(
                name=thread_name,
                auto_archive_duration=1440  # 24 hours
            )
            
            if thread:
                self.logger.info(f"Successfully created thread: {thread.name} (ID: {thread.id})")
                
                # Only pin/unpin if this is not a top generations thread
                if not is_top_generations:
                    try:
                        pinned_messages = await message.channel.pins()
                        for pinned_msg in pinned_messages:
                            if pinned_msg.author.id == self.user.id:
                                await pinned_msg.unpin()
                                self.logger.info(f"Unpinned previous message: {pinned_msg.id}")
                    except Exception as e:
                        self.logger.error(f"Error unpinning previous messages: {e}")
                    
                    try:
                        await message.pin()
                        self.logger.info(f"Pinned new thread starter message: {message.id}")
                    except Exception as e:
                        self.logger.error(f"Error pinning new message: {e}")
                
                return thread
            else:
                self.logger.error("Thread creation returned None")
                return None
                
        except discord.Forbidden as e:
            self.logger.error(f"Forbidden error creating thread: {e}")
            self.logger.debug(traceback.format_exc())
            return None
        except discord.HTTPException as e:
            self.logger.error(f"HTTP error creating thread: {e}")
            self.logger.debug(traceback.format_exc())
            return None
        except Exception as e:
            self.logger.error(f"Error creating thread: {e}")
            self.logger.debug(traceback.format_exc())
            return None


    async def cleanup(self):
        """Cleanup resources properly"""
        async with self._cleanup_lock:
            if self._shutdown_flag:
                self.logger.warning("Cleanup already in progress")
                return
                
            self._shutdown_flag = True
            
            try:
                self.logger.info("Starting cleanup...")
                
                # Close internal HTTP session if it exists
                if hasattr(self, 'http') and hasattr(self.http, '_session') and self.http._session:
                    if not self.http._session.closed:
                        self.logger.info("Closing internal HTTP session (self.http._session)...")
                        try:
                            await self.http._session.close()
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            self.logger.error(f"Error closing self.http._session: {e}")
                        finally:
                            self.http._session = None
                
                # Close DB
                if hasattr(self, 'db'):
                    self.logger.info("Closing database connections...")
                    try:
                        self.db.close()
                    except Exception as e:
                        self.logger.error(f"Error closing database: {e}")
                    finally:
                        self.db = None  # Clear reference
                
                # Let base class close the Discord connection
                self.logger.info("Closing Discord connection...")
                try:
                    await super().cleanup()
                except Exception as e:
                    self.logger.error(f"Error in Discord cleanup: {e}")
                    self.logger.debug(traceback.format_exc())
                
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
                self.logger.debug(traceback.format_exc())
            finally:
                self._shutdown_flag = False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure cleanup runs when using async context manager"""
        await self.cleanup()

    def is_forum_channel(self, channel_id: int) -> bool:
        """Check if a channel is a forum channel."""
        channel = self.get_channel(channel_id)
        return isinstance(channel, discord.ForumChannel)

    async def _wait_for_connection(self, timeout=30):
        """Wait for the bot to be fully connected."""
        start_time = time.time()
        while not self.is_ready():
            if time.time() - start_time > timeout:
                raise TimeoutError("Timed out waiting for bot to be ready")
            await asyncio.sleep(1)
        
        if not self._last_heartbeat:
            self._last_heartbeat = datetime.now()
        elif (datetime.now() - self._last_heartbeat).total_seconds() > 30:
            self.logger.warning("No recent heartbeat detected, proceeding and resetting heartbeat")
            self._last_heartbeat = datetime.now()

    async def _get_channel_with_retry(self, channel_id: int) -> Optional[discord.TextChannel]:
        """Get a channel with retry logic and caching."""
        now = time.time()
        if channel_id in self._channel_cache:
            cache_entry = self._channel_cache[channel_id]
            if now - cache_entry['timestamp'] < self._cache_ttl:
                return cache_entry['channel']
        
        for attempt in range(self._max_retries):
            try:
                await self._wait_for_connection()
                channel = self.get_channel(channel_id)
                if channel:
                    self._channel_cache[channel_id] = {
                        'channel': channel,
                        'timestamp': now
                    }
                    return channel
                
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay)
            except (TimeoutError, ConnectionError) as e:
                self.logger.warning(f"Connection issue on attempt {attempt + 1}/{self._max_retries}: {e}")
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay)
            except Exception as e:
                self.logger.error(f"Unexpected error getting channel on attempt {attempt + 1}/{self._max_retries}: {e}")
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay)
        
        return None

    async def _execute_db_operation(self, operation, *args, db_handler=None):
        """Execute a database operation in a thread pool to avoid blocking the event loop."""
        loop = asyncio.get_running_loop()
        try:
            await self._wait_for_connection()
            def thread_safe_operation(*operation_args):
                thread_db = db_handler or DatabaseHandler(dev_mode=self.dev_mode)
                conn = thread_db._get_connection()
                try:
                    result = operation(thread_db, *operation_args)
                    conn.commit()
                    return result
                except Exception as e:
                    conn.rollback()
                    raise e
            
            return await loop.run_in_executor(None, thread_safe_operation, *args)
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def _post_summary_with_transaction(self, channel_id: int, summary: str, messages: list, current_date: datetime, db_handler: DatabaseHandler) -> bool:
        """Atomic operation for posting summary and updating database"""
        try:
            # Generate a short summary using the NewsSummarizer
            short_summary = await self.news_summarizer.generate_short_summary(summary, len(messages))
            
            def transaction(db, short_sum):
                conn = db._get_connection()
                try:
                    def _transaction_operation(_conn):
                        cursor = _conn.cursor()
                        try:
                            cursor.execute("""
                                INSERT INTO daily_summaries (
                                    date, channel_id, full_summary, short_summary, created_at, status
                                ) VALUES (?, ?, ?, ?, datetime('now'), 'pending')
                            """, (
                                current_date.strftime('%Y-%m-%d'),
                                channel_id,
                                summary,
                                short_sum,
                            ))
                            summary_id = cursor.lastrowid
                            
                            cursor.execute("""
                                UPDATE daily_summaries 
                                SET status = 'completed' 
                                WHERE daily_summary_id = ?
                            """, (summary_id,))
                            
                            conn.commit()
                            return True
                        except Exception as e:
                            conn.rollback()
                            raise e
                    
                    return db._execute_with_retry(_transaction_operation)
                except Exception as e:
                    self.logger.error(f"Transaction failed: {e}")
                    raise
            
            return await self._execute_db_operation(transaction, short_summary, db_handler=db_handler)
            
        except Exception as e:
            self.logger.error(f"Error in _post_summary_with_transaction: {e}")
            self.logger.debug(traceback.format_exc())
            return False

    async def generate_summary(self):
        """
        Generate and post summaries following these steps:
        1) Generate individual channel summaries and post to their channels (except for forum channels)
        2) Combine channel summaries for overall summary
        3) Post overall summary to summary channel
        4) Post top generations
        5) Post top art sharing
        """
        try:
            async with self._summary_lock:
                self.logger.info("Generating requested summary...")
                db_handler = DatabaseHandler(dev_mode=self.dev_mode)
                try:
                    def add_columns(db):
                        cursor = None
                        try:
                            cursor = db.conn.cursor()
                            cursor.execute("""
                                ALTER TABLE daily_summaries 
                                ADD COLUMN status TEXT DEFAULT 'completed'
                            """)
                            cursor.execute("""
                                ALTER TABLE daily_summaries 
                                ADD COLUMN error TEXT
                            """)
                        except Exception:
                            pass
                        finally:
                            if cursor:
                                cursor.close()
                    
                    await self._execute_db_operation(add_columns)

                    summary_channel = await self._get_channel_with_retry(self.summary_channel_id)
                    if not summary_channel:
                        self.logger.error(f"Could not find summary channel {self.summary_channel_id} after {self._max_retries} attempts")
                        return
                    
                    current_date = datetime.utcnow()

                    # We'll handle channel picking ourselves:
                    if self.dev_mode:
                        active_channels = await self._get_dev_mode_channels(db_handler)
                    else:
                        active_channels = await self._get_production_channels(db_handler)
                    
                    if not active_channels:
                        self.logger.warning("No active channels found")
                        return

                    channel_summaries = []
                    self.logger.info("Processing individual summaries for channels with 25+ messages:")
                    
                    for channel_info in active_channels:
                        channel_id = channel_info['channel_id']
                        post_channel_id = channel_info.get('post_channel_id', channel_id)
                        
                        try:
                            messages = await self.get_channel_history(channel_id, db_handler)
                            if not messages:
                                continue
                            
                            # === Call into NewsSummarizer to summarize these messages ===
                            channel_summary = await self.news_summarizer.generate_news_summary(messages)
                            if not channel_summary or channel_summary in [
                                "[NOTHING OF NOTE]", 
                                "[NO SIGNIFICANT NEWS]",
                                "[NO MESSAGES TO ANALYZE]"
                            ]:
                                continue
                            
                            # Post to the channel (unless it's a forum)
                            if not self.is_forum_channel(post_channel_id):
                                channel_obj = await self._get_channel_with_retry(post_channel_id)
                                if channel_obj:
                                    formatted_summary = self.news_summarizer.format_news_for_discord(channel_summary)
                                    loop = asyncio.get_running_loop()
                                    def get_existing_thread_id():
                                        def op(conn):
                                            cursor = conn.cursor()
                                            query = """
                                                SELECT summary_thread_id 
                                                FROM channel_summary 
                                                WHERE channel_id = ? 
                                                AND strftime('%Y-%m', created_at) = strftime('%Y-%m', CURRENT_TIMESTAMP)
                                                ORDER BY created_at DESC LIMIT 1
                                            """
                                            cursor.execute(query, (channel_id,))
                                            row = cursor.fetchone()
                                            cursor.close()
                                            return row[0] if row and row[0] else None
                                        return db_handler._execute_with_retry(op)
                                    
                                    existing_thread_id = await loop.run_in_executor(None, get_existing_thread_id)
                                    thread = None
                                    if existing_thread_id:
                                        max_retries = 3
                                        retry_delay = 1
                                        for attempt in range(max_retries):
                                            try:
                                                thread = await self.fetch_channel(existing_thread_id)
                                                break
                                            except discord.NotFound:
                                                self.logger.warning(f"Thread {existing_thread_id} not found, will create new one")
                                                break
                                            except (discord.HTTPException, discord.Forbidden) as e:
                                                if attempt < max_retries - 1:
                                                    self.logger.warning(f"Error fetching thread {existing_thread_id} (attempt {attempt + 1}/{max_retries}): {e}")
                                                    await asyncio.sleep(retry_delay * (attempt + 1))
                                                else:
                                                    self.logger.error(f"Failed to fetch thread {existing_thread_id} after {max_retries} attempts: {e}")
                                                    return
                                    
                                    # Create new thread if we don't have one yet (either no existing_thread_id or failed to fetch)
                                    if not thread:
                                        self.logger.info(f"No existing thread found for channel {channel_id}, creating new one")
                                        thread_title = f"#{channel_obj.name} - Monthly Summary - {current_date.strftime('%B, %Y')}"
                                        self.logger.info(f"Attempting to send header message with title: {thread_title}")
                                        summary_message = await self.safe_send_message(channel_obj, f"Summary thread for {current_date.strftime('%B, %Y')}")
                                        if summary_message:
                                            self.logger.info(f"Header message sent to channel {channel_id}. Attempting to create a new thread with title: {thread_title}")
                                            thread = await self.create_summary_thread(summary_message, thread_title)
                                            if thread:
                                                self.logger.info(f"Successfully created summary thread for channel {channel_id}: {thread.id}")
                                                await loop.run_in_executor(None, db_handler.update_summary_thread, channel_id, thread.id)
                                            else:
                                                self.logger.error(f"Failed to create thread for channel {channel_id} - create_summary_thread returned None")
                                        else:
                                            self.logger.error(f"Failed to send header message to channel {channel_id}")
                                    
                                    if thread:
                                        self.logger.info(f"Using summary thread in channel {post_channel_id}: {thread.id}")
                                        # Post date headline first and capture the header message
                                        date_headline = f"# {current_date.strftime('%A, %B %d, %Y')}\n"
                                        header_msg = await self.safe_send_message(thread, date_headline)
                                        await asyncio.sleep(1)
                                        # Post each portion of the summary
                                        for item in formatted_summary:
                                            await self.safe_send_message(thread, item['content'])
                                            await asyncio.sleep(1)
                                        # Post top gens for the specific channel into this thread
                                        await self.top_generations.post_top_gens_for_channel(thread, channel_id)
                                        # Generate and post short summary with link back using the header message's id
                                        short_summary = await self.news_summarizer.generate_short_summary(channel_summary, len(messages))
                                        link = f"https://discord.com/channels/{channel_obj.guild.id}/{thread.id}/{header_msg.id}"
                                        await self.safe_send_message(thread, f"\n---\n\n***Click here to jump to the beginning of today's summary:***{link}")
                                        channel_header = f"**### Channel summary for {current_date.strftime('%A, %B %d, %Y')}**"
                                        await self.safe_send_message(channel_obj, f"{channel_header}{short_summary}\n[Click here to jump to the summary thread]({link})")
                                    else:
                                        self.logger.error(f"Failed to create or fetch thread for channel {channel_id}")
                            
                            # Store it in DB
                            success = await self._post_summary_with_transaction(
                                channel_id,
                                channel_summary,
                                messages,
                                current_date,
                                db_handler
                            )
                            if success:
                                channel_summaries.append(channel_summary)
                        except Exception as e:
                            self.logger.error(f"Error processing channel {channel_id}: {e}")
                            self.logger.debug(traceback.format_exc())
                            continue

                    # Combine them
                    if channel_summaries:
                        self.logger.info(f"Combining summaries from {len(channel_summaries)} channels...")
                        overall_summary = await self.news_summarizer.combine_channel_summaries(channel_summaries)
                        
                        if overall_summary and overall_summary not in [
                            "[NOTHING OF NOTE]", 
                            "[NO SIGNIFICANT NEWS]",
                            "[NO MESSAGES TO ANALYZE]"
                        ]:
                            formatted_summary = self.news_summarizer.format_news_for_discord(overall_summary)
                            header = await self.safe_send_message(summary_channel, f"\n\n# Daily Summary - {current_date.strftime('%A, %B %d, %Y')}\n\n")
                            if header is not None:
                                self.first_message = header
                            else:
                                self.logger.error("Failed to post header message; first_message remains unset.")
                            
                            self.logger.info("Posting main summary to summary channel")
                            for item in formatted_summary:
                                await self.safe_send_message(summary_channel, item['content'])
                                await asyncio.sleep(1)
                        else:
                            await self.safe_send_message(summary_channel, "_No significant activity to summarize in the last 24 hours._")
                    else:
                        await self.safe_send_message(summary_channel, "_No messages found in the last 24 hours for overall summary._")

                except Exception as e:
                    self.logger.error(f"Claude API / summarization error during summary generation: {e}")
                    await self.safe_send_message(summary_channel, "⚠️ Unable to generate summary due to error. Check logs.")

                # Step 4) Post top generations
                await self.top_generations.post_top_x_generations(summary_channel, limit=4)

                # Step 5) Post top art sharing
                await self.top_art_sharing.post_top_art_share(summary_channel)
                
                # Link back to the start
                self.logger.info("Attempting to send link back to start...")
                if self.first_message:
                    self.logger.info(f"First message exists with ID: {self.first_message.id}")
                    link_to_start = f"https://discord.com/channels/{self.first_message.guild.id}/{self.first_message.channel.id}/{self.first_message.id}"
                    self.logger.info(f"Generated link: {link_to_start}")
                    await self.safe_send_message(summary_channel, f"\n---\n\n***Click here to jump to the beginning of today's summary:*** {link_to_start}")
                    self.logger.info("Sent link back to start message")
                else:
                    self.logger.warning("No first_message found, cannot send link back")

        except Exception as e:
            self.logger.error(f"Critical error in summary generation: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def _get_dev_mode_channels(self, db_handler):
        """Get active channels for dev mode"""
        try:
            test_channels_str = os.getenv('TEST_DATA_CHANNEL')
            if not test_channels_str:
                self.logger.error("TEST_DATA_CHANNEL not set in environment")
                return []
            
            test_channel_ids = [int(cid.strip()) for cid in test_channels_str.split(',') if cid.strip()]
            if not test_channel_ids:
                self.logger.error("No valid channel IDs found in TEST_DATA_CHANNEL")
                return []

            # Retrieve DEV_CHANNELS_TO_MONITOR from already loaded configuration
            dev_channels = self.dev_channels_to_monitor
            if not dev_channels:
                self.logger.error("DEV_CHANNELS_TO_MONITOR not set or empty in environment")
                return []

            # Replacing the channel_query assignment to fix linter errors
            channel_query = (
                "SELECT c.channel_id, c.channel_name, COALESCE(c2.channel_name, 'Unknown') as source, "
                "COUNT(m.message_id) as msg_count "
                "FROM channels c "
                "LEFT JOIN channels c2 ON c.category_id = c2.channel_id "
                "LEFT JOIN messages m ON c.channel_id = m.channel_id "
                "AND m.created_at > datetime('now', '-24 hours') "
                "WHERE c.channel_id IN ({}) OR c.category_id IN ({}) "
                "GROUP BY c.channel_id, c.channel_name, source "
                "HAVING COUNT(m.message_id) >= 25 "
                "ORDER BY msg_count DESC"
            ).format(
                ",".join(str(cid) for cid in test_channel_ids),
                ",".join(str(cid) for cid in test_channel_ids)
            )
            loop = asyncio.get_running_loop()
            def db_operation():
                try:
                    db_handler.conn.execute("PRAGMA busy_timeout = 5000")
                    db_handler.conn.row_factory = sqlite3.Row
                    cursor = db_handler.conn.cursor()
                    cursor.execute(channel_query)
                    results = [dict(row) for row in cursor.fetchall()]
                    cursor.close()
                    db_handler.conn.row_factory = None

                    # Begin mapping: override each result's post_channel_id based on DEV_CHANNELS_TO_MONITOR
                    if dev_channels:
                        if len(dev_channels) == len(test_channel_ids):
                            # Map by index: for each result, find its index in test_channel_ids and assign corresponding dev channel
                            for result in results:
                                try:
                                    idx = test_channel_ids.index(result['channel_id'])
                                    result['post_channel_id'] = dev_channels[idx]
                                except ValueError:
                                    result['post_channel_id'] = dev_channels[0]
                        elif len(dev_channels) == 1:
                            for result in results:
                                result['post_channel_id'] = dev_channels[0]
                        else:
                            # In case multiple dev channels exist but count doesn't match, default to first one
                            for result in results:
                                result['post_channel_id'] = dev_channels[0]
                    else:
                        for result in results:
                            result['post_channel_id'] = result['channel_id']

                    return results
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        self.logger.error("Database lock timeout exceeded")
                    else:
                        self.logger.error(f"Database operational error: {e}")
                    return []
                except Exception as e:
                    self.logger.error(f"Error getting active channels: {e}")
                    return []

            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, db_operation),
                    timeout=10
                )
            except asyncio.TimeoutError:
                self.logger.error("Timeout while executing database query")
                return []
            except Exception as e:
                self.logger.error(f"Error executing database query: {e}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting active channels: {e}")
            return []

    async def _get_production_channels(self, db_handler):
        """Get active channels for production mode"""
        try:
            channel_query = """
                SELECT 
                    c.channel_id,
                    c.channel_name,
                    COALESCE(c2.channel_name, 'Unknown') as source,
                    COUNT(m.message_id) as msg_count
                FROM channels c
                LEFT JOIN channels c2 ON c.category_id = c2.channel_id
                LEFT JOIN messages m ON c.channel_id = m.channel_id
                    AND m.created_at > datetime('now', '-24 hours')
                WHERE c.channel_id IN ({})
                    OR c.category_id IN ({})
                GROUP BY c.channel_id, c.channel_name, source
                HAVING COUNT(m.message_id) >= 25
                ORDER BY msg_count DESC
            """.format(
                ",".join((str(cid) for cid in self.channels_to_monitor)),
                ",".join((str(cid) for cid in self.channels_to_monitor))
            
            loop = asyncio.get_running_loop()
            def db_operation():
                try:
                    db_handler.conn.execute("PRAGMA busy_timeout = 5000")
                    db_handler.conn.row_factory = sqlite3.Row
                    cursor = db_handler.conn.cursor()
                    cursor.execute(channel_query)
                    results = [dict(row) for row in cursor.fetchall()]
                    cursor.close()
                    db_handler.conn.row_factory = None
                    return results
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        self.logger.error("Database lock timeout exceeded")
                    else:
                        self.logger.error(f"Database operational error: {e}")
                    return []
                except Exception as e:
                    self.logger.error(f"Error getting active channels: {e}")
                    return []

            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, db_operation),
                    timeout=10
                )
            except asyncio.TimeoutError:
                self.logger.error("Timeout while executing database query")
                return []
            except Exception as e:
                self.logger.error(f"Error executing database query: {e}")
                return []
        except Exception as e:
            self.logger.error(f"Error getting active channels: {e}")
            return []

    async def setup_discord(self):
        """Initialize Discord connection and event handlers."""
        try:
            @self.discord_client.event
            async def on_disconnect():
                try:
                    self.logger.warning("Discord client disconnected. Attempting to reconnect in 5 seconds...")
                    await asyncio.sleep(5)
                    
                    loop = asyncio.get_running_loop()
                    
                    await self.discord_client.close()
                    
                    intents = discord.Intents.default()
                    intents.message_content = True
                    intents.guilds = True
                    intents.messages = True
                    intents.members = True
                    intents.presences = True
                    
                    self.discord_client = commands.Bot(
                        command_prefix="!",
                        intents=intents,
                        heartbeat_timeout=60.0,
                        guild_ready_timeout=10.0,
                        gateway_queue_size=512,
                        loop=loop
                    )
                    
                    await self.setup_discord()
                    
                    await self.discord_client.start(self.discord_token)
                    self.logger.info("Reconnection successful")
                except Exception as e:
                    self.logger.error(f"Error during reconnection: {e}")
                    self.logger.error(traceback.format_exc())
        except Exception as e:
            self.logger.error(f"Error in setup_discord: {e}")
            self.logger.error(traceback.format_exc())

    def register_events(self):
        """Register event handlers for the bot."""
        try:
            pass
        except Exception as e:
            self.logger.error(f"Error registering events: {e}")
            self.logger.error(traceback.format_exc())

if __name__ == "__main__":
    def main():
        pass
    main()
