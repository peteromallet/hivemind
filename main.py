import discord
import anthropic
from datetime import datetime, timedelta, timezone
import asyncio
import os
from discord.ext import commands
from dotenv import load_dotenv
import io
import aiohttp
import argparse
import re
import logging
import traceback
import random
from typing import List, Tuple, Set, Dict, Optional, Any
from dataclasses import dataclass
from src.db_handler import DatabaseHandler
from utils.errors import *
from utils.error_handler import ErrorHandler, handle_errors
import json
from logging.handlers import RotatingFileHandler

# Configure logging
def setup_logging(dev_mode=False):
    """Configure logging with separate files for dev/prod and rotation"""
    logger = logging.getLogger('ChannelSummarizer')
    logger.setLevel(logging.DEBUG if dev_mode else logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if dev_mode else logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Production file handler (always active)
    prod_log_file = 'discord_bot.log'
    prod_handler = RotatingFileHandler(
        prod_log_file,
        maxBytes=1024 * 1024,  # 1MB per file
        backupCount=5,
        encoding='utf-8'
    )
    prod_handler.setLevel(logging.INFO)
    prod_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    prod_handler.setFormatter(prod_formatter)
    logger.addHandler(prod_handler)
    
    # Development file handler (only when in dev mode)
    if dev_mode:
        dev_log_file = 'discord_bot_dev.log'
        dev_handler = LineCountRotatingFileHandler(
            dev_log_file,
            maxBytes=512 * 1024,  # 512KB per file
            backupCount=200,
            encoding='utf-8',
            max_lines=200
        )
        dev_handler.setLevel(logging.DEBUG)
        dev_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        dev_handler.setFormatter(dev_formatter)
        
        # Only do rollover if file exists AND exceeds limits
        if os.path.exists(dev_log_file):
            if os.path.getsize(dev_log_file) > 512 * 1024:
                dev_handler.doRollover()
            else:
                # Count lines in existing file
                with open(dev_log_file, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f)
                if line_count > 200:
                    dev_handler.doRollover()
        
        logger.addHandler(dev_handler)
    
    # Log the logging configuration
    logger.info(f"Logging configured in {'development' if dev_mode else 'production'} mode")
    logger.info(f"Production log file: {prod_log_file}")
    if dev_mode:
        logger.info(f"Development log file: {dev_log_file}")
    
    return logger

class LineCountRotatingFileHandler(RotatingFileHandler):
    """A handler that rotates based on both size and line count"""
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, 
                 encoding=None, delay=False, max_lines=None):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.max_lines = max_lines
        
        # Initialize line count from existing file
        if os.path.exists(filename):
            with open(filename, 'r', encoding=encoding or 'utf-8') as f:
                self.line_count = sum(1 for _ in f)
        else:
            self.line_count = 0
    
    def doRollover(self):
        """Override doRollover to keep last N lines"""
        if self.stream:
            self.stream.close()
            self.stream = None
            
        if self.max_lines:
            # Read all lines from the current file
            with open(self.baseFilename, 'r', encoding=self.encoding) as f:
                lines = f.readlines()
            
            # Keep only the last max_lines
            lines = lines[-self.max_lines:]
            
            # Write the last max_lines back to the file
            with open(self.baseFilename, 'w', encoding=self.encoding) as f:
                f.writelines(lines)
            
            self.line_count = len(lines)
        
        if not self.delay:
            self.stream = self._open()
    
    def emit(self, record):
        """Emit a record and check line count"""
        if self.max_lines and self.line_count >= self.max_lines:
            self.doRollover()
            
        super().emit(record)
        self.line_count += 1

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

class RateLimiter:
    """Manages rate limiting for Discord API calls with exponential backoff."""
    
    def __init__(self):
        self.backoff_times = {}  # Store backoff times per channel
        self.base_delay = 1.0    # Base delay in seconds
        self.max_delay = 64.0    # Maximum delay in seconds
        self.jitter = 0.1        # Random jitter factor
        
    async def execute(self, key, coroutine):
        """
        Executes a coroutine with rate limit handling.
        
        Args:
            key: Identifier for the rate limit (e.g., channel_id)
            coroutine: The coroutine to execute
            
        Returns:
            The result of the coroutine execution
        """
        max_retries = 5
        attempt = 0
        
        while attempt < max_retries:
            try:
                # Add jitter to prevent thundering herd
                if key in self.backoff_times:
                    jitter = random.uniform(-self.jitter, self.jitter)
                    await asyncio.sleep(self.backoff_times[key] * (1 + jitter))
                
                result = await coroutine
                
                # Reset backoff on success
                self.backoff_times[key] = self.base_delay
                return result
                
            except discord.HTTPException as e:
                attempt += 1
                
                if e.status == 429:  # Rate limit hit
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else None
                    
                    if retry_after:
                        self.logger.warning(f"Rate limit hit for {key}. Retry after {retry_after}s")
                        await asyncio.sleep(retry_after)
                    else:
                        # Calculate exponential backoff
                        current_delay = self.backoff_times.get(key, self.base_delay)
                        next_delay = min(current_delay * 2, self.max_delay)
                        self.backoff_times[key] = next_delay
                        
                        self.logger.warning(f"Rate limit hit for {key}. Using exponential backoff: {next_delay}s")
                        await asyncio.sleep(next_delay)
                        
                elif attempt == max_retries:
                    self.logger.error(f"Failed after {max_retries} attempts: {e}")
                    raise
                else:
                    self.logger.warning(f"Discord API error (attempt {attempt}/{max_retries}): {e}")
                    # Calculate exponential backoff for other errors
                    current_delay = self.backoff_times.get(key, self.base_delay)
                    next_delay = min(current_delay * 2, self.max_delay)
                    self.backoff_times[key] = next_delay
                    await asyncio.sleep(next_delay)
            
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                self.logger.debug(traceback.format_exc())
                raise

class Attachment:
    def __init__(self, filename: str, data: bytes, content_type: str, reaction_count: int, username: str):
        self.filename = filename
        self.data = data
        self.content_type = content_type
        self.reaction_count = reaction_count
        self.username = username

class AttachmentHandler:
    def __init__(self, max_size: int = 25 * 1024 * 1024):
        self.max_size = max_size
        self.attachment_cache: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger('ChannelSummarizer')
        
    def clear_cache(self):
        """Clear the attachment cache"""
        self.attachment_cache.clear()
        
    async def process_attachment(self, attachment: discord.Attachment, message: discord.Message, session: aiohttp.ClientSession) -> Optional[Attachment]:
        """Process a single attachment with size and type validation."""
        try:
            # Remove excessive debug logging
            cache_key = f"{message.channel.id}:{message.id}"
            
            async with session.get(attachment.url, timeout=300) as response:
                if response.status != 200:
                    raise APIError(f"Failed to download attachment: HTTP {response.status}")

                file_data = await response.read()
                if len(file_data) > self.max_size:
                    self.logger.warning(f"Skipping large file {attachment.filename} ({len(file_data)/1024/1024:.2f}MB)")
                    return None

                total_reactions = sum(reaction.count for reaction in message.reactions) if message.reactions else 0

                processed_attachment = Attachment(
                    filename=attachment.filename,
                    data=file_data,
                    content_type=attachment.content_type,
                    reaction_count=total_reactions,
                    username=message.author.name
                )

                if cache_key not in self.attachment_cache:
                    self.attachment_cache[cache_key] = {
                        'attachments': [],
                        'reaction_count': total_reactions,
                        'username': message.author.name,
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
        
        Returns:
            List[Attachment]: Sorted list of Attachment objects.
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
        
        # Add bold formatting if not already present
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
            
            # Start new chunk if we hit emoji or length limit
            if (any(line.startswith(emoji) for emoji in ['🎥', '💻', '🎬', '🤖', '📱', '💡', '🔧', '🎨', '📊']) and 
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
        
        # Split by lines to avoid breaking mid-sentence
        lines = content.split('\n')
        
        for line in lines:
            if len(current_chunk) + len(line) + 1 <= max_length:
                current_chunk += line + '\n'
            else:
                # If current chunk is not empty, add it to chunks
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = line + '\n'
        
        # Add the last chunk if not empty
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

class ChannelSummarizer(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        intents.members = True
        intents.presences = True

        super().__init__(
            command_prefix="!", 
            intents=intents,
            heartbeat_timeout=60.0,
            guild_ready_timeout=10.0,
            gateway_queue_size=512
        )
        
        # Remove initial dev_mode setting and logger setup
        self._dev_mode = None
        self.logger = None
        
        self.claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        self.session = None
        
        self.rate_limiter = RateLimiter()
        self.attachment_handler = AttachmentHandler()
        self.message_formatter = MessageFormatter()
        self.db = DatabaseHandler()
        self.error_handler = ErrorHandler()
        
        self.guild_id = None
        self.summary_channel_id = None
        self.channels_to_monitor = []
        self.dev_channels_to_monitor = []
        self.first_message = None

    def setup_logger(self, dev_mode):
        """Initialize or update logger configuration"""
        # Always recreate logger when explicitly setting up
        self.logger = setup_logging(dev_mode)
        
        # Now that logger is set up, we can log initialization info
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
            self.setup_logger(value)  # Reconfigure logger with new mode

    def load_config(self):
        """Load configuration based on mode"""
        self.logger.debug("Loading configuration...")
        self.logger.debug(f"Current TEST_DATA_CHANNEL: {os.getenv('TEST_DATA_CHANNEL')}")
        
        # Force reload the .env file
        load_dotenv(override=True)
        self.logger.debug(f"After reload TEST_DATA_CHANNEL: {os.getenv('TEST_DATA_CHANNEL')}")
        
        # Log all channel-related env vars
        self.logger.debug("All channel-related environment variables:")
        for key, value in os.environ.items():
            if 'CHANNEL' in key:
                self.logger.debug(f"{key}: {value}")
        
        try:
            if self.dev_mode:
                self.logger.info("Loading development configuration")  # Changed
                self.guild_id = int(os.getenv('DEV_GUILD_ID'))
                self.summary_channel_id = int(os.getenv('DEV_SUMMARY_CHANNEL_ID'))
                channels_str = os.getenv('DEV_CHANNELS_TO_MONITOR')
                if not channels_str:
                    raise ConfigurationError("DEV_CHANNELS_TO_MONITOR not found in environment")
                self.dev_channels_to_monitor = [chan.strip() for chan in channels_str.split(',')]
                self.logger.info(f"DEV_CHANNELS_TO_MONITOR: {self.dev_channels_to_monitor}")  # Changed
            else:
                self.logger.info("Loading production configuration")  # Changed
                self.guild_id = int(os.getenv('GUILD_ID'))
                self.summary_channel_id = int(os.getenv('PRODUCTION_SUMMARY_CHANNEL_ID'))
                channels_str = os.getenv('CHANNELS_TO_MONITOR')
                if not channels_str:
                    raise ConfigurationError("CHANNELS_TO_MONITOR not found in environment")
                self.channels_to_monitor = [int(chan.strip()) for chan in channels_str.split(',')]
                self.logger.info(f"CHANNELS_TO_MONITOR: {self.channels_to_monitor}")  # Changed
            
            self.logger.info(f"Configured with guild_id={self.guild_id}, "  # Changed
                        f"summary_channel={self.summary_channel_id}, "
                        f"channels={self.channels_to_monitor if not self.dev_mode else self.dev_channels_to_monitor}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")  # Changed
            raise ConfigurationError(f"Failed to load configuration: {e}")

    def load_test_data(self) -> Dict[str, Any]:
        """Load test data from test.json"""
        try:
            with open('test.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load test data: {e}")
            return {"messages": []}

    async def setup_hook(self):
        """This is called when the bot is starting up"""
        try:
            self.session = aiohttp.ClientSession()
            
            notification_channel = self.get_channel(self.summary_channel_id)
            admin_user = await self.fetch_user(int(os.getenv('ADMIN_USER_ID')))
            self.error_handler = ErrorHandler(notification_channel, admin_user)
            
        except Exception as e:
            raise ConfigurationError("Failed to initialize bot", e)

    async def get_channel_history(self, channel_id):
        """Get channel history with support for test data channel in dev mode"""
        if self.dev_mode:
            try:
                # Remove excessive environment logging
                test_channel_id = int(os.getenv('TEST_DATA_CHANNEL'))
                test_channel = self.get_channel(test_channel_id)
                
                if not test_channel:
                    self.logger.error(f"Could not find channel with ID {test_channel_id}")
                    return []
                
                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                # Add detailed environment logging
                self.logger.debug("Environment state in get_channel_history:")
                self.logger.debug("All environment variables containing 'CHANNEL':")
                for key, value in os.environ.items():
                    if 'CHANNEL' in key:
                        self.logger.debug(f"{key}: {value}")
                
                test_channel_id = int(os.getenv('TEST_DATA_CHANNEL'))
                self.logger.debug(f"TEST_DATA_CHANNEL before conversion: {os.getenv('TEST_DATA_CHANNEL')}")
                self.logger.debug(f"TEST_DATA_CHANNEL after conversion: {test_channel_id}")
                
                test_channel = self.get_channel(test_channel_id)
                if test_channel:
                    self.logger.debug(f"Found test channel: {test_channel.name} ({test_channel.id})")
                else:
                    self.logger.error(f"Could not find channel with ID {test_channel_id}")
                
                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                messages = []
                
                async for message in test_channel.history(after=yesterday, limit=None):
                    total_reactions = sum(reaction.count for reaction in message.reactions) if message.reactions else 0
                    
                    # Process attachments - store with the test channel ID
                    if message.attachments:
                        async with aiohttp.ClientSession() as session:
                            for attachment in message.attachments:
                                processed = await self.attachment_handler.process_attachment(
                                    attachment, 
                                    message, 
                                    session
                                )
                    
                    messages.append({
                        'content': message.content,
                        'author': message.author.name,
                        'timestamp': message.created_at,
                        'jump_url': message.jump_url,
                        'reactions': total_reactions,
                        'id': str(message.id),
                        'attachments': [
                            {
                                'url': a.url,
                                'filename': a.filename,
                                'content_type': a.content_type
                            } for a in message.attachments
                        ]
                    })
                
                self.logger.info(f"Loaded {len(messages)} messages from test data channel")
                return messages
                
            except Exception as e:
                self.logger.error(f"Error loading test data channel: {e}")
                self.logger.debug(traceback.format_exc())
                return []
        
        # Production and Development Code Path
        try:
            channel = self.get_channel(channel_id)
            if not channel:
                raise ChannelSummarizerError(f"Could not access channel {channel_id}")
            
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            messages = []
            
            async for message in channel.history(after=yesterday, limit=None):
                total_reactions = sum(reaction.count for reaction in message.reactions) if message.reactions else 0
                
                # Process attachments
                if message.attachments:
                    async with aiohttp.ClientSession() as session:
                        for attachment in message.attachments:
                            processed = await self.attachment_handler.process_attachment(
                                attachment, message, session
                            )
                
                messages.append({
                    'content': message.content,
                    'author': message.author.name,
                    'timestamp': message.created_at,
                    'jump_url': message.jump_url,
                    'reactions': total_reactions,
                    'id': str(message.id),
                    'attachments': [
                        {
                            'url': a.url,
                            'filename': a.filename,
                            'content_type': a.content_type
                        } for a in message.attachments
                    ]
                })
            
            return messages
            
        except Exception as e:
            self.logger.error(f"Error getting channel history: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    @handle_errors("get_claude_summary")
    async def get_claude_summary(self, messages):
        """
        Generate summary using Claude API with comprehensive error handling.
        """
        if not messages:
            self.logger.info("No messages to summarize")
            return "[NOTHING OF NOTE]"
        
        self.logger.info(f"Generating summary for {len(messages)} messages")
        
        try:
            # Build the conversation prompt
            conversation = """Please summarize the interesting and noteworthy Discord happenings and ideas in bullet points. ALWAYS include Discord links and external links. You should extract ideas and information that may be useful to others from conversations. Avoid stuff like bug reports that are circumstantial or not useful to others. Break them into topics and sub-topics.

If there's nothing significant or noteworthy in the messages, just respond with exactly "[NOTHING OF NOTE]" (and no other text). Always include external links and Discord links wherever possible.

Requirements:
1. Make sure to ALWAYS include Discord links and external links 
2. Use Discord's markdown format (not regular markdown)
3. Use - for top-level points (no bullet for the header itself). Only use - for clear sub-points that directly relate to the point above. You should generally just create a new point for each new topic.
4. Make each main topic a ## header with an emoji
5. Use ** for bold text (especially for usernames and main topics)
6. Keep it simple - just bullet points and sub-points for each topic, no headers or complex formatting
7. ALWAYS include the message author's name in bold (**username**) for each point if there's a specific person who did something, said something important, or seemed to be helpful - mention their username, don't tag them. Call them "Banodocians" instead of "users".
8. Always include a funny or relevant emoji in the topic title

Here's one example of what a good summary and topic should look like:

## 🤏 **H264/H265 Compression Techniques for Video Generation Improves Img2Vid**
- **zeevfarbman** recommended h265 compression for frame degradation with less perceptual impact: https://discord.com/channels/1076117621407223829/1309520535012638740/1316462339318354011
- **johndopamine** suggested using h264 node in MTB node pack for better video generation: https://discord.com/channels/564534534/1316786801247260672
- Codec compression can help "trick" current workflows/models: https://github.com/tdrussell/codex-pipe
- melmass confirmed adding h265 support to their tools: https://discord.com/channels/1076117621407223829/1309520535012638740/1316786801247260672

And here's another example of a good summary and topic:

## 🏋 **Person Training for Hunyuan Video is Now Possible**    
- **Kytra** explained that training can be done on relatively modest hardware (24GB VRAM): https://discord.com/channels/1076117621407223829/1316079815446757396/1316418253102383135
- **TDRussell** provided the repository link: https://github.com/tdrussell/diffusion-pipe
- Banodocians are generally experimenting with training LoRAs using images and videos

While here are bad topics to cover:
- Bug reports that seem unremarkable and not useful to others
- Messages that are not helpful or informative to others
- Discussions that ultimately have no real value to others
- Humourous messages that are not helpful or informative to others
- Personal messages, commentary or information that are not likely to be helpful or informative to others

Remember:

1. You MUST ALWAYS include relevant Discord links and external links
2. Only include topics that are likely to be interesting and noteworthy to others
3. You MUST ALWAYS include the message author's name in bold (**username**) for each point if there's a specific person who did something, said something important, or seemed to be helpful - mention their username, don't tag them. Call them "Banodocians" instead of "users".
4. You MUST ALWAYS use Discord's markdown format (not regular markdown)
5. Keep most information at the top bullet level. Only use sub-points for direct supporting details
6. Make topics clear headers with ##

IMPORTANT: For each bullet point, use the EXACT message URL provided in the data - do not write <message_url> but instead use the actual URL from the message data.

Please provide the summary now - don't include any other introductory text, ending text, or explanation of the summary:\n\n"""
        
            for msg in messages:
                conversation += f"{msg['timestamp']} - {msg['author']}: {msg['content']}"
                if msg['reactions']:
                    conversation += f"\nReactions: {msg['reactions']}"
                conversation += f"\nDiscord link: {msg['jump_url']}\n\n"

            loop = asyncio.get_running_loop()
            
            # Define a helper function to call the synchronous Claude API method
            def create_summary():
                return self.claude.messages.create(
                    model="claude-3-5-sonnet-latest",
                    max_tokens=8192,
                    messages=[
                        {
                            "role": "user",
                            "content": conversation
                        }
                    ],
                    timeout=120  # 120-second timeout
                )
            
            # Run the synchronous create_summary in a separate thread to avoid blocking
            response = await loop.run_in_executor(None, create_summary)
            
            self.logger.debug(f"Response type: {type(response)}")
            self.logger.debug(f"Response content: {response.content}")
            
            # Ensure the response has the expected structure
            if not hasattr(response, 'content') or not response.content:
                raise ValueError("Invalid response format from Claude API.")

            summary_text = response.content[0].text.strip()
            self.logger.info("Summary generated successfully")
            return summary_text
                
        except asyncio.CancelledError:
            self.logger.info("Summary generation cancelled - shutting down gracefully")
            raise
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt detected - shutting down gracefully")
            raise
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def generate_short_summary(self, full_summary: str, message_count: int) -> str:
        """
        Get a short summary using Claude with proper async handling.
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conversation = f"""Create exactly 3 bullet points summarizing key developments. STRICT format requirements:
1. The FIRST LINE MUST BE EXACTLY: 📨 __{message_count} messages sent__
2. Then three bullet points that:
   - Start with -
   - Give a short summary of one of the main topics from the full summary - priotise topics that are related to the channel and are likely to be useful to others.
   - Bold the most important finding/result/insight using **
   - Keep each to a single line
4. DO NOT MODIFY THE MESSAGE COUNT OR FORMAT IN ANY WAY

Required format:
"📨 __{message_count} messages sent__
• [Main topic 1] 
• [Main topic 2]
• [Main topic 3]"
DO NOT CHANGE THE MESSAGE COUNT LINE. IT MUST BE EXACTLY AS SHOWN ABOVE. DO NOT ADD INCLUDE ELSE IN THE MESSAGE OTHER THAN THE ABOVE.

Full summary to work from:
{full_summary}"""

                loop = asyncio.get_running_loop()
                
                # Define a helper function to call the synchronous Claude API method
                def create_short_summary():
                    return self.claude.messages.create(
                        model="claude-3-5-haiku-latest",
                        max_tokens=8192,
                        messages=[
                            {
                                "role": "user",
                                "content": conversation
                            }
                        ]
                    )
                
                # Run the synchronous create_summary in a separate thread to avoid blocking
                response = await loop.run_in_executor(None, create_short_summary)
                
                return response.content[0].text.strip()
                    
            except asyncio.TimeoutError:
                retry_count += 1
                self.logger.error(f"Timeout attempt {retry_count}/{max_retries} while generating short summary")
                if retry_count < max_retries:
                    self.logger.info(f"Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    self.logger.error("All retry attempts failed")
                    return f"__📨 {message_count} messages sent__\n• Error generating summary\u200B"
                    
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Error attempt {retry_count}/{max_retries} while generating short summary: {e}")
                if retry_count < max_retries:
                    self.logger.info(f"Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    self.logger.error("All retry attempts failed")
                    return f"__📨 {message_count} messages sent__\n• Error generating summary\u200B"

    async def send_initial_message(self, channel, short_summary: str, source_channel_name: str) -> Tuple[discord.Message, Optional[str]]:
        """
        Send the initial summary message to the channel with top reacted media if available.
        Returns tuple of (message, used_message_id if media was attached, else None)
        """
        try:
            source_channel = discord.utils.get(self.get_all_channels(), name=source_channel_name.strip('#'))
            
            message_content = (f"## <#{source_channel.id}>\n{short_summary}" 
                             if source_channel else f"## {source_channel_name}\n{short_summary}")
            
            # Find the most reacted media file
            top_media = None
            top_reactions = 2  # Minimum reaction threshold
            top_message_id = None
            
            for message_id, cache_data in self.attachment_handler.attachment_cache.items():
                if cache_data['reaction_count'] > top_reactions:
                    for attachment in cache_data['attachments']:
                        if any(attachment.filename.lower().endswith(ext) 
                              for ext in ('.png', '.jpg', '.jpeg', '.gif', '.mp4', '.mov', '.webm')):
                            top_media = attachment
                            top_reactions = cache_data['reaction_count']
                            top_message_id = message_id
            
            # If we found popular media, attach it to the message
            initial_message = None
            if top_media:
                file = discord.File(
                    io.BytesIO(top_media.data),
                    filename=top_media.filename,
                    description=f"Most popular media (🔥 {top_reactions} reactions)"
                )
                initial_message = await self.safe_send_message(channel, message_content, file=file)
            else:
                initial_message = await self.safe_send_message(channel, message_content)
            
            self.logger.info(f"Initial message sent successfully: {initial_message.id if initial_message else 'None'}")
            return initial_message, top_message_id
            
        except Exception as e:
            self.logger.error(f"Failed to send initial message: {e}")
            self.logger.debug(traceback.format_exc())
            raise DiscordError(f"Failed to send initial message: {e}")

    async def create_summary_thread(self, message, thread_name, is_top_generations=False):
        """Create a thread for a summary message."""
        try:
            self.logger.info(f"Creating thread for message {message.id}, attempt 1")
            
            # Only add "Detailed Summary" prefix if it's not a top generations thread
            thread_name_with_prefix = thread_name if is_top_generations else f" Detailed Summary - {thread_name}"
            
            thread = await message.create_thread(
                name=thread_name_with_prefix,
                auto_archive_duration=1440  # 24 hours
            )
            
            self.logger.info(f"Thread created successfully: {message.id} with name '{thread_name}'")
            return thread
            
        except Exception as e:
            self.logger.error(f"Error creating thread: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def prepare_topic_files(self, topic: str) -> List[Tuple[discord.File, int, str, str]]:
        """
        Prepare files for a given topic.
        
        Args:
            topic: The topic text to process
            
        Returns:
            List of tuples containing (discord.File, reaction_count, message_id, username)
        """
        topic_files = []
        message_links = re.findall(r'https://discord\.com/channels/\d+/\d+/(\d+)', topic)
        
        for message_id in message_links:
            if message_id in self.attachment_handler.attachment_cache:
                for attachment in self.attachment_handler.attachment_cache[message_id]['attachments']:
                    try:
                        if len(attachment.data) <= 25 * 1024 * 1024:  # 25MB limit
                            file = discord.File(
                                io.BytesIO(attachment.data),
                                filename=attachment.filename,
                                description=f"From message ID: {message_id} (🔥 {self.attachment_handler.attachment_cache[message_id]['reaction_count']} reactions)"
                            )
                            topic_files.append((
                                file,
                                self.attachment_handler.attachment_cache[message_id]['reaction_count'],
                                message_id,
                                self.attachment_handler.attachment_cache[message_id].get('username', 'Unknown')  # Add username
                            ))
                    except Exception as e:
                        self.logger.error(f"Failed to prepare file {attachment.filename}: {e}")
                        continue
                        
        return sorted(topic_files, key=lambda x: x[1], reverse=True)[:10]  # Limit to top 10 files

    async def send_topic_chunk(self, thread: discord.Thread, chunk: str, files: List[Tuple[discord.File, int, str, str]] = None):
        try:
            if files:
                # Get list of usernames in order of their attachments
                usernames = [f"**{file_tuple[3]}**" for file_tuple in files]  # Access username from tuple
                unique_usernames = []
                [unique_usernames.append(x) for x in usernames if x not in unique_usernames]
                
                # Format usernames with 'and' and ':'
                if len(unique_usernames) > 1:
                    formatted_usernames = f"{', '.join(unique_usernames[:-1])} and {unique_usernames[-1]}"
                else:
                    formatted_usernames = unique_usernames[0]
                
                # Only add the header if this chunk has files
                if "Related attachments" not in chunk:
                    chunk = f"{chunk}\nRelated attachments from {formatted_usernames} / "
                
                await self.safe_send_message(thread, chunk, files=[file_tuple[0] for file_tuple in files])
            else:
                await self.safe_send_message(thread, chunk)
        except Exception as e:
            self.logger.error(f"Failed to send topic chunk: {e}")
            self.logger.debug(traceback.format_exc())

    async def process_topic(self, thread: discord.Thread, topic: str, is_first: bool = False) -> Tuple[Set[str], List[discord.File]]:
        """Process and send a single topic to the thread. Returns used message IDs and files."""
        try:
            if not topic.strip():
                return set(), []

            # Format topic header
            formatted_topic = ("---\n" if not is_first else "") + f"## {topic}"
            self.logger.info(f"Processing topic: {formatted_topic[:100]}...")

            # Get message IDs from this chunk
            chunk_message_ids = set(re.findall(r'https://discord\.com/channels/\d+/(\d+)/(\d+)', topic))
            used_message_ids = set()
            used_files = []  # Track files used in this topic

            # Prepare files for just this chunk
            chunk_files = []
            for channel_id, message_id in chunk_message_ids:
                cache_key = f"{channel_id}:{message_id}"
                if cache_key in self.attachment_handler.attachment_cache:
                    cache_data = self.attachment_handler.attachment_cache[cache_key]
                    for attachment in cache_data['attachments']:
                        try:
                            if len(attachment.data) <= 25 * 1024 * 1024:  # 25MB limit
                                file = discord.File(
                                    io.BytesIO(attachment.data),
                                    filename=attachment.filename,
                                    description=f"From {cache_data['username']} (🔥 {cache_data['reaction_count']} reactions)"
                                )
                                chunk_files.append((
                                    file,
                                    cache_data['reaction_count'],
                                    message_id,
                                    cache_data['username']
                                ))
                                used_files.append(file)  # Track the file
                        except Exception as e:
                            self.logger.error(f"Failed to prepare file: {e}")
                            continue

            # Sort files by reaction count and limit to top 10
            chunk_files.sort(key=lambda x: x[1], reverse=True)
            chunk_files = chunk_files[:10]

            # Send the chunk with its specific attachments
            if chunk_files:
                # Send content first
                await self.safe_send_message(thread, formatted_topic)
                
                # Then send each attachment individually
                for file, reaction_count, message_id, username in chunk_files:
                    try:
                        await self.safe_send_message(
                            thread,
                            file=file
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to send attachment: {e}")
            else:
                # If no attachments, just send the content
                await self.safe_send_message(thread, formatted_topic)

            return chunk_message_ids, used_files

        except Exception as e:
            self.logger.error(f"Error processing topic: {e}")
            self.logger.debug(traceback.format_exc())
            return set(), []
    
    async def process_unused_attachments(self, thread: discord.Thread, used_message_ids: Set[str], max_attachments: int = 10, previous_thread_id: Optional[int] = None, used_files: List[discord.File] = None):
        """Process and send unused but popular attachments."""
        unused_attachments = []
        
        # Create set of filenames that were already used
        used_filenames = {f.filename for f in (used_files or [])}
        
        # Get all attachments already in the thread
        try:
            async for message in thread.history(limit=None):
                for attachment in message.attachments:
                    used_filenames.add(attachment.filename)
                    self.logger.debug(f"Adding existing thread attachment to used files: {attachment.filename}")
        except Exception as e:
            self.logger.error(f"Error getting thread history for attachment tracking: {e}")
        
        for cache_key, cache_data in self.attachment_handler.attachment_cache.items():
            channel_part, message_part = cache_key.split(":", 1)

            # Skip if message was already used or if attachment was used as main image
            if message_part not in used_message_ids and cache_data['reaction_count'] >= 3:
                message_link = f"https://discord.com/channels/{self.guild_id}/{channel_part}/{message_part}"

                for attachment in cache_data['attachments']:
                    try:
                        # Skip if this file was already used anywhere in the thread
                        if attachment.filename in used_filenames:
                            self.logger.debug(f"Skipping already used file: {attachment.filename}")
                            continue
                        
                        if len(attachment.data) <= 25 * 1024 * 1024:  # 25MB limit
                            file = discord.File(
                                io.BytesIO(attachment.data),
                                filename=attachment.filename,
                                description=f"From {attachment.username} (🔥 {attachment.reaction_count} reactions)"
                            )
                            unused_attachments.append((
                                file, 
                                attachment.reaction_count,
                                message_part,
                                attachment.username,
                                message_link
                            ))
                    except Exception as e:
                        self.logger.error(f"Failed to prepare unused attachment: {e}")
                        continue

        if unused_attachments:
            # Sort by reaction count
            unused_attachments.sort(key=lambda x: x[1], reverse=True)
            
            # Post header
            await self.safe_send_message(thread, "\n\n---\n# 📎 Other Popular Attachments")
            
            # Post each attachment individually with a message link (limited by max_attachments)
            for file, reaction_count, message_id, username, message_link in unused_attachments[:max_attachments]:
                message_content = f"By **{username}**: {message_link}"
                await self.safe_send_message(thread, message_content, file=file)

        # Add links to previous thread and current thread beginning
        try:
            footer_text = "\n---\n\u200B\n"

            if previous_thread_id and str(previous_thread_id) != str(thread.id):  # Convert to strings for comparison
                try:
                    # Add more detailed logging
                    self.logger.info(f"Attempting to fetch previous thread {previous_thread_id}")
                    previous_thread = None
                    
                    # Try both fetch_channel and get_channel
                    try:
                        previous_thread = self.get_channel(previous_thread_id)
                        if not previous_thread:
                            previous_thread = await self.fetch_channel(previous_thread_id)
                    except discord.NotFound:
                        self.logger.warning(f"Could not find thread with ID {previous_thread_id}")
                    
                    if previous_thread and isinstance(previous_thread, discord.Thread):
                        try:
                            # Get first message of the thread for proper linking
                            async for first_message in previous_thread.history(oldest_first=True, limit=1):
                                thread_date = previous_thread.created_at.strftime('%B %Y')
                                footer_text += (
                                    f"**You can find a summary of this channel's activity from "
                                    f"{thread_date} here:** {first_message.jump_url}\n\n"
                                )
                                self.logger.info(f"Successfully added link to previous thread from {thread_date}")
                                break
                        except discord.Forbidden:
                            self.logger.error(f"Bot lacks permissions to access thread {previous_thread_id}")
                        except discord.NotFound:
                            self.logger.error(f"Thread {previous_thread_id} exists but cannot be accessed")
                        except Exception as e:
                            self.logger.error(f"Error creating previous thread link: {e}")
                    else:
                        self.logger.error(
                            f"Retrieved channel is not a thread: {type(previous_thread)}"
                            f" for ID {previous_thread_id}"
                        )
                        # Clear invalid thread ID from database
                        self.db.update_summary_thread(thread.parent.id, None)
                        self.logger.info(f"Cleared invalid thread ID {previous_thread_id} from database")
                except Exception as e:
                    self.logger.error(f"Error processing previous thread: {e}")
                    self.logger.debug(traceback.format_exc())

            # Add current thread jump link
            try:
                async for first_message in thread.history(oldest_first=True, limit=1):
                    footer_text += f"***Click here to jump to the beginning of this thread: {first_message.jump_url}***"
                    await self.safe_send_message(thread, footer_text)
                    self.logger.info("Successfully added thread navigation links")
                    break
            except Exception as e:
                self.logger.error(f"Failed to add thread jump link: {e}")
                self.logger.debug(traceback.format_exc())

        except Exception as e:
            self.logger.error(f"Failed to add thread links: {e}")
            self.logger.debug(traceback.format_exc())

    async def post_summary(self, channel_id, summary: str, source_channel_name: str, message_count: int):
        try:
            source_channel = self.get_channel(channel_id)
            if not source_channel:
                raise DiscordError(f"Could not access source channel {channel_id}")

            self.logger.info(f"Starting post_summary for channel {source_channel.name} (ID: {channel_id})")
            
            # Add debug logging for content lengths
            self.logger.debug(f"Summary length: {len(summary)} characters")
            self.logger.debug(f"First 500 chars of summary: {summary[:500]}")
            
            # Get existing thread ID from DB
            existing_thread_id = self.db.get_summary_thread_id(channel_id)
            self.logger.debug(f"Existing thread ID: {existing_thread_id}")
            
            source_thread = None
            thread_existed = False  # Track if thread existed before
            if existing_thread_id:
                try:
                    self.logger.info(f"Attempting to fetch existing thread with ID: {existing_thread_id}")
                    source_thread = await self.fetch_channel(existing_thread_id)
                    
                    # Add check for orphaned thread
                    if isinstance(source_thread, discord.Thread):
                        try:
                            # Try to fetch the parent message
                            parent_message = await source_channel.fetch_message(source_thread.id)
                            self.logger.info(f"Successfully fetched existing thread: {source_thread.name}")
                            thread_existed = True
                        except discord.NotFound:
                            self.logger.warning(f"Parent message for thread {existing_thread_id} was deleted. Creating new thread.")
                            source_thread = None
                            self.db.update_summary_thread(channel_id, None)
                    else:
                        self.logger.error(f"Fetched channel is not a thread: {existing_thread_id}")
                        source_thread = None
                        self.db.update_summary_thread(channel_id, None)
                except discord.NotFound:
                    self.logger.error(f"Thread {existing_thread_id} not found")
                    source_thread = None
                    self.db.update_summary_thread(channel_id, None)
                except Exception as e:
                    self.logger.error(f"Failed to fetch existing thread: {e}")
                    source_thread = None
                    self.db.update_summary_thread(channel_id, None)

            if not source_thread:
                # Only unpin messages when creating a new thread
                pins = await source_channel.pins()
                for pin in pins:
                    if pin.author.id == self.user.id:  # Check if the pin was made by the bot
                        await pin.unpin()
                    
                self.logger.info("Creating new thread for channel")
                current_date = datetime.utcnow()
                short_month = current_date.strftime('%b')
                thread_message = await source_channel.send(
                    f"📝 A new summary thread has been created for <#{source_channel.id}> for {datetime.utcnow().strftime('%B %Y')}.\n\n"
                    "All of the messages in this channel will be summarised here for your convenience:"
                )
                await thread_message.pin()  # Pin the new thread message
                
                thread_name = f"{short_month} - #{source_channel_name} Summary"
                source_thread = await thread_message.create_thread(name=thread_name)
                
                self.logger.info(f"Created new thread with ID: {source_thread.id}")
                self.db.update_summary_thread(channel_id, source_thread.id)
                self.logger.info(f"Updated DB with new thread ID: {source_thread.id}")

            # Generate short summary and handle main summary channel post
            short_summary = await self.generate_short_summary(summary, message_count)
            
            # Get top attachment for initial message
            all_files = self.attachment_handler.get_all_files_sorted()
            top_attachment = all_files[0] if all_files else None
            
            initial_file = None
            if top_attachment:
                initial_file = discord.File(
                    io.BytesIO(top_attachment.data),
                    filename=top_attachment.filename,
                    description=f"🔥 {top_attachment.reaction_count} reactions"
                )
            
            channel_mention = f"<#{source_channel.id}>"

            # Remove test mode check and use production channel directly
            summary_channel = self.get_channel(self.summary_channel_id)
            initial_message = await self.safe_send_message(
                summary_channel,
                f"## <#{source_channel.id}>\n{short_summary}",
                file=initial_file
            )

            # Track all files used in any part of the summary
            used_files = []
            if initial_file:
                used_files.append(initial_file)

            # Create and process main summary thread
            thread = await self.create_summary_thread(initial_message, source_channel_name)
            topics = summary.split("## ")
            topics = [t.strip().rstrip('#').strip() for t in topics if t.strip()]
            used_message_ids = set()

            # Add "Detailed Summary" headline at the beginning (only in main summary thread)
            await self.safe_send_message(thread, "# Detailed Summary")

            for i, topic in enumerate(topics):
                topic_used_ids, topic_files = await self.process_topic(thread, topic, is_first=(i == 0))
                used_message_ids.update(topic_used_ids)
                used_files.extend(topic_files)  # Track files used in topics
                
            await self.process_unused_attachments(
                thread, 
                used_message_ids, 
                previous_thread_id=existing_thread_id,
                used_files=used_files  # Pass all used files
            )

            # Post to source channel thread (with limited attachments)
            current_date = datetime.utcnow().strftime('%A, %B %d, %Y')

            # Create a new file object for the source thread
            if initial_file:
                initial_file.fp.seek(0)
                source_file = discord.File(
                    initial_file.fp,
                    filename=initial_file.filename,
                    description=initial_file.description
                )
            else:
                source_file = None

            # Keep original header for source thread (without "Detailed Summary")
            await self.safe_send_message(source_thread, f"# Summary from {current_date}")

            # Process topics for source thread
            for i, topic in enumerate(topics):
                topic_used_ids, topic_files = await self.process_topic(source_thread, topic, is_first=(i == 0))
                used_message_ids.update(topic_used_ids)
                used_files.extend(topic_files)  # Track files used in topics

            # Only difference: limit attachments to 3 in the final section
            await self.process_unused_attachments(source_thread, used_message_ids, max_attachments=3, previous_thread_id=existing_thread_id)

            # After posting the summary to the thread, notify the channel if it was an existing thread
            if thread_existed:
                # Build proper Discord deep link using channel and thread IDs
                thread_link = f"https://discord.com/channels/{self.guild_id}/{source_channel.id}/{source_thread.id}"
                notification_message = f"📝 A new daily summary has been added for <#{source_channel.id}>.\n\nYou can see all of {datetime.utcnow().strftime('%B')}'s activity here: {thread_link}"
                await self.safe_send_message(source_channel, notification_message)
                self.logger.info(f"Posted thread update notification to {source_channel.name}")

        except Exception as e:
            self.logger.error(f"Error in post_summary: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def generate_summary(self):
        self.logger.info("generate_summary started")
        self.logger.info("\nStarting summary generation")
        
        try:
            # Remove test mode check and use production channel directly
            summary_channel = self.get_channel(self.summary_channel_id)
            if not summary_channel:
                raise DiscordError(f"Could not access summary channel {self.summary_channel_id}")
            
            self.logger.info(f"Found summary channel: {summary_channel.name} "
                       f"({'DEV' if self.dev_mode else 'PRODUCTION'} mode)")
            
            active_channels = False
            date_header_posted = False
            self.first_message = None

            # Process channels based on whether they are categories or specific channels
            channels_to_process = []
            if self.dev_mode:
                if "everyone" in self.dev_channels_to_monitor:
                    guild = self.get_guild(self.guild_id)
                    if not guild:
                        raise DiscordError(f"Could not access guild {self.guild_id}")
                    
                    # Only exclude support channels
                    channels_to_process = [channel.id for channel in guild.text_channels 
                                         if 'support' not in channel.name.lower()]
                else:
                    # Monitor specified channels or categories
                    for item in self.dev_channels_to_monitor:
                        try:
                            item_id = int(item)
                            channel = self.get_channel(item_id)
                            if isinstance(channel, discord.CategoryChannel):
                                # If it's a category, add all text channels within it, excluding support channels
                                self.logger.info(f"Processing category: {channel.name}")
                                channels_to_process.extend([
                                    c.id for c in channel.channels 
                                    if isinstance(c, discord.TextChannel) and 'support' not in c.name.lower()
                                ])
                            else:
                                # If it's a regular channel, add it if it doesn't have "support" in its name
                                if 'support' not in channel.name.lower():
                                    channels_to_process.append(item_id)
                        except ValueError:
                            self.logger.warning(f"Invalid channel/category ID: {item}")
            else:
                # Production mode - same logic for handling categories and channels
                for item in self.channels_to_monitor:
                    channel = self.get_channel(item)
                    if isinstance(channel, discord.CategoryChannel):
                        # If it's a category, add all text channels within it, excluding support channels
                        self.logger.info(f"Processing category: {channel.name}")
                        channels_to_process.extend([
                            c.id for c in channel.channels 
                            if isinstance(c, discord.TextChannel) and 'support' not in c.name.lower()
                        ])
                    else:
                        # If it's a regular channel, add it if it doesn't have "support" in its name
                        if 'support' not in channel.name.lower():
                            channels_to_process.append(item)
            
            self.logger.info(f"Final list of channels to process: {channels_to_process}")
            
            # Process channels sequentially
            for channel_id in channels_to_process:
                try:
                    # Clear attachment cache before processing each channel
                    self.attachment_handler.clear_cache()
                    
                    channel = self.get_channel(channel_id)
                    if not channel:
                        self.logger.error(f"Could not access channel {channel_id}")
                        continue
                    
                    self.logger.info(f"Processing channel: {channel.name}")
                    messages = await self.get_channel_history(channel.id)
                    
                    self.logger.info(f"Channel {channel.name} has {len(messages)} messages")
                    
                    # Process messages for caching attachments regardless of channel type
                    if len(messages) >= 10:
                        # For gens channels, just process attachments without creating summary
                        if 'gens' in channel.name.lower():
                            self.logger.info(f"Processing attachments for gens channel: {channel.name}")
                            continue  # Skip to next channel after attachments are cached
                        
                        # For non-gens channels, generate summary
                        summary = await self.get_claude_summary(messages)
                        self.logger.info(f"Generated summary for {channel.name}: {summary[:100]}...")
                                
                        if "[NOTHING OF NOTE]" not in summary:
                            self.logger.info(f"Noteworthy activity found in {channel.name}")
                            active_channels = True
                            
                            if not date_header_posted:
                                current_date = datetime.utcnow()
                                header = f"# 📅 Daily Summary for {current_date.strftime('%A, %B %d, %Y')}"
                                self.first_message = await summary_channel.send(header)
                                date_header_posted = True
                                self.logger.info("Posted date header")
                            
                            short_summary = await self.generate_short_summary(summary, len(messages))
                            
                            # Store in database regardless of mode
                            self.db.store_daily_summary(
                                channel_id=channel.id,
                                channel_name=channel.name,
                                messages=messages,
                                full_summary=summary,
                                short_summary=short_summary
                            )
                            self.logger.info(f"Stored summary in database for {channel.name}")
                            
                            await self.post_summary(
                                channel.id,
                                summary,
                                channel.name,
                                len(messages)
                            )
                            await asyncio.sleep(2)
                        else:
                            self.logger.info(f"No noteworthy activity in {channel.name}")
                    else:
                        self.logger.warning(f"Skipping {channel.name} - only {len(messages)} messages")
                    
                except Exception as e:
                    self.logger.error(f"Error processing channel {channel.name}: {e}")
                    self.logger.debug(traceback.format_exc())
                    continue
            
            if not active_channels:
                self.logger.info("No channels had significant activity - sending notification")
                await summary_channel.send("No channels had significant activity in the past 24 hours.")
            else:
                # Create top generations thread after all channels are processed
                self.logger.info("Active channels found, attempting to create top generations thread")
                try:
                    await self.create_top_generations_thread(summary_channel)
                    self.logger.info("Top generations thread creation completed")
                except Exception as e:
                    self.logger.error(f"Failed to create top generations thread: {e}")
                    self.logger.debug(traceback.format_exc())
                    # Don't raise here to allow the rest of the summary to complete

            # Add footer with jump link at the very end
            if self.first_message:
                footer_text = f"""---

**_Click here to jump to the top of today's summary:_** {self.first_message.jump_url}"""

                await self.safe_send_message(summary_channel, footer_text)
                self.logger.info("Footer message added to summary channel")
            else:
                self.logger.error("first_message is not defined. Cannot add footer.")
            
        except Exception as e:
            self.logger.error(f"Critical error in summary generation: {e}")
            self.logger.debug(traceback.format_exc())
            raise
        self.logger.info("generate_summary completed")

    async def on_ready(self):
        self.logger.info(f"Logged in as {self.user}")

    async def safe_send_message(self, channel, content=None, embed=None, file=None, files=None, reference=None):
        """Safely send a message with retry logic and error handling."""
        try:
            return await self.rate_limiter.execute(
                f"channel_{channel.id}",
                channel.send(
                    content=content,
                    embed=embed,
                    file=file,
                    files=files,
                    reference=reference
                )
            )
        except discord.HTTPException as e:
            self.logger.error(f"HTTP error sending message: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            raise

    async def create_media_content(self, files: List[Tuple[discord.File, int, str, str]], max_media: int = 4) -> Optional[discord.File]:
        """Create either a collage of images or a combined video based on media type."""
        try:
            from PIL import Image
            import moviepy.editor as mp
            import io
            
            self.logger.info(f"Starting media content creation with {len(files)} files")
            
            images = []
            videos = []
            has_audio = False
            
            # First pass - categorize media and check for audio
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
                
            # If we have videos with audio, combine them sequentially
            if videos and has_audio:
                self.logger.info("Creating combined video with audio")
                final_video = mp.concatenate_videoclips(videos)
                output_path = 'combined_video.mp4'
                final_video.write_videofile(output_path)
                
                # Clean up
                for video in videos:
                    video.close()
                final_video.close()
                
                self.logger.info("Video combination complete")
                
                # Convert to discord.File
                with open(output_path, 'rb') as f:
                    return discord.File(f, filename='combined_video.mp4')
                
            # If we have videos without audio or images, create a collage
            elif images or (videos and not has_audio):
                self.logger.info("Creating image/GIF collage")
                
                # Convert silent videos to GIFs for the collage
                for i, video in enumerate(videos):
                    self.logger.debug(f"Converting video {i+1}/{len(videos)} to GIF")
                    gif_path = f'temp_gif_{len(images)}.gif'
                    video.write_gif(gif_path)
                    gif_img = Image.open(gif_path)
                    images.append(gif_img)
                    video.close()
                
                if not images:
                    self.logger.warning("No images available for collage")
                    return None
                
                # Calculate collage dimensions
                n = len(images)
                if n == 1:
                    cols, rows = 1, 1
                elif n == 2:
                    cols, rows = 2, 1
                else:
                    cols, rows = 2, 2
                
                self.logger.debug(f"Creating {cols}x{rows} collage for {n} images")
                
                # Create collage
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
                
                # Convert to discord.File
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
            # Clean up temporary files
            import os
            self.logger.debug("Cleaning up temporary files")
            for f in os.listdir():
                if f.startswith('temp_'):
                    try:
                        os.remove(f)
                        self.logger.debug(f"Removed temporary file: {f}")
                    except Exception as e:
                        self.logger.warning(f"Failed to remove temporary file {f}: {e}")

    async def create_top_generations_thread(self, summary_channel):
        """Creates a thread showcasing the top video generations with >3 reactors."""
        try:
            self.logger.info("Starting create_top_generations_thread")
            
            # In dev mode, use the test data channel
            if self.dev_mode:
                test_channel_id = str(os.getenv('TEST_DATA_CHANNEL'))
                self.logger.info(f"Using test data channel {test_channel_id} for development")  # Changed from logger to self.logger
                monitored_channels = [test_channel_id]
            else:
                # Production mode - use ALL channels
                guild = self.get_guild(self.guild_id)
                if not guild:
                    raise DiscordError(f"Could not access guild {self.guild_id}")
                
                # Include all channels except support
                monitored_channels = [
                    str(channel.id) for channel in guild.text_channels 
                    if 'support' not in channel.name.lower()
                ]
            
            self.logger.info(f"Monitoring all channels for generations: {monitored_channels}")
            
            # Rest of the method remains the same, but remove the 'gens' channel check
            all_attachments = []
            for cache_key, cache_data in self.attachment_handler.attachment_cache.items():
                channel_id, message_id = cache_key.split(":")
                self.logger.debug(f"Processing cache item for channel {channel_id}")
                
                # Skip if channel is not in monitored list
                if channel_id not in monitored_channels:
                    self.logger.debug(f"Skipping channel {channel_id} - not in monitored list")
                    continue
                    
                # Get the channel to check its name
                channel = self.get_channel(int(channel_id))
                if not channel:
                    self.logger.warning(f"Could not find channel {channel_id}")
                    continue
                    
                # Only skip support channels, allow gens channels
                if 'support' in channel.name.lower():
                    self.logger.debug(f"Skipping support channel: {channel.name}")
                    continue
                    
                for attachment in cache_data['attachments']:
                    # Only include video files
                    if any(attachment.filename.lower().endswith(ext) 
                          for ext in ('.mp4', '.mov', '.webm', '.avi')):
                        self.logger.info(f"Found video file: {attachment.filename}")
                        
                        # Get the message to count unique reactors
                        try:
                            message = await channel.fetch_message(int(message_id))
                            unique_reactors = set()
                            for reaction in message.reactions:
                                async for user in reaction.users():
                                    unique_reactors.add(user.id)
                            
                            self.logger.info(f"Message {message_id} has {len(unique_reactors)} unique reactors")
                            
                            all_attachments.append({
                                'attachment': attachment,
                                'reaction_count': len(unique_reactors),
                                'username': cache_data['username'],
                                'message_id': message_id,
                                'channel_id': channel_id,
                                'channel_name': channel.name,
                                'message': message,
                                'unique_reactors': len(unique_reactors)
                            })
                            self.logger.info(f"Added attachment to processing list")
                        except discord.NotFound:
                            self.logger.warning(f"Message {message_id} not found in channel {channel.name}")
                            continue
                        except Exception as e:
                            self.logger.error(f"Error fetching message reactions: {e}")
                            self.logger.debug(traceback.format_exc())
                            continue

            self.logger.info(f"Found {len(all_attachments)} total video attachments")

            # Filter for attachments with >3 reactors before sorting
            filtered_attachments = [
                att for att in all_attachments 
                if att['unique_reactors'] > 3
            ]

            self.logger.info(f"Found {len(filtered_attachments)} video attachments with >3 reactions")

            if not filtered_attachments:
                # Don't create a thread if there are no qualifying generations
                self.logger.info("No video generations with >3 reactions - skipping thread creation")
                return

            # Sort by unique reactor count and get top 10
            top_generations = sorted(
                filtered_attachments, 
                key=lambda x: x['unique_reactors'], 
                reverse=True
            )[:10]

            self.logger.info(f"Selected top {len(top_generations)} generations")

            # Get the top generation for the header message
            top_gen = top_generations[0]
            top_message = top_gen['message'].content[:100] + "..." if len(top_gen['message'].content) > 100 else top_gen['message'].content
            
            # Create dynamic header based on number of generations
            header_title = (
                "Today's Top Generation" if len(top_generations) == 1 
                else f"Today's Top {len(top_generations)} Generations"
            )
            
            # Create the initial message and thread with top generation details
            header_content = [
                f"# {header_title}\n\n"                
                f"## 1. By **{top_gen['username']}** in {top_gen['channel_name']}\n"
                f"🔥 {top_gen['unique_reactors']} unique reactions\n"
            ]
            
            # Only add message text if it's not empty
            if top_message.strip():
                header_content.append(f"💭 Message text: `{top_message}`\n")
            
            # Add plain URL link
            header_content.append(f"🔗 {top_gen['message'].jump_url}")
            
            # Join all content parts
            header_content = ''.join(header_content)
            
            # Create header message with the top generation's video
            file = discord.File(
                io.BytesIO(top_gen['attachment'].data),
                filename=top_gen['attachment'].filename,
                description=f"Top Generation - {top_gen['unique_reactors']} unique reactions"
            )
            
            self.logger.info("Creating initial thread message with top generation")
            header_message = await self.safe_send_message(
                summary_channel,
                header_content,
                file=file
            )

            self.logger.info("Creating thread")
            thread = await self.create_summary_thread(
                header_message,
                f"Top Generations for {datetime.utcnow().strftime('%B %d, %Y')}",
                is_top_generations=True
            )

            # Remove the introduction text and start directly with the generations
            # Post each generation (starting from index 1 to skip the top one we already posted)
            for i, gen in enumerate(top_generations[1:], 2):
                try:
                    attachment = gen['attachment']
                    message = gen['message']
                    channel_name = gen['channel_name']
                    
                    # Create message link
                    message_link = message.jump_url
                    
                    # Get message content (first 100 chars)
                    msg_text = message.content[:100] + "..." if len(message.content) > 100 else message.content
                    
                    # Build description content
                    description = [
                        f"## {i}. By **{gen['username']}** in #{gen['channel_name']}\n"
                        f"🔥 {gen['unique_reactors']} unique reactions\n"
                    ]
                    
                    # Only add message text if it's not empty
                    if msg_text.strip():
                        description.append(f"💭 Message text: `{msg_text}`\n")
                    
                    # Add plain URL link
                    description.append(f"🔗 {message_link}")
                    
                    # Join all description parts
                    description = ''.join(description)

                    file = discord.File(
                        io.BytesIO(attachment.data),
                        filename=attachment.filename,
                        description=f"#{i} - {gen['unique_reactors']} unique reactions"
                    )

                    await self.safe_send_message(thread, description, file=file)
                    await asyncio.sleep(1)  # Prevent rate limiting

                except Exception as e:
                    self.logger.error(f"Error posting generation #{i}: {e}")
                    continue

            # Add footer
            footer = (
                "\n---\n"
                "**These generations represent the most popular non-#art_sharing videos "
                "from the past 24 hours, ranked by unique reactions from the community.**\n\n"
                "_Only generations with more than 3 unique reactions are included in this list._"
            )
            await self.safe_send_message(thread, footer)

            self.logger.info("Top generations thread created successfully")

        except Exception as e:
            self.logger.error(f"Error creating top generations thread: {e}")
            self.logger.debug(traceback.format_exc())
            raise

async def schedule_daily_summary(bot):
    """
    Schedule daily summaries with error handling and recovery.
    """
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    while True:
        try:
            now = datetime.utcnow()
            target = now.replace(hour=10, minute=0, second=0, microsecond=0)
            
            if now.hour >= 10:
                target += timedelta(days=1)
            
            delay = (target - now).total_seconds()
            bot.logger.info(f"Waiting {delay/3600:.2f} hours until next summary at {target} UTC")
            
            await asyncio.sleep(delay)
            
            await bot.generate_summary()
            bot.logger.info(f"Summary generated successfully at {datetime.utcnow()} UTC")
            # Reset failure counter on success
            consecutive_failures = 0
            
        except Exception as e:
            consecutive_failures += 1
            bot.logger.error(f"Error in scheduler (failure {consecutive_failures}): {e}")
            bot.logger.debug(traceback.format_exc())
            
            if consecutive_failures >= max_consecutive_failures:
                bot.logger.critical(f"Too many consecutive failures ({consecutive_failures}). "
                              f"Stopping scheduler.")
                # Optionally notify administrators
                raise
            
            # Wait for a short period before retrying
            await asyncio.sleep(300)  # 5 minutes

def main():
    # Parse command line arguments FIRST
    parser = argparse.ArgumentParser(description='Discord Channel Summarizer Bot')
    parser.add_argument('--run-now', action='store_true', help='Run the summary process immediately')
    parser.add_argument('--dev', action='store_true', help='Run in development mode')
    args = parser.parse_args()
    
    # Create and configure the bot
    bot = ChannelSummarizer()
    
    # Set dev mode before any other initialization
    bot.dev_mode = args.dev
    
    # Now load config and continue initialization
    bot.load_config()
    
    # Get bot token after loading config
    bot_token = os.getenv('DISCORD_BOT_TOKEN')
    if not bot_token:
        raise ValueError("Discord bot token not found in environment variables")
    
    if args.dev:
        bot.logger.info("Running in DEVELOPMENT mode - using test data")  # Changed from main_logger to bot.logger
    
    # Create event loop
    loop = asyncio.get_event_loop()
    
    # Modify the on_ready event to handle immediate execution if requested
    @bot.event
    async def on_ready():
        bot.logger.info(f"Logged in as {bot.user.name} ({bot.user.id})")
        bot.logger.info("Connected to servers: %s", [guild.name for guild in bot.guilds])
        
        if args.run_now:
            bot.logger.info("Running summary process immediately...")
            try:
                await bot.generate_summary()
                bot.logger.info("Summary process completed. Shutting down...")
            finally:
                # Only close sessions and shutdown in immediate mode
                if bot.session and not bot.session.closed:
                    await bot.session.close()
                await bot.close()
        else:
            # Start the scheduler for regular operation
            loop.create_task(schedule_daily_summary(bot))
    
    # Run the bot with proper cleanup
    try:
        loop.run_until_complete(bot.start(bot_token))
    except KeyboardInterrupt:
        bot.logger.info("Keyboard interrupt received - shutting down...")
        # Only do full cleanup on keyboard interrupt
        try:
            # Cancel all running tasks
            tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
            if tasks:
                bot.logger.info(f"Cancelling {len(tasks)} pending tasks...")
                for task in tasks:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            
            # Close the bot connection
            if not loop.is_closed():
                loop.run_until_complete(bot.close())
        finally:
            bot.logger.info("Closing event loop...")
            loop.close()
    else:
        # In normal operation, just keep running
        loop.run_forever()

if __name__ == "__main__":
    main()