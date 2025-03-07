import discord
from discord.ext import commands
import anthropic
import os
from typing import List, Dict, Tuple
import asyncio
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
import json
import logging
import sys
import aiohttp
from common.db_handler import DatabaseHandler
from src.common.base_bot import BaseDiscordBot

class SearchAnswerBot(BaseDiscordBot):
    def __init__(self):
        # Initialize logger first
        self.logger = logging.getLogger('search_bot')
        self.logger.setLevel(logging.INFO)
        
        # Set up intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        
        # Initialize the bot with required parameters
        super().__init__(
            command_prefix="!",
            intents=intents,
            heartbeat_timeout=120.0,
            guild_ready_timeout=30.0,
            gateway_queue_size=512,
            logger=self.logger
        )
        
        # Set up logging before any other operations
        self.setup_logging()
        
        # Ensure environment variables are loaded
        load_dotenv()
        
        # Add error handling for required environment variables
        required_env_vars = ['ANTHROPIC_API_KEY', 'GUILD_ID', 'DISCORD_BOT_TOKEN', 'ADMIN_USER_ID']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        self.claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        self.answer_channel_id = 1322583491019407361
        self.guild_id = int(os.getenv('GUILD_ID'))
        self.channel_map = {}
        
        # Add rate limiting
        self.search_cooldown = commands.CooldownMapping.from_cooldown(
            2, # Number of searches
            60.0, # Per 60 seconds
            commands.BucketType.user
        )

    def setup_logging(self):
        """Setup logging configuration with file rotation."""
        # Define log file path with absolute path
        log_dir = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(log_dir, 'search_dev_logs.log')
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            # Test file creation/writing permissions
            with open(log_file, 'w') as f:
                f.write("Initializing log file\n")
            
            print(f"Created log file at: {log_file}")
            
            # Configure logging
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file, mode='a'),  # Changed to append mode
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Test logging
            logging.info("Logging system initialized")
            
        except Exception as e:
            print(f"Error setting up logging: {e}")
            print(f"Attempted to create log at: {log_file}")
            # Fallback to just console logging
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[logging.StreamHandler(sys.stdout)]
            )

    async def setup_hook(self):
        logging.info(f"Bot is setting up...")
        
    async def on_ready(self):
        logging.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logging.info(f'Connected to {len(self.guilds)} guilds')
        logging.info(f'Watching for questions in channel ID: {self.answer_channel_id}')
        logging.info('------')

    async def get_searchable_channels(self) -> Dict[int, str]:
        """Get all text channels in the guild that are not support channels."""
        guild = self.get_guild(self.guild_id)
        if not guild:
            logging.warning(f"Warning: Could not find guild with ID {self.guild_id}")
            return {}
            
        channels = {}
        for channel in guild.text_channels:
            # Skip channels with 'support' in the name
            if 'support' not in channel.name.lower():
                channels[channel.id] = channel.name
                
        # Add debug logging
        logging.info(f"Found {len(channels)} searchable channels:")
        for channel_id, channel_name in channels.items():
            logging.debug(f"- #{channel_name} (ID: {channel_id})")
            
        return channels

    async def determine_relevant_channels(self, question: str, channels: Dict[int, str], search_info: Dict) -> List[int]:
        """Ask Claude which channels are most relevant for the search."""
        prompt = f"""Given this question and list of Discord channels, return ONLY the channel IDs that are most relevant for finding the answer.
Format as a JSON list of integers. Example: [123456789, 987654321]

Question: {question}

Available channels:
{json.dumps(channels, indent=2)}

Return ONLY the list of relevant channel IDs, nothing else."""

        input_tokens = len(prompt.split())
        
        try:
            # Run the blocking Claude API call in a thread pool
            loop = asyncio.get_running_loop()
            
            def call_claude():
                return self.claude.messages.create(
                    model="claude-3-5-haiku-latest",
                    max_tokens=500,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }]
                )
            
            response = await loop.run_in_executor(None, call_claude)
            
            output_tokens = len(response.content[0].text.split())
            input_cost = (input_tokens / 1000000) * 0.80
            output_cost = (output_tokens / 1000000) * 4.00
            total_cost = input_cost + output_cost
            
            logging.info(f"\nChannel Selection API Usage:")
            logging.info(f"Input tokens: {input_tokens:,}")
            logging.info(f"Output tokens: {output_tokens:,}")
            logging.info(f"Estimated cost: ${total_cost:.6f}")
            
            # Extract the JSON list from the response
            response_text = response.content[0].text.strip()
            logging.debug(f"Claude response: {response_text}")
            
            # Try to parse as JSON
            try:
                channel_ids = json.loads(response_text)
                if isinstance(channel_ids, list):
                    # Filter to only valid channel IDs
                    valid_ids = [cid for cid in channel_ids if cid in channels]
                    if valid_ids:
                        search_info['channel_selection_tokens'] = input_tokens + output_tokens
                        search_info['channel_selection_cost'] = total_cost
                        return valid_ids
            except json.JSONDecodeError:
                logging.error("Failed to parse Claude response as JSON")
            
            # If we get here, something went wrong with the response
            logging.error("Invalid response format from Claude")
            return list(channels.keys())[:3]  # Return first 3 channels as fallback
            
        except Exception as e:
            logging.error(f"Error determining relevant channels: {e}")
            return []

    async def generate_search_queries(self, question: str, search_info: Dict) -> List[Dict[str, str]]:
        """Generate search queries based on the question."""
        
        system_prompt = """Generate 2-3 precise search queries for finding information in Discord channels.
        Rules:
        - Keep queries very short (1-3 words)
        - Focus on the most specific, relevant terms
        - Avoid generic terms unless necessary
        - Include technical terms if relevant
        - Prioritize exact matches over broad concepts
        
        Format as JSON list with 'query' and 'reason' keys.
        Example for "How do I adjust video settings in Hunyuan?":
        [
            {"query": "video settings", "reason": "Most specific match for the question"},
            {"query": "resolution config", "reason": "Alternative technical term"}
        ]
        
        Question: """
        
        input_tokens = len(system_prompt.split()) + len(question.split())
        
        try:
            # Run the blocking Claude API call in a thread pool
            loop = asyncio.get_running_loop()
            
            def call_claude():
                return self.claude.messages.create(
                    model="claude-3-5-haiku-latest",
                    max_tokens=500,
                    messages=[{
                        "role": "user",
                        "content": system_prompt + question
                    }]
                )
            
            response = await loop.run_in_executor(None, call_claude)
            
            output_tokens = len(response.content[0].text.split())
            input_cost = (input_tokens / 1000000) * 0.80
            output_cost = (output_tokens / 1000000) * 4.00
            total_cost = input_cost + output_cost
            
            logging.info(f"\nQuery Generation API Usage:")
            logging.info(f"Input tokens: {input_tokens:,}")
            logging.info(f"Output tokens: {output_tokens:,}")
            logging.info(f"Estimated cost: ${total_cost:.6f}")
            
            search_info['query_generation_tokens'] = input_tokens + output_tokens
            search_info['query_generation_cost'] = total_cost
            
            # Try to parse as JSON
            try:
                queries = json.loads(response.content[0].text.strip())
                if isinstance(queries, list):
                    return queries
            except json.JSONDecodeError:
                logging.error("Failed to parse Claude response as JSON")
            
            # If we get here, something went wrong with the response
            logging.error("Invalid response format from Claude")
            return [{"query": question, "reason": "Fallback to original question"}]
            
        except Exception as e:
            logging.error(f"Error generating search queries: {e}")
            return [{"query": question, "reason": "Error occurred, using original question"}]

    async def search_channels(self, query: str, channels: List[int], limit: int = None) -> List[discord.Message]:
        """Search for messages in specified channels using archived data first."""
        self.logger.info(f"Searching channels for query: {query}")
        
        results = []
        db = None
        
        # Get database path from environment or use default
        db_path = os.getenv('DISCORD_ARCHIVE_DB_PATH', 
                            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'discord_archive.db'))
        
        try:
            db = DatabaseHandler(db_path)
            for channel_id in channels:
                archived_messages = db.search_messages(query, channel_id)
                
                # Convert archived messages back to discord.Message objects
                for msg_data in archived_messages:
                    channel = self.get_channel(msg_data['channel_id'])
                    if channel:
                        # Create partial Message object from archived data
                        message = discord.PartialMessage(
                            channel=channel,
                            id=msg_data['id']
                        )
                        # Fetch full message if needed
                        try:
                            full_message = await message.fetch()
                            results.append(full_message)
                        except discord.NotFound:
                            # Message was deleted, use archived data
                            message._update(msg_data)
                            results.append(message)
                            
                self.logger.info(f"Found {len(archived_messages)} archived matches in #{channel.name}")
                
            # If we didn't find enough results in archive, search recent messages
            if len(results) < (limit or 100):
                recent_results = await self._search_recent_messages(query, channels, limit)
                results.extend(recent_results)
                
        except Exception as e:
            self.logger.error(f"Error searching archive: {e}")
            # Fallback to searching recent messages
            results = await self._search_recent_messages(query, channels, limit)
        
        finally:
            if db:
                db.close()
        
        return results

    async def _search_recent_messages(self, query: str, channels: List[int], limit: int = None) -> List[discord.Message]:
        """Search only recent messages using Discord API."""
        results = []
        for channel_id in channels:
            try:
                channel = self.get_channel(channel_id)
                if not channel:
                    self.logger.warning(f"Could not find channel {channel_id}")
                    continue
                    
                self.logger.info(f"Searching recent messages in #{channel.name}")
                
                # Add delay between channel searches to avoid rate limits
                await asyncio.sleep(1)
                
                message_count = 0
                async for message in channel.history(limit=limit or 100):
                    message_count += 1
                    if query.lower() in message.content.lower():
                        results.append(message)
                        
                    if message_count % 100 == 0:
                        await asyncio.sleep(1)  # Rate limiting delay
                        
                self.logger.info(f"Found {len(results)} recent matches in #{channel.name}")
                
            except Exception as e:
                self.logger.error(f"Error searching channel {channel_id}: {e}")
                
        return results

    def format_messages_for_context(self, messages: List[discord.Message]) -> str:
        """Format messages for Claude context."""
        context = []
        for msg in messages:
            timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
            channel_name = msg.channel.name if msg.channel else "unknown-channel"
            context.append(f"[{timestamp}] #{channel_name} - {msg.author.name}: {msg.content}")
            if msg.attachments:
                context.append(f"[Attachments: {', '.join(a.filename for a in msg.attachments)}]")
            context.append(f"Message Link: {msg.jump_url}\n")
        return "\n".join(context)

    async def get_claude_answer(self, question: str, context: str, search_info: Dict) -> str:
        """Get answer from Claude using the context and search information."""
        try:
            prompt = f"""Based on the following context from Discord messages, please answer this question: {question}

Search Information:
Channels searched: {', '.join(f"#{self.channel_map.get(cid, str(cid))}" for cid in search_info['channels'])}
Queries used: {', '.join(q['query'] for q in search_info['queries'])}

Context:
{context}

Please provide a clear, concise answer that:
1. Directly addresses the question
2. Cites specific messages from the context when relevant (include Discord message links)
3. Acknowledges if certain aspects can't be fully answered from the available context
4. Uses Discord markdown formatting
5. Mentions which channels/queries were most helpful in finding the information

Answer:"""

            # Calculate input tokens
            input_tokens = len(prompt.split())  # Simple approximation
            
            # Run the blocking Claude API call in a thread pool
            loop = asyncio.get_running_loop()
            
            def call_claude():
                return self.claude.messages.create(
                    model="claude-3-5-haiku-latest",
                    max_tokens=1500,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }]
                )
            
            response = await loop.run_in_executor(None, call_claude)
            
            # Calculate output tokens and costs
            output_tokens = len(response.content[0].text.split())  # Simple approximation
            input_cost = (input_tokens / 1000000) * 0.80  # $0.80 per million tokens
            output_cost = (output_tokens / 1000000) * 4.00  # $4.00 per million tokens
            total_cost = input_cost + output_cost
            
            logging.info(f"\nClaude API Usage:")
            logging.info(f"Input tokens: {input_tokens:,}")
            logging.info(f"Output tokens: {output_tokens:,}")
            logging.info(f"Estimated cost: ${total_cost:.6f}")
            
            search_info['answer_generation_tokens'] = input_tokens + output_tokens
            search_info['answer_generation_cost'] = total_cost
            search_info['total_cost'] = (search_info.get('channel_selection_cost', 0) + 
                                       search_info.get('query_generation_cost', 0) + 
                                       search_info.get('answer_generation_cost', 0))
            
            return response.content[0].text
            
        except anthropic.APIError as e:
            logging.error(f"Claude API error: {e}")
            return "Sorry, I encountered an error generating the answer. Please try again later."
            
        except Exception as e:
            logging.error(f"Unexpected error getting Claude answer: {e}")
            return "An unexpected error occurred. Please try again later."

    async def create_answer_thread(self, channel_id: int, question_msg: discord.Message, answer: str, search_info: Dict):
        """Create a thread from the question message with the answer and search metadata."""
        try:
            # Create thread from the question message
            thread = await question_msg.create_thread(
                name=f"Answer: {question_msg.content[:50]}...",
                auto_archive_duration=1440
            )
            
            # Format initial status message
            status_msg = "🔍 **Searching...**\n"
            status_msg += f"Channels being searched:\n"
            for cid in search_info['channels']:
                channel_name = self.channel_map.get(cid, str(cid))
                status_msg += f"• #{channel_name}\n"
            
            status_msg += "\nQueries being used:\n"
            for q in search_info['queries']:
                status_msg += f"• `{q['query']}` ({q['reason']})\n"
            
            # Send initial status
            await thread.send(status_msg)
            
            # Format channel results and API usage for final metadata
            channel_results = []
            for cid in search_info['channels']:
                channel_name = self.channel_map.get(cid, str(cid))
                result_count = len([msg for msg in search_info.get('results', []) 
                                  if msg.channel.id == cid])
                channel_results.append(f"#{channel_name} ({result_count} results)")
            
            # Add API usage information
            api_usage = (
                f"\n\n**API Usage:**\n"
                f"Channel Selection: {search_info.get('channel_selection_tokens', 0):,} tokens (${search_info.get('channel_selection_cost', 0):.4f})\n"
                f"Query Generation: {search_info.get('query_generation_tokens', 0):,} tokens (${search_info.get('query_generation_cost', 0):.4f})\n"
                f"Answer Generation: {search_info.get('answer_generation_tokens', 0):,} tokens (${search_info.get('answer_generation_cost', 0):.4f})\n"
                f"Total Cost: ${search_info.get('total_cost', 0):.4f}"
            )
            
            # Send search metadata with API usage
            metadata = (
                f"*Search completed with:*\n"
                f"Queries: {', '.join(q['query'] for q in search_info['queries'])}\n"
                f"Channels: {', '.join(channel_results)}\n"
                f"Total unique results: {len(search_info.get('results', []))}"
                f"{api_usage}"
            )
            await thread.send(metadata)
            
            # Split answer into chunks if needed (Discord 2000 char limit)
            chunks = [answer[i:i+1900] for i in range(0, len(answer), 1900)]
            for chunk in chunks:
                await thread.send(chunk)
                
        except Exception as e:
            logging.error(f"Error creating answer thread: {e}")
            # Fallback: try to send as regular message if thread creation fails
            channel = self.get_channel(channel_id)
            if channel:
                await channel.send(f"Error creating thread: {e}\n\nAnswer to {question_msg.author.mention}:\n{answer}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle incoming messages."""
        if message.channel.id != self.answer_channel_id or message.author.bot:
            return
        
        # Add rate limiting check
        bucket = self.search_cooldown.get_bucket(message)
        retry_after = bucket.update_rate_limit()
        if retry_after:
            await message.reply(f"Please wait {int(retry_after)} seconds before searching again.")
            return
            
        # Check if message is from admin user
        admin_user_id = int(os.getenv('ADMIN_USER_ID'))
        if message.author.id != admin_user_id:
            await message.reply(f"Sorry, I only run queries for POM ")
            return
            
        # Process the question
        question = message.content
        logging.info(f"\nProcessing question: {question}")
        
        # Get all available channels
        self.channel_map = await self.get_searchable_channels()
        
        # Initialize search_info at the start
        search_info = {
            'channels': [],
            'queries': [],
            'results': [],
            'channel_selection_tokens': 0,
            'channel_selection_cost': 0,
            'query_generation_tokens': 0,
            'query_generation_cost': 0,
            'answer_generation_tokens': 0,
            'answer_generation_cost': 0,
            'total_cost': 0
        }
        
        # Determine which channels to search
        relevant_channels = await self.determine_relevant_channels(question, self.channel_map, search_info)
        
        # Generate search queries
        queries = await self.generate_search_queries(question, search_info)
        
        # Update search_info
        search_info.update({
            'channels': relevant_channels,
            'queries': queries,
            'results': []
        })
        
        # Collect all relevant messages
        all_results = []
        for query_dict in queries:
            query = query_dict['query']
            logging.info(f"\nExecuting search for query: {query}")
            results = await self.search_channels(query, relevant_channels)
            all_results.extend(results)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_results = []
        for msg in all_results:
            if msg.id not in seen:
                seen.add(msg.id)
                unique_results.append(msg)
        
        logging.info(f"\nFinal unique results: {len(unique_results)} messages")
        
        # Update search_info with results
        search_info['results'] = unique_results
        
        # Format context
        context = self.format_messages_for_context(unique_results)
        
        # Get answer from Claude
        answer = await self.get_claude_answer(question, context, search_info)
        
        # Create thread with answer
        await self.create_answer_thread(self.answer_channel_id, message, answer, search_info)

async def main():
    print("Starting main function...")
    try:
        bot = SearchAnswerBot()
        print("Bot instance created...")
        logging.info("Starting bot...")
        token = os.getenv('DISCORD_BOT_TOKEN')
        if not token:
            raise ValueError("DISCORD_BOT_TOKEN not found in environment variables")
        print("Starting bot with token...")
        await bot.start(token)
    except Exception as e:
        print(f"Error starting bot: {e}")
        logging.error(f"Error starting bot: {e}")
        raise

if __name__ == "__main__":
    print("Script starting...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot shutdown by user")
        logging.info("\nBot shutdown by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        logging.error(f"Fatal error: {e}")
        logging.exception("Full traceback:")