"""Script to analyze Discord channels and generate descriptions using Claude."""
import os
import sys
import time
# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import discord
from discord.ext import commands
import anthropic
from dotenv import load_dotenv
import streamlit as st
import json
from typing import Dict, List, Optional
import logging
from src.common.db_handler import DatabaseHandler
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChannelAnalyzer(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guild_messages = True
        
        super().__init__(command_prefix="!", intents=intents)
        self.claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        # Don't initialize DB here - will be done in the thread that needs it

    def load_existing_descriptions(self):
        """Load existing channel descriptions from JSON file."""
        try:
            if os.path.exists(self.descriptions_file):
                with open(self.descriptions_file, 'r') as f:
                    self.channel_descriptions = json.load(f)
            else:
                self.channel_descriptions = {}
        except Exception as e:
            logger.error(f"Error loading descriptions: {e}")
            self.channel_descriptions = {}

    def save_descriptions(self):
        """Save channel descriptions to JSON file."""
        try:
            with open(self.descriptions_file, 'w') as f:
                json.dump(self.channel_descriptions, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving descriptions: {e}")

    async def get_channel_description(self, channel_data: Dict) -> Dict[str, str]:
        """Get channel description from Claude based on recent messages."""
        try:
            logger.info(f"Getting channel {channel_data['id']} from Discord...")
            
            # Wait for guild to be available
            retries = 0
            guild = None
            while retries < 5 and not guild:
                logger.info(f"Attempt {retries + 1}: Fetching guild with ID {os.getenv('GUILD_ID')}.")
                guild = self.get_guild(int(os.getenv('GUILD_ID')))
                if not guild:
                    logger.info("Guild not available yet, waiting...")
                    await asyncio.sleep(1)
                    retries += 1
                    
            if not guild:
                logger.error("Could not find guild after retries")
                raise ValueError(f"Could not find guild after {retries} attempts")
            
            channel = guild.get_channel(channel_data['id'])
            
            if not channel:
                logger.error(f"Could not find channel with ID {channel_data['id']}")
                raise ValueError(f"Could not find channel with ID {channel_data['id']}")

            logger.info(f"Collecting messages from channel #{channel_data['name']}...")
            # Collect last 100 messages
            messages = []
            async for message in channel.history(limit=100):
                messages.append(f"{message.author.name}: {message.content}")
            logger.info(f"Collected {len(messages)} messages")

            logger.info("Preparing Claude prompt...")
            prompt = f"""Analyze these messages from the Discord channel #{channel_data['name']} and create a clear description.
Return the response in this exact JSON format:
{{
    "description": "Brief overview of the channel's purpose",
    "suitable_posts": "Bullet list of what belongs here",
    "unsuitable_posts": "Bullet list of what doesn't belong here",
    "rules": "Any specific rules or guidelines"
}}

Good example:

{{
    "description": "For discussing academic papers, books, and publications related to artificial intelligence and machine learning",
    "suitable_posts": "• Links to academic papers on arXiv, similar repositories,AI/ML book recommendations
• Discussions related to the above",
    "unsuitable_posts": "• Stuff related to deep implementations 
- post in specific channels for that",
    "rules": "• Share links to legitimate academic sources or repositories
• No bullshit hype    
• Keep content focused on AI/ML literature and research
}}

Recent messages from #{channel_data['name']}:
{chr(10).join(messages)}"""

            logger.info("Sending request to Claude...")
            
            # Run the blocking Claude API call in a thread pool
            loop = asyncio.get_running_loop()
            
            def call_claude():
                return self.claude.messages.create(
                    model="claude-3-5-sonnet-latest",
                    max_tokens=1000,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }]
                )
            
            response = await loop.run_in_executor(None, call_claude)
            logger.info("Received response from Claude")

            # Sanitize the response text before parsing JSON
            response_text = response.content[0].text.strip()
            
            try:
                # Parse the JSON response
                description = json.loads(response_text)
                return description
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing Claude response as JSON: {e}")
                logger.error(f"Raw response: {response_text}")
                return {
                    "description": "Error parsing channel description",
                    "suitable_posts": "• Unable to determine",
                    "unsuitable_posts": "• Unable to determine",
                    "rules": "• Unable to determine"
                }
                
        except Exception as e:
            logger.error(f"Error getting channel description: {e}")
            logger.error(traceback.format_exc())
            return {
                "description": "Error getting channel description",
                "suitable_posts": "• Unable to determine",
                "unsuitable_posts": "• Unable to determine",
                "rules": "• Unable to determine"
            }

async def main():
    load_dotenv()
    
    # Add loading placeholder
    loading_placeholder = st.empty()
    loading_placeholder.info("Connecting to Discord...")
    
    try:
        bot = ChannelAnalyzer()
        
        @bot.event
        async def on_ready():
            try:
                logger.info(f"Logged in as {bot.user}")
                
                # Get all text channels
                guild = bot.get_guild(int(os.getenv('GUILD_ID')))
                if not guild:
                    loading_placeholder.error("Could not find guild")
                    return
                    
                # Create DB connection in this thread
                db = DatabaseHandler()
                
                # Store channel objects in a format that can be serialized
                channels_data = []
                for channel in guild.channels:
                    if isinstance(channel, discord.TextChannel):
                        channels_data.append({
                            'id': channel.id,
                            'name': channel.name,
                            'is_nsfw': channel.is_nsfw()
                        })
                        
                        # Add/update channel in database
                        result = db.execute_query(
                            "SELECT channel_id FROM channels WHERE channel_id = ?",
                            (channel.id,)
                        )
                        
                        if not result:
                            # Add new channel
                            db.execute_query("""
                                INSERT INTO channels 
                                (channel_id, channel_name, description, suitable_posts, 
                                 unsuitable_posts, rules, setup_complete, nsfw, enriched)
                                VALUES (?, ?, ?, ?, ?, ?, FALSE, ?, FALSE)
                            """, (
                                channel.id,
                                channel.name,
                                "",
                                "",
                                "",
                                "",
                                channel.is_nsfw()
                            ))
                        else:
                            # Update existing channel
                            db.execute_query("""
                                UPDATE channels 
                                SET channel_name = ?, nsfw = ?
                                WHERE channel_id = ?
                            """, (
                                channel.name,
                                channel.is_nsfw(),
                                channel.id
                            ))
                
                # Store channels data in session state
                st.session_state.channels_data = channels_data
                
                if channels_data:
                    loading_placeholder.success(f"Found {len(channels_data)} channels!")
                else:
                    loading_placeholder.warning("No channels found!")
                
            except Exception as e:
                logger.error(f"Error in on_ready: {e}")
                loading_placeholder.error(f"Error: {str(e)}")
            finally:
                await bot.close()

        await bot.start(os.getenv('DISCORD_BOT_TOKEN'))
        
    except Exception as e:
        logger.error(f"Main function error: {e}")
        loading_placeholder.error(f"Error: {str(e)}")

def create_streamlit_app():
    """Create Streamlit interface for reviewing channel descriptions."""
    # Create single database connection
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseHandler()
    
    st.title("Discord Channel Analysis")
    
    if 'channels_loaded' not in st.session_state:
        st.session_state.channels_loaded = False
        
    if 'channels_data' not in st.session_state:
        st.session_state.channels_data = []
        
    if 'current_channel_index' not in st.session_state:
        st.session_state.current_channel_index = 0
        
    if 'view_mode' not in st.session_state:
        st.session_state.view_mode = "All Channels"

    # Only show Discord connection button if channels aren't loaded
    if not st.session_state.channels_loaded:
        if st.button("Connect to Discord"):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Pass the database handler to ChannelAnalyzer
                bot = ChannelAnalyzer()
                loop.run_until_complete(main())
                st.session_state.channels_loaded = True
                st.rerun()
            finally:
                loop.close()
            return
        return

    # Use session state database connection throughout
    db = st.session_state.db
    
    # Channel view selector and bulk process button
    col1, col2 = st.columns([2, 3])
    with col1:
        view_mode = st.selectbox(
            "View Channels:",
            ["All Channels", "Enriched Channels", "Unenriched Channels", "Setup Complete Channels"],
            key="view_mode"
        )
    
    with col2:
        if st.button("Enrich All Channels"):  # Renamed from "Process All Channels"
            db = DatabaseHandler()
            unenriched_channels = []  # Renamed from unprocessed_channels
            
            # Get all unenriched channels
            for channel in st.session_state.channels_data:
                result = db.execute_query(
                    "SELECT setup_complete FROM channels WHERE channel_id = ? AND setup_complete = FALSE",
                    (channel['id'],)
                )
                if result:
                    unenriched_channels.append(channel)
            
            if unenriched_channels:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, channel in enumerate(unenriched_channels):
                    progress = (i + 1) / len(unenriched_channels)
                    progress_bar.progress(progress)
                    status_text.info(f"Analyzing channel {i + 1}/{len(unenriched_channels)}: #{channel['name']}")
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    async def process_single_channel():
                        try:
                            logger.info("Initializing ChannelAnalyzer for single channel processing.")
                            bot = ChannelAnalyzer()
                            logger.info("Logging in to Discord with the provided token.")
                            await bot.login(os.getenv('DISCORD_BOT_TOKEN'))
                            
                            # Create an event for bot ready
                            ready_event = asyncio.Event()
                            
                            @bot.event
                            async def on_ready():
                                logger.info("on_ready event triggered.")
                                ready_event.set()
                                
                            # Start a background task for the connection
                            connect_task = asyncio.create_task(bot.connect())
                            
                            # Wait for ready with timeout
                            try:
                                logger.info("Waiting for bot to be ready...")
                                await asyncio.wait_for(ready_event.wait(), timeout=30.0)
                                logger.info("Bot is ready, proceeding to get channel description.")
                                
                                analysis = await bot.get_channel_description(channel)
                                logger.info("Received analysis from Claude.")
                                
                                # Update the database with the analysis
                                db = DatabaseHandler()
                                db.execute_query("""
                                    UPDATE channels 
                                    SET description = ?, 
                                        suitable_posts = ?, 
                                        unsuitable_posts = ?, 
                                        rules = ?,
                                        enriched = TRUE
                                    WHERE channel_id = ?
                                """, (
                                    str(analysis.get('description', '')),
                                    str(analysis.get('suitable_posts', '')),
                                    str(analysis.get('unsuitable_posts', '')),
                                    str(analysis.get('rules', '')),
                                    channel['id']
                                ))
                                logger.info("Database updated with channel analysis.")
                                st.success("✅ Channel analysis complete!")
                                
                            except asyncio.TimeoutError:
                                logger.error("Timeout waiting for bot to become ready")
                                raise
                            finally:
                                # Ensure proper cleanup
                                logger.info("Cleaning up Discord connection...")
                                await bot.close()
                                if not connect_task.done():
                                    connect_task.cancel()
                                    try:
                                        await connect_task
                                    except asyncio.CancelledError:
                                        pass
                                
                                # Close any remaining connections
                                if not bot.http._HTTPClient__session.closed:
                                    await bot.http._HTTPClient__session.close()
                                
                        except Exception as e:
                            error_msg = f"Error processing channel: {str(e)}"
                            logger.error(error_msg)
                            status_text.error(error_msg)
                            raise
                    
                    try:
                        loop.run_until_complete(process_single_channel())
                        st.rerun()  # Refresh to show new data
                    finally:
                        loop.close()
                
                progress_bar.empty()
                status_text.success(f"Finished analyzing {len(unenriched_channels)} channels!")
            else:
                st.info("No unanalyzed channels found!")

    # Filter channels based on view mode
    filtered_channels = []
    
    if view_mode == "All Channels":
        filtered_channels = st.session_state.channels_data
    elif view_mode == "Enriched Channels":
        for channel in st.session_state.channels_data:
            result = db.execute_query(
                "SELECT channel_id FROM channels WHERE channel_id = ? AND enriched = TRUE",
                (channel['id'],)
            )
            if result:
                filtered_channels.append(channel)
    elif view_mode == "Unenriched Channels":
        for channel in st.session_state.channels_data:
            result = db.execute_query(
                "SELECT channel_id FROM channels WHERE channel_id = ? AND enriched = FALSE",
                (channel['id'],)
            )
            if result:
                filtered_channels.append(channel)
    elif view_mode == "Setup Complete Channels":
        for channel in st.session_state.channels_data:
            result = db.execute_query(
                "SELECT channel_id FROM channels WHERE channel_id = ? AND setup_complete = TRUE",
                (channel['id'],)
            )
            if result:
                filtered_channels.append(channel)

    # Filter toggle


    # Reset channel index if needed
    if st.session_state.current_channel_index >= len(filtered_channels):
        st.session_state.current_channel_index = 0

    # Show channel count
    st.write(f"Showing {len(filtered_channels)} channels")

    # Display current channel if any exist
    if filtered_channels:
        current_channel = filtered_channels[st.session_state.current_channel_index]
        st.header(f"Channel: #{current_channel['name']}")
        
        # Create new DB connection for this query
        db = DatabaseHandler()
        channel_data = db.execute_query("""
            SELECT description, suitable_posts, unsuitable_posts, rules, setup_complete, enriched
            FROM channels WHERE channel_id = ?
        """, (current_channel['id'],))[0]
        
        # Display channel status and process button
        status_col1, status_col2 = st.columns(2)
        with status_col1:
            st.write("📋 Status:")
            st.write(f"- Enriched: {'✅' if channel_data[5] else '❌'}")
            st.write(f"- Setup Complete: {'✅' if channel_data[4] else '❌'}")
        
        with status_col2:
            if st.button("Enrich This Channel"):
                status_text = st.empty()
                status_text.info(f"Analyzing channel #{current_channel['name']}...")
                logger.info(f"Starting analysis of channel #{current_channel['name']}")
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def process_single_channel():
                    try:
                        logger.info("Initializing ChannelAnalyzer for single channel processing.")
                        bot = ChannelAnalyzer()
                        logger.info("Logging in to Discord with the provided token.")
                        await bot.login(os.getenv('DISCORD_BOT_TOKEN'))
                        
                        # Create an event for bot ready
                        ready_event = asyncio.Event()
                        
                        @bot.event
                        async def on_ready():
                            logger.info("on_ready event triggered.")
                            ready_event.set()
                        
                        # Start a background task for the connection
                        connect_task = asyncio.create_task(bot.connect())
                        
                        # Wait for ready with timeout
                        try:
                            logger.info("Waiting for bot to be ready...")
                            await asyncio.wait_for(ready_event.wait(), timeout=30.0)
                            logger.info("Bot is ready, proceeding to get channel description.")
                            
                            analysis = await bot.get_channel_description(current_channel)
                            logger.info("Received analysis from Claude.")
                            
                            # Update the database with the analysis
                            db = DatabaseHandler()
                            db.execute_query("""
                                UPDATE channels 
                                SET description = ?, 
                                    suitable_posts = ?, 
                                    unsuitable_posts = ?, 
                                    rules = ?,
                                    enriched = TRUE
                                WHERE channel_id = ?
                            """, (
                                str(analysis.get('description', '')),
                                str(analysis.get('suitable_posts', '')),
                                str(analysis.get('unsuitable_posts', '')),
                                str(analysis.get('rules', '')),
                                current_channel['id']
                            ))
                            logger.info("Database updated with channel analysis.")
                            st.success("✅ Channel analysis complete!")
                            
                        except asyncio.TimeoutError:
                            logger.error("Timeout waiting for bot to become ready")
                            raise
                        finally:
                            # Ensure proper cleanup
                            logger.info("Cleaning up Discord connection...")
                            await bot.close()
                            if not connect_task.done():
                                connect_task.cancel()
                                try:
                                    await connect_task
                                except asyncio.CancelledError:
                                    pass
                            
                            # Close any remaining connections
                            if not bot.http._HTTPClient__session.closed:
                                await bot.http._HTTPClient__session.close()
                            
                    except Exception as e:
                        error_msg = f"Error processing channel: {str(e)}"
                        logger.error(error_msg)
                        status_text.error(error_msg)
                        raise
                
                try:
                    loop.run_until_complete(process_single_channel())
                    logger.info("Process complete, refreshing page...")
                    st.rerun()  # Refresh to show new data
                except Exception as e:
                    logger.error(f"Failed to process channel: {e}")
                    status_text.error(f"Failed to process channel: {e}")
                finally:
                    loop.close()
                    logger.info("Event loop closed")

        # Display editable fields
        description = st.text_area("Channel Description", value=channel_data[0], height=100)
        suitable_posts = st.text_area("Suitable Content", value=channel_data[1], height=150)
        unsuitable_posts = st.text_area("Unsuitable Content", value=channel_data[2], height=150)
        rules = st.text_area("Rules", value=channel_data[3], height=100)

        # Navigation and action buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("Previous") and st.session_state.current_channel_index > 0:
                st.session_state.current_channel_index -= 1
                st.rerun()
                
        with col2:
            if st.button("Save"):
                db.execute_query("""
                    UPDATE channels 
                    SET description = ?, 
                        suitable_posts = ?, 
                        unsuitable_posts = ?, 
                        rules = ?
                    WHERE channel_id = ?
                """, (
                    description,
                    suitable_posts,
                    unsuitable_posts,
                    rules,
                    current_channel['id']
                ))
                st.success("Changes saved!")
                
        with col3:
            if st.button("Complete Setup"):
                db.execute_query("""
                    UPDATE channels 
                    SET setup_complete = TRUE
                    WHERE channel_id = ?
                """, (current_channel['id'],))
                st.success("Channel setup marked as complete!")
                time.sleep(1)
                st.rerun()
                
        with col4:
            if st.button("Next") and st.session_state.current_channel_index < len(filtered_channels) - 1:
                st.session_state.current_channel_index += 1
                st.rerun()
                
        # Progress indicator
        st.progress((st.session_state.current_channel_index + 1) / len(filtered_channels))
        st.write(f"Channel {st.session_state.current_channel_index + 1} of {len(filtered_channels)}")
    else:
        st.info("No channels found matching the selected filter.")

if __name__ == "__main__":
    st.set_page_config(layout="wide")
    create_streamlit_app() 