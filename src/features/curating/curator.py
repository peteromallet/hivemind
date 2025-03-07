import discord
from discord.ext import commands
import asyncio
import os
import logging
import traceback
from src.common.log_handler import LogHandler
import json
from src.common.db_handler import DatabaseHandler
import datetime
from src.common.base_bot import BaseDiscordBot

class ArtCurator(BaseDiscordBot):
    def __init__(self, logger=None, dev_mode=False):
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
            gateway_queue_size=512,
            logger=logger
        )
        
        # Setup logger
        self.logger = logger or logging.getLogger('ArtCurator')
        self.logger.setLevel(logging.DEBUG)  # Set debug logging
        
        # Initialize variables that will be set by dev_mode setter
        self._dev_mode = None
        self.art_channel_id = None
        self.curator_ids = []
        
        # Set initial dev mode (this will trigger the setter to load correct IDs)
        self.dev_mode = True  # Always use dev mode for now
        
        # Add a set to track curators currently in rejection flow
        self._active_rejections = set()
        
        # Get BOT_USER_ID from environment
        self.bot_user_id = int(os.getenv('BOT_USER_ID', 0))
        
        # Dictionary to track messages waiting for reactions
        self._pending_reactions = {}
        
        # Register event handlers
        self.setup_events()
        
        # Shutdown flag for clean exit
        self._shutdown_flag = False

    async def cleanup(self):
        """Cleanup resources before shutdown."""
        self.logger.info("Starting cleanup...")
        
        # Cancel any pending tasks
        for pending_data in self._pending_reactions.values():
            if pending_data.get('task'):
                pending_data['task'].cancel()
        
        # Clear tracking dictionaries
        self._pending_reactions.clear()
        self._active_rejections.clear()
        
        # Close the session
        if hasattr(self, 'http'):
            if not self.http._session.closed:
                await self.http._session.close()
                self.logger.info("Closed HTTP session")
        
        self.logger.info("Cleanup completed")

    async def close(self):
        """Override close to ensure proper cleanup."""
        await self.cleanup()
        await super().close()
        
    @property
    def dev_mode(self):
        return self._dev_mode
        
    @dev_mode.setter
    def dev_mode(self, value):
        self._dev_mode = value
        # Update channel ID based on dev mode
        if value:
            self.art_channel_id = int(os.getenv('DEV_ART_CHANNEL_ID', 0))
            self.curator_ids = [int(id) for id in os.getenv('DEV_CURATOR_IDS', '').split(',') if id]
            self.logger.info(f"Using development art channel: {self.art_channel_id}")
            self.logger.info(f"Using development curator IDs: {self.curator_ids}")
        else:
            self.art_channel_id = int(os.getenv('ART_CHANNEL_ID', 0))
            self.curator_ids = [int(id) for id in os.getenv('CURATOR_IDS', '').split(',') if id]
            self.logger.info(f"Using production art channel: {self.art_channel_id}")
            self.logger.info(f"Using production curator IDs: {self.curator_ids}")
        
    def setup_events(self):
        @self.event
        async def on_ready():
            self.logger.info(f'{self.user} has connected to Discord!')
            self.logger.info(f'Bot is in {len(self.guilds)} guilds')
            self.logger.info(f'Intents configured: {self.intents}')
            self.logger.info(f'Art channel ID: {self.art_channel_id}')
            self.logger.info(f'Curator IDs: {self.curator_ids}')

        @self.event
        async def on_message(message):
            # Ignore messages from the bot itself
            if message.author == self.user:
                return

            # Only log message receipt in dev mode
            if self.dev_mode:
                self.logger.debug(f"Received message from {message.author} in channel {message.channel.id}")

            # Check if message is in the art channel
            if message.channel.id == self.art_channel_id:
                self.logger.info(f"Processing message in art channel from {message.author}")
                allowed_domains = [
                    'youtube.com', 'youtu.be',    # YouTube
                    'vimeo.com',                  # Vimeo
                    'tiktok.com',                 # TikTok
                    'streamable.com',             # Streamable
                    'twitch.tv',                  # Twitch
                    'fixupx.com',                 # FixupX
                    'fxtwitter.com',              # Twitter Embed Fixes
                    'vxtwitter.com',
                    'twittpr.com',
                    'ddinstagram.com',            # Instagram Embed Fix
                    'rxddit.com'                  # Reddit Embed Fix
                ]

                # Check if attachments are valid media files
                has_valid_attachment = any(
                    (attachment.content_type and (
                        attachment.content_type.startswith('image/') or 
                        attachment.content_type.startswith('video/')
                    )) or 
                    (attachment.filename.lower().endswith(
                        ('.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tiff', '.svg',
                         '.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv', '.wmv', '.m4v',
                         '.heic', '.heif',
                         '.apng')
                    ))
                    for attachment in message.attachments
                )

                if self.dev_mode:
                    self.logger.debug(f"Message has valid attachment: {has_valid_attachment}")

                # Check for any links in the message
                links = [word for word in message.content.split() 
                        if 'http://' in word or 'https://' in word]
                
                if self.dev_mode:
                    self.logger.debug(f"Found links in message: {links}")
                
                # Check if all links are from allowed domains
                all_links_allowed = all(
                    any(domain in link.lower() for domain in allowed_domains)
                    for link in links
                ) if links else True  # True if no links present

                # Check if any of the links are from allowed domains (for reaction purposes)
                has_valid_link = any(
                    domain in link.lower() 
                    for domain in allowed_domains 
                    for link in links
                )

                if self.dev_mode:
                    self.logger.debug(f"Message has valid link: {has_valid_link}")
                    self.logger.debug(f"All links are allowed: {all_links_allowed}")

                # If there are any links and not all are from allowed domains, suppress embeds
                if links and not all_links_allowed:
                    try:
                        await message.edit(suppress=True)
                        self.logger.info(f"Suppressed embeds for message from {message.author} - contains non-video links.")
                        
                        # Check if user has already received the notification
                        try:
                            member = await message.guild.fetch_member(message.author.id)
                            if member:
                                # Get member's notifications from database
                                db = DatabaseHandler(dev_mode=self.dev_mode)
                                member_data = db.get_member(member.id)
                                notifications = json.loads(member_data.get('notifications', '[]')) if member_data else []
                                
                                if 'no_art_share_link' not in notifications:
                                    # Send notification message
                                    notification_msg = (
                                        f"Hi {message.author.mention},\n\n"
                                        "You posted a link to art sharing that didn't seem like it would embed a video on Discord. "
                                        "For the sake of the Discord viewing experience, we hide these links.\n\n"
                                        "If you meant to post a video, please share a link to a platform that embeds links on Discord (YT, etc.) or a file "
                                        "- alongside a non-embedding link if you like!\n\n"
                                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                        "YouTube, TikTok, and Vimeo links embed natively, but some other popular services don't.\n\n"
                                        "Below are some services that allow you to embed links by using them instead of the original domain:\n\n"
                                        "• Reddit: rxddit.com\n"
                                        "• Instagram: ddinstagram.com\n"
                                        "• Twitter: fixupx.com\n\n"
                                        "Example of using a mirror link:\n"
                                        "Instead of: https://reddit.com/r/news/comments/123abc\n"
                                        "Use: https://rxddit.com/r/news/comments/123abc\n\n"
                                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                        "Love,\n"
                                        "BNDC"
                                    )
                                    try:
                                        await message.author.send(notification_msg)
                                        self.logger.info(f"Sent non-video link notification to {message.author}")
                                        
                                        # Add notification to user's list
                                        notifications.append('no_art_share_link')
                                        db.execute_query(
                                            "UPDATE members SET notifications = ? WHERE member_id = ?",
                                            (json.dumps(notifications), member.id)
                                        )
                                        db.conn.commit()
                                    except discord.Forbidden:
                                        self.logger.warning(f"Could not send DM to {message.author}")
                                db.close()
                        except Exception as e:
                            self.logger.error(f"Error handling notification for {message.author}: {e}")
                            
                    except discord.Forbidden:
                        self.logger.error("Bot doesn't have permission to edit messages.")
                    except Exception as e:
                        self.logger.error(f"Error editing message: {e}", exc_info=True)

                # Track valid messages for potential reaction
                if has_valid_attachment or has_valid_link:
                    self._pending_reactions[message.id] = {
                        'message': message,
                        'has_other_reaction': False,
                        'task': None,
                        'is_new': True  # Flag to indicate this is a new message
                    }

            # Process commands after handling the message
            await self.process_commands(message)

        @self.event
        async def on_reaction_add(reaction, user):
            # Skip this handler as we'll handle everything in on_raw_reaction_add
            pass

        @self.event
        async def on_raw_reaction_add(payload):
            if self.dev_mode:
                self.logger.debug(f"Raw reaction event received: {payload.emoji}")
            
            # Skip if reaction is from bot
            if payload.user_id == self.user.id:
                if self.dev_mode:
                    self.logger.debug(f"Skipping bot's own reaction for message {payload.message_id}")
                return

            # Check if in art channel
            if payload.channel_id == self.art_channel_id:
                # Get the channel and message
                channel = self.get_channel(payload.channel_id)
                if channel is None:
                    self.logger.error(f"Channel with ID {payload.channel_id} not found.")
                    return

                try:
                    message = await channel.fetch_message(payload.message_id)
                    
                    # Check if message has valid content
                    has_valid_attachment = any(
                        (attachment.content_type and (
                            attachment.content_type.startswith('image/') or 
                            attachment.content_type.startswith('video/')
                        )) or 
                        (attachment.filename.lower().endswith(
                            ('.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tiff', '.svg',
                             '.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv', '.wmv', '.m4v',
                             '.heic', '.heif',
                             '.apng')
                        ))
                        for attachment in message.attachments
                    )

                    # Check for valid links
                    allowed_domains = [
                        'youtube.com', 'youtu.be',    # YouTube
                        'vimeo.com',                  # Vimeo
                        'tiktok.com',                 # TikTok
                        'streamable.com',             # Streamable
                        'twitch.tv',                  # Twitch
                        'fixupx.com',                 # FixupX
                        'fxtwitter.com',              # Twitter Embed Fixes
                        'vxtwitter.com',
                        'twittpr.com',
                        'ddinstagram.com',            # Instagram Embed Fix
                        'rxddit.com'                  # Reddit Embed Fix
                    ]
                    
                    links = [word for word in message.content.split() 
                            if 'http://' in word or 'https://' in word]
                    has_valid_link = any(
                        domain in link.lower() 
                        for domain in allowed_domains 
                        for link in links
                    )

                    # If message has valid content
                    if (has_valid_attachment or has_valid_link):
                        # Check if we already have either reaction
                        has_love_letter = False
                        has_inbox_tray = False
                        for reaction in message.reactions:
                            if str(reaction.emoji) == '💌':
                                async for user in reaction.users():
                                    if user.id == self.user.id:
                                        has_love_letter = True
                                        break
                            elif str(reaction.emoji) == '📥':
                                async for user in reaction.users():
                                    if user.id == self.user.id:
                                        has_inbox_tray = True
                                        break
                            if has_love_letter or has_inbox_tray:
                                break

                        # If neither reaction exists, proceed with adding inbox tray
                        if not has_love_letter and not has_inbox_tray:
                            # If message is already being tracked, update it
                            if message.id in self._pending_reactions:
                                pending_data = self._pending_reactions[message.id]
                                if not pending_data['has_other_reaction']:
                                    if self.dev_mode:
                                        self.logger.debug(f"First non-bot reaction detected for tracked message {payload.message_id}")
                                    pending_data['has_other_reaction'] = True
                                    
                                    # Schedule the reaction
                                    async def add_delayed_reaction(message_id):
                                        try:
                                            if self.dev_mode:
                                                self.logger.debug(f"Starting 5 second delay for reaction on message {message_id}")
                                            await asyncio.sleep(5)  # Reduced from 30 to 5 for testing
                                            
                                            try:
                                                # Refetch message to ensure it's still valid
                                                message = await channel.fetch_message(message_id)
                                                # Check again for either reaction in case one was added during the delay
                                                has_either_reaction = any(
                                                    (str(reaction.emoji) in ['💌', '📥'] and 
                                                    any(user.id == self.user.id async for user in reaction.users()))
                                                    for reaction in message.reactions
                                                )
                                                if not has_either_reaction:
                                                    await message.add_reaction('📥')
                                                    self.logger.info(f"Added delayed inbox tray reaction to message from {message.author}.")
                                            except discord.NotFound:
                                                self.logger.info(f"Message {message_id} was deleted before adding delayed reaction")
                                            except Exception as e:
                                                self.logger.error(f"Error adding delayed reaction: {e}", exc_info=True)
                                            
                                            # Clean up tracking
                                            if message_id in self._pending_reactions:
                                                del self._pending_reactions[message_id]
                                                
                                        except Exception as e:
                                            self.logger.error(f"Error in delayed reaction task: {e}", exc_info=True)
                                    
                                    pending_data['task'] = asyncio.create_task(add_delayed_reaction(message.id))
                            else:
                                # For untracked messages, schedule the reaction immediately
                                if self.dev_mode:
                                    self.logger.debug(f"Scheduling delayed reaction for untracked message {payload.message_id}")
                                
                                async def add_delayed_reaction(message_id):
                                    try:
                                        await asyncio.sleep(5)  # Reduced from 30 to 5 for testing
                                        try:
                                            message = await channel.fetch_message(message_id)
                                            # Check again for either reaction in case one was added during the delay
                                            has_either_reaction = any(
                                                (str(reaction.emoji) in ['💌', '📥'] and 
                                                any(user.id == self.user.id async for user in reaction.users()))
                                                for reaction in message.reactions
                                            )
                                            if not has_either_reaction:
                                                await message.add_reaction('📥')
                                                self.logger.info(f"Added delayed inbox tray reaction to untracked message from {message.author}.")
                                        except discord.NotFound:
                                            self.logger.info(f"Message {message_id} was deleted before adding delayed reaction")
                                        except Exception as e:
                                            self.logger.error(f"Error adding delayed reaction: {e}", exc_info=True)
                                    except Exception as e:
                                        self.logger.error(f"Error in delayed reaction task: {e}", exc_info=True)
                                
                                asyncio.create_task(add_delayed_reaction(payload.message_id))

                except Exception as e:
                    self.logger.error(f"Error processing reaction: {e}", exc_info=True)
                    return
            
            # Check if reaction is love letter box or inbox tray (for thread creation)
            if (str(payload.emoji) in ['💌', '📥'] and 
                payload.channel_id == self.art_channel_id):
                
                # Log the reaction details in dev mode
                if self.dev_mode:
                    self.logger.debug(f"Love letter or inbox tray reaction - Reactor ID: {payload.user_id}, Bot ID: {self.user.id}")
                
                # Skip if reaction is from any bot
                if payload.user_id == self.user.id:
                    self.logger.info("Ignoring bot's own reaction")
                    return
                
                self.logger.info(f"Love letter or inbox tray reaction received in art channel from user {payload.user_id}")
                
                # Get the channel and message
                channel = self.get_channel(payload.channel_id)
                if channel is None:
                    self.logger.error(f"Channel with ID {payload.channel_id} not found.")
                    return
                try:
                    message = await channel.fetch_message(payload.message_id)
                    
                    # Create thread if one doesn't exist
                    if not message.thread:
                        self.logger.info("No existing thread found, creating new thread...")
                        thread = await message.create_thread(
                            name=f"Discussion: {message.author.display_name}'s Art",
                            auto_archive_duration=10080  # Archives after 1 week of inactivity
                        )
                        self.logger.info(f"Created thread {thread.id} for message {message.id}")
                        
                        # Remove all inbox tray reactions after thread creation
                        for reaction in message.reactions:
                            if str(reaction.emoji) == '📥':
                                try:
                                    await message.clear_reaction('📥')
                                    self.logger.info("Removed inbox tray reactions after thread creation")
                                except Exception as e:
                                    self.logger.error(f"Error removing inbox tray reactions: {e}")
                                break
                        
                        # Delete the system message about thread creation
                        try:
                            # Wait a moment for the system message to appear
                            await asyncio.sleep(2)
                            
                            # Get the message directly from the parent channel, just like test script
                            self.logger.info(f"Attempting to fetch system message from parent channel {channel.name} ({channel.id})")
                            async for msg in channel.history(limit=5):
                                if msg.type == discord.MessageType.thread_created and msg.id != message.id:
                                    # Get message details just like test script
                                    self.logger.info(f"Found message details:")
                                    self.logger.info(f"  ID: {msg.id}")
                                    self.logger.info(f"  Author: {msg.author} ({msg.author.id})")
                                    self.logger.info(f"  Type: {msg.type}")
                                    self.logger.info(f"  Content: {msg.content}")
                                    self.logger.info(f"  Created at: {msg.created_at}")
                                    self.logger.info(f"  Channel: {msg.channel.name} ({msg.channel.id})")
                                    
                                    # Check if we should delete this message - same check as test script
                                    if msg.author.id not in [self.user.id, self.bot_user_id]:
                                        self.logger.warning(f"Message author {msg.author.id} is not bot or target user - skipping deletion")
                                        continue
                                    
                                    # Try to delete it - same as test script
                                    self.logger.info("Attempting to delete message...")
                                    await msg.delete()
                                    self.logger.info(f"Successfully deleted message {msg.id}")
                                    break
                            
                        except discord.NotFound:
                            self.logger.error(f"Message not found")
                        except discord.Forbidden as e:
                            self.logger.error(f"Forbidden to delete message: {e}")
                        except Exception as e:
                            self.logger.error(f"Error deleting message: {e}")
                            self.logger.error(f"Full traceback: {traceback.format_exc()}")
                        
                        # Add message about tagging the author
                        await thread.send(f"Make sure to tag \@{message.author.name} in messages to make sure they see your comment!")
                        self.logger.info(f"Added tagging reminder message to thread.")
                        
                        # Schedule the activity check
                        asyncio.create_task(self._check_thread_activity(thread, message))
                        self.logger.info(f"Scheduled activity check for thread {thread.id}")
                        
                except Exception as e:
                    self.logger.error(f"Error creating thread: {e}", exc_info=True)
                    return
            
            # Handle X reaction for curators
            elif (str(payload.emoji) in ['❌', '𝕏', 'X', '🇽'] and 
                  payload.channel_id == self.art_channel_id):
                
                self.logger.info(f"X reaction received in art channel from user {payload.user_id}")
                
                if self.dev_mode:
                    self.logger.debug(f"Reaction from user ID {payload.user_id}. Curators are: {self.curator_ids}")
                
                # Get the channel and message
                channel = self.get_channel(payload.channel_id)
                if channel is None:
                    self.logger.error(f"Channel with ID {payload.channel_id} not found.")
                    return
                try:
                    message = await channel.fetch_message(payload.message_id)
                    user = self.get_user(payload.user_id)
                    if user is None:
                        self.logger.error(f"User with ID {payload.user_id} not found.")
                        return
                    
                    # Check if reactor is a curator
                    if payload.user_id in self.curator_ids:
                        self.logger.info(f"Valid curator {user.name} ({payload.user_id}) reacted with X")
                        await self._handle_curator_rejection(message, user)
                    else:
                        self.logger.info(f"Non-curator {user.name} ({payload.user_id}) reacted with X - ignoring")
                        
                except Exception as e:
                    self.logger.error(f"Error fetching message or user: {e}", exc_info=True)
                    return

        @self.event
        async def on_error(event, *args, **kwargs):
            self.logger.error(f'Error in {event}:', exc_info=True)
            traceback.print_exc()

    async def _handle_curator_rejection(self, message, user):
        """Handle the rejection process when a curator reacts with X"""
        # Check if curator is already processing a rejection
        if user.id in self._active_rejections:
            try:
                await user.send("Please complete your current rejection process before starting a new one. Reply to the above message with your reason for rejection or reply 'forget' to stop that rejection.")
                await message.remove_reaction('❌', user)
                self.logger.info(f"Curator {user.name} attempted multiple rejections - blocked.")
                return
            except discord.Forbidden:
                self.logger.error(f"Couldn't DM curator {user.name} about multiple rejections.")
                return

        self._active_rejections.add(user.id)
        self.logger.warning(f"Curator {user.name} initiated a rejection.")
        
        try:
            # Get the content URL or first attachment URL
            content_url = ""
            if message.attachments:
                content_url = message.attachments[0].url
            elif message.content:
                # Extract first URL from content if it exists
                words = message.content.split()
                urls = [word for word in words if word.startswith(('http://', 'https://'))]
                if urls:
                    content_url = urls[0]

            # DM curator asking for reason
            prompt = f"You rejected an art post by <@{message.author.id}>"
            if content_url:
                prompt += f": {content_url}"
            prompt += "\n\nPlease reply with the reason for rejection within 5 minutes or reply 'forget' to stop rejection:"
            
            await user.send(prompt)
            if self.dev_mode:
                self.logger.debug(f"Sent DM to curator {user.name} for reason.")

            def check(m):
                return m.author == user and isinstance(m.channel, discord.DMChannel)
            
            # Wait for curator's response
            try:
                reason_msg = await self.wait_for('message', check=check, timeout=300.0)
                reason = reason_msg.content
                
                # Check if curator wants to cancel
                if reason.lower().strip() == 'forget':
                    await user.send("Rejection cancelled.")
                    await message.remove_reaction('❌', user)
                    self.logger.warning(f"Curator {user.name} cancelled the rejection.")
                    return
                
                if self.dev_mode:
                    self.logger.info(f"Received rejection reason from {user.name}: {reason}")

                # Only proceed with deletion if we got a non-empty reason
                if reason.strip():
                    # Store author before deleting message
                    author = message.author
                    
                    # Delete the post first
                    await message.delete()
                    self.logger.warning(f"Deleted message from {author} as per curator {user.name}.")

                    # Format the reason
                    formatted_reason = "" + reason.strip().replace('\n', '\n> ')
                    
                    # Send DM to the original author
                    try:
                        # Format the message parts separately to handle newlines
                        message_parts = [
                            f"Hi <@{author.id}>,\n\n",
                            f"I'm sorry to say that your art post was removed by curator <@{user.id}>.\n\n",
                            f"**Reason for removal:**\n> {formatted_reason}\n\n"
                        ]
                        
                        # Add file reference if there was an attachment
                        if message.attachments:
                            message_parts.extend([
                                "**Your submission:**\n",
                                f"> {message.attachments[0].url}\n\n"
                            ])
                        elif message.content.strip():  # If no attachment but has content (likely a link)
                            content = message.content.strip()
                            urls = [word for word in content.split() 
                                  if word.startswith(('http://', 'https://'))]
                            
                            # Add the first URL if found
                            if urls:
                                message_parts.extend([
                                    "**Your submission:**\n",
                                    f"> {urls[0]}\n\n"
                                ])
                                
                                # Remove the URL from content and check if there's remaining text
                                remaining_content = ' '.join(
                                    word for word in content.split() 
                                    if not word.startswith(('http://', 'https://'))
                                ).strip()
                                
                                if remaining_content:
                                    message_parts.extend([
                                        "**Your comment:**\n",
                                        f"> {remaining_content}\n\n"
                                    ])
                            else:
                                message_parts.extend([
                                    "**Your comment:**\n",
                                    f"> {content}\n\n"
                                ])

                        # Add footer
                        message_parts.append(f"If you would like to discuss this further, please DM <@{user.id}> directly.")

                        # Join all parts and send
                        final_message = ''.join(message_parts)
                        await author.send(final_message)
                        if self.dev_mode:
                            self.logger.info(f"Sent removal reason DM to {author}.")
                        
                        # Get all threads in the channel
                        threads = await message.guild.active_threads()
                        if self.dev_mode:
                            self.logger.debug(f"Found {len(threads)} active threads")
                            for thread in threads:
                                self.logger.debug(f"Thread {thread.id}: parent={thread.parent_id}, starter={thread.starter_message.id if thread.starter_message else 'None'}")
                        
                        # Find the thread that was started from this message and is in the correct channel
                        message_thread = None
                        for thread in threads:
                            try:
                                if (thread.parent_id == message.channel.id and 
                                    thread.name.startswith(f"Discussion: {message.author.display_name}")):
                                    message_thread = thread
                                    break
                            except Exception as e:
                                if self.dev_mode:
                                    self.logger.debug(f"Error checking thread {thread.id}: {e}")
                                continue
                        
                        if message_thread:
                            # Delete all messages in the thread first
                            try:
                                async for msg in message_thread.history(limit=None, oldest_first=False):
                                    await msg.delete()
                                    if self.dev_mode:
                                        self.logger.info(f"Deleted message in thread for {author}'s post")
                            except Exception as e:
                                self.logger.error(f"Error deleting thread messages: {e}", exc_info=True)

                            # Then delete the thread itself
                            try:
                                await message_thread.delete()
                                if self.dev_mode:
                                    self.logger.info(f"Deleted thread for message from {author}.")
                            except Exception as e:
                                self.logger.error(f"Error deleting thread: {e}", exc_info=True)
                        else:
                            if self.dev_mode:
                                self.logger.debug(f"No thread found for message from {author}.")
                    except Exception as e:
                        self.logger.error(f"Error handling thread deletion: {e}", exc_info=True)
                                
                    except discord.Forbidden:
                        self.logger.warning(f"Couldn't DM original poster {author}.")
                else:
                    await user.send("Empty reason provided. Post will not be deleted.")
                    await message.remove_reaction('❌', user)
                    self.logger.warning(f"Empty reason provided by {user.name}. Reaction removed.")
                
            except asyncio.TimeoutError:
                await user.send("No reason provided within 5 minutes. Post will not be deleted.")
                await message.remove_reaction('❌', user)
                self.logger.warning(f"Curator {user.name} did not provide a reason in time.")
                
        except discord.Forbidden:
            self.logger.error(f"Couldn't DM curator {user.name}.")

        finally:
            # Make sure we remove the curator from active rejections even if there's an error
            self._active_rejections.remove(user.id)

    async def _check_thread_activity(self, thread, original_message):
        """Check if a thread has any activity after 30 minutes and delete if inactive."""
        try:
            # Wait 30 minutes
            await asyncio.sleep(1800)  # 30 minutes in seconds
            
            # Fetch the thread again to ensure it still exists
            try:
                thread = await self.fetch_channel(thread.id)
            except discord.NotFound:
                # Thread was already deleted
                return
            
            # Get all messages in the thread
            messages = []
            async for msg in thread.history(limit=None, oldest_first=True):
                messages.append(msg)
            
            # If there are only 1 message (the tagging reminder), consider it inactive
            if len(messages) <= 1:
                self.logger.info(f"Thread {thread.id} inactive after 30 minutes - deleting")
                
                # Delete the thread
                await thread.delete()
                
                try:
                    # Re-add the inbox reaction to the original message
                    await original_message.add_reaction('📥')
                    self.logger.info(f"Re-added inbox reaction to message {original_message.id}")
                except Exception as e:
                    self.logger.error(f"Error re-adding reaction: {e}")
            else:
                self.logger.info(f"Thread {thread.id} has activity - keeping thread")
                
        except Exception as e:
            self.logger.error(f"Error checking thread activity: {e}", exc_info=True)
