import os
import re
import json
import sqlite3
import asyncio
import traceback
import discord
from typing import Optional
from datetime import datetime, timedelta

class TopArtSharing:
    def __init__(self, bot):
        self.bot = bot

    async def post_top_art_share(self, summary_channel: discord.TextChannel):
        """
        Finds the top art post (image or other) in the last 24h 
        from the art channel and sends a brief message about it.
        """
        try:
            self.bot.logger.info("Starting post_top_art_share")
            
            art_channel_id = int(os.getenv('DEV_ART_CHANNEL_ID' if self.bot.dev_mode else 'ART_CHANNEL_ID', 0))
            if art_channel_id == 0:
                self.bot.logger.error("Invalid art channel ID (0)")
                return

            self.bot.logger.info(f"Art channel ID: {art_channel_id}")
            
            yesterday = datetime.utcnow() - timedelta(hours=24)
            query = """
                SELECT 
                    m.message_id,
                    m.content,
                    m.attachments,
                    m.jump_url,
                    COALESCE(mem.server_nick, mem.global_name, mem.username) as author_name,
                    m.reactors,
                    m.embeds,
                    m.author_id,
                    CASE 
                        WHEN m.reactors IS NULL OR m.reactors = '[]' THEN 0
                        ELSE json_array_length(m.reactors)
                    END as unique_reactor_count
                FROM messages m
                JOIN members mem ON m.author_id = mem.member_id
                WHERE m.channel_id = ?
                AND m.created_at > ?
                AND json_valid(m.attachments)
                AND m.attachments != '[]'
                ORDER BY unique_reactor_count DESC
                LIMIT 1
            """
            try:
                self.bot.db.conn.row_factory = sqlite3.Row
                cursor = self.bot.db.conn.cursor()
                cursor.execute(query, (art_channel_id, yesterday.isoformat()))
                top_art = cursor.fetchone()
                
                if not top_art:
                    self.bot.logger.info("No art posts found in database for the last 24 hours.")
                    return
                    
                top_art = dict(top_art)
                attachments = json.loads(top_art['attachments'])
                if not attachments:
                    self.bot.logger.warning("No attachments found in top art post query result.")
                    return
                
                attachment = attachments[0]
                attachment_url = attachment.get('url')
                if not attachment_url:
                    self.bot.logger.error("No URL found in attachment for top art share.")
                    return
                
                # Check if content has a YouTube or Vimeo link
                has_video_link = False
                if top_art['content']:
                    has_video_link = any(x in top_art['content'].lower() for x in ['youtu.be', 'youtube.com', 'vimeo.com'])
                
                author_display = top_art['author_name']
                
                content_lines = [
                    f"## Top Art Sharing Post by {author_display}"
                ]
                
                if top_art['content'] and top_art['content'].strip():
                    # If it's purely a single video link, skip repeating the link. Otherwise show content
                    if not has_video_link or len(top_art['content'].split()) > 1:
                        # Replace user mentions
                        replaced = self._replace_user_mentions(top_art['content'])
                        content_lines.append(f"💭 *\"{replaced}\"*")
                
                # If the post was purely a single YouTube link or so, skip the URL duplication
                if not has_video_link or len(top_art['content'].split()) > 1:
                    content_lines.append(attachment_url)
                
                content_lines.append(f"🔗 Original post: {top_art['jump_url']}")
                
                formatted_content = "\n".join(content_lines)
                await self.bot.safe_send_message(summary_channel, formatted_content)
                self.bot.logger.info("Posted top art share successfully")

            except Exception as e:
                self.bot.logger.error(f"Database error in post_top_art_share: {e}")
                self.bot.logger.debug(traceback.format_exc())
        except Exception as e:
            self.bot.logger.error(f"Error posting top art share: {e}")
            self.bot.logger.debug(traceback.format_exc())

    def _replace_user_mentions(self, text: str) -> str:
        cursor = self.bot.db.conn.cursor()
        
        def replace_mention(match):
            user_id = match.group(1)
            cursor.execute("""
                SELECT COALESCE(server_nick, global_name, username) as display_name 
                FROM members 
                WHERE member_id = ?
            """, (user_id,))
            result = cursor.fetchone()
            return f"@{result[0] if result else 'unknown'}"
        
        escaped_content = re.sub(r'<@!?(\d+)>', replace_mention, text)
        return escaped_content

