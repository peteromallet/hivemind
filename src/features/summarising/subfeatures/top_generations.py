import os
import re
import json
import sqlite3
import asyncio
import traceback
import discord
from typing import Optional, List, Dict
from datetime import datetime, timedelta

class TopGenerations:
    def __init__(self, bot):
        """
        bot is an instance of your ChannelSummarizer (or a compatible class).
        We store it so we can access bot.db, bot.logger, bot.safe_send_message, etc.
        """
        self.bot = bot

    async def post_top_x_generations(
        self,
        summary_channel: discord.TextChannel,
        limit: int = 5,
        channel_id: Optional[int] = None,
        ignore_message_ids: Optional[List[int]] = None
    ):
        """
        (4) Send the top X gens post. 
        We'll just pick top `limit` video-type messages with >= 3 unique reactors in the last 24 hours,
        and post them in a thread.
        """
        try:
            self.bot.logger.info("Starting post_top_x_generations")
            yesterday = datetime.utcnow() - timedelta(hours=24)

            art_channel_id = int(os.getenv('DEV_ART_CHANNEL_ID' if self.bot.dev_mode else 'ART_CHANNEL_ID', 0))
            
            channel_condition = ""
            query_params = []
            
            # If dev mode, we only consider test channels – otherwise use your real channels
            if self.bot.dev_mode:
                # We might have "test" channels defined via env
                test_channels_str = os.getenv("TEST_DATA_CHANNEL", "")
                if not test_channels_str:
                    self.bot.logger.error("TEST_DATA_CHANNEL not set")
                    return
                
                test_channel_ids = [int(cid.strip()) for cid in test_channels_str.split(',') if cid.strip()]
                if not test_channel_ids:
                    self.bot.logger.error("No valid channel IDs found in TEST_DATA_CHANNEL")
                    return
                
                # We'll skip date filtering if you do local debug, or adapt as needed
                # For example, to keep exactly the same logic as original:
                date_condition = "1=1"
                channels_str = ','.join(str(c) for c in test_channel_ids)
                channel_condition = f" AND m.channel_id IN ({channels_str})"
            else:
                # Production: we do filter on date
                query_params.append(yesterday.isoformat())
                date_condition = "m.created_at > ?"
                
                if channel_id:
                    channel_condition = "AND m.channel_id = ?"
                    query_params.append(channel_id)
                else:
                    if self.bot.channels_to_monitor:
                        channels_str = ','.join(str(c) for c in self.bot.channels_to_monitor)
                        # Include sub-channels in the same categories
                        channel_condition = (
                            f" AND (m.channel_id IN ({channels_str}) "
                            f"     OR EXISTS (SELECT 1 FROM channels c2 WHERE c2.channel_id = m.channel_id AND c2.category_id IN ({channels_str})))"
                        )
            
            # Exclude the art channel from these top generations
            if art_channel_id != 0:
                channel_condition += f" AND m.channel_id != {art_channel_id}"

            ignore_condition = ""
            if ignore_message_ids and len(ignore_message_ids) > 0:
                ignore_ids_str = ','.join(str(mid) for mid in ignore_message_ids)
                ignore_condition = f" AND m.message_id NOT IN ({ignore_ids_str})"
            
            # Build a query that looks for attachments with .mp4/.mov/.webm, and 3+ unique reactors
            query = f"""
                WITH video_messages AS (
                    SELECT 
                        m.message_id,
                        m.channel_id,
                        m.content,
                        m.attachments,
                        m.reactors,
                        m.jump_url,
                        c.channel_name,
                        COALESCE(mem.server_nick, mem.global_name, mem.username) as author_name,
                        CASE 
                            WHEN m.reactors IS NULL OR m.reactors = '[]' THEN 0
                            ELSE json_array_length(m.reactors)
                        END as unique_reactor_count
                    FROM messages m
                    JOIN channels c ON m.channel_id = c.channel_id
                    JOIN members mem ON m.author_id = mem.member_id
                    WHERE {date_condition}
                    {channel_condition}
                    {ignore_condition}
                    AND json_valid(m.attachments)
                    AND m.attachments != '[]'
                    AND LOWER(c.channel_name) NOT LIKE '%nsfw%'
                    AND EXISTS (
                        SELECT 1
                        FROM json_each(m.attachments)
                        WHERE LOWER(json_extract(value, '$.filename')) LIKE '%.mp4'
                           OR LOWER(json_extract(value, '$.filename')) LIKE '%.mov'
                           OR LOWER(json_extract(value, '$.filename')) LIKE '%.webm'
                    )
                )
                SELECT *
                FROM video_messages
                WHERE unique_reactor_count >= 3
                ORDER BY unique_reactor_count DESC
                LIMIT {limit}
            """

            self.bot.db.conn.row_factory = sqlite3.Row
            cursor = self.bot.db.conn.cursor()
            cursor.execute(query, query_params)
            top_generations = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            self.bot.db.conn.row_factory = None
            
            if not top_generations:
                self.bot.logger.info(f"No qualifying videos found - skipping top {limit} gens post.")
                return None
            
            first_gen = top_generations[0]
            attachments = json.loads(first_gen['attachments'])
            
            # Find a video attachment in the first (top) generation
            video_attachment = next(
                (a for a in attachments if any(a.get('filename', '').lower().endswith(ext) 
                                               for ext in ('.mp4', '.mov', '.webm'))),
                None
            )
            if not video_attachment:
                return None
                
            desc = [
                f"## {'Top Generation' if len(top_generations) == 1 else f'Top {len(top_generations)} Generations'}"
                + (f" in #{first_gen['channel_name']}" if channel_id else "")
                + "\n",
                f"1. By **{first_gen['author_name']}**" + (f" in #{first_gen['channel_name']}" if not channel_id else "")
            ]
            
            # If there's text content, trim and un-mention
            if first_gen['content'] and first_gen['content'].strip():
                desc.append(self._replace_user_mentions(first_gen['content'][:150]))
            
            desc.append(f"🔥 {first_gen['unique_reactor_count']} unique reactions")
            desc.append(video_attachment['url'])
            desc.append(f"🔗 Original post: {first_gen['jump_url']}")
            msg_text = "\n".join(desc)
            
            header_message = await self.bot.safe_send_message(summary_channel, msg_text)
            
            # If multiple top gens, create a thread to list them
            if len(top_generations) > 1:
                thread = await self.bot.create_summary_thread(
                    header_message,
                    f"Top Generations - {self.bot._get_today_str()}",
                    is_top_generations=True
                )
                
                if not thread:
                    self.bot.logger.error("Failed to create thread for top generations")
                    return None
                
                # Post the rest (2..N)
                for i, row in enumerate(top_generations[1:], start=2):
                    gen = dict(row)
                    attachments = json.loads(gen['attachments'])
                    video_attachment = next(
                        (a for a in attachments if any(a.get('filename', '').lower().endswith(ext)
                                                       for ext in ('.mp4', '.mov', '.webm'))),
                        None
                    )
                    if not video_attachment:
                        continue
                    
                    desc = [
                        f"**{i}.** By **{gen['author_name']}**" + (f" in #{gen['channel_name']}" if not channel_id else "")
                    ]
                    
                    if gen['content'] and gen['content'].strip():
                        desc.append(self._replace_user_mentions(gen['content'][:150]))
                    
                    desc.append(f"🔥 {gen['unique_reactor_count']} unique reactions")
                    desc.append(video_attachment['url'])
                    desc.append(f"🔗 Original post: {gen['jump_url']}")
                    msg_text = "\n".join(desc)
                    
                    await self.bot.safe_send_message(thread, msg_text)
                    await asyncio.sleep(1)
            
            self.bot.logger.info("Posted top X gens successfully.")
            return top_generations[0] if top_generations else None

        except Exception as e:
            self.bot.logger.error(f"Error in post_top_x_generations: {e}")
            self.bot.logger.debug(traceback.format_exc())
            return None

    async def post_top_gens_for_channel(self, thread: discord.Thread, channel_id: int):
        """
        (5)(iv) Post the top gens from that channel that haven't yet been included,
        i.e., with over 3 reactions, in the last 24 hours.
        """
        try:
            self.bot.logger.info(f"Posting top gens for channel {channel_id} in thread {thread.name}")
            
            yesterday = datetime.utcnow() - timedelta(hours=24)
            
            query = """
                SELECT 
                    m.message_id,
                    m.content,
                    m.attachments,
                    m.jump_url,
                    COALESCE(mem.server_nick, mem.global_name, mem.username) as author_name,
                    CASE 
                        WHEN m.reactors IS NULL OR m.reactors = '[]' THEN 0
                        ELSE json_array_length(m.reactors)
                    END as unique_reactor_count
                FROM messages m
                JOIN members mem ON m.author_id = mem.member_id
                JOIN channels c ON m.channel_id = c.channel_id
                WHERE m.channel_id = ?
                AND m.created_at > ?
                AND json_valid(m.attachments)
                AND m.attachments != '[]'
                AND LOWER(c.channel_name) NOT LIKE '%nsfw%'
                AND EXISTS (
                    SELECT 1
                    FROM json_each(m.attachments)
                    WHERE LOWER(json_extract(value, '$.filename')) LIKE '%.mp4'
                       OR LOWER(json_extract(value, '$.filename')) LIKE '%.mov'
                       OR LOWER(json_extract(value, '$.filename')) LIKE '%.webm'
                )
                AND (
                    CASE 
                        WHEN m.reactors IS NULL OR m.reactors = '[]' THEN 0
                        ELSE json_array_length(m.reactors)
                    END
                ) >= 3
                ORDER BY unique_reactor_count DESC
                LIMIT 5
            """
            
            self.bot.db.conn.row_factory = sqlite3.Row
            cursor = self.bot.db.conn.cursor()
            cursor.execute(query, (channel_id, yesterday.isoformat()))
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            self.bot.db.conn.row_factory = None
            
            if not results:
                self.bot.logger.info(f"No top generations found for channel {channel_id}")
                return

            await self.bot.safe_send_message(thread, "\n## Top Generations\n")
            
            for i, row in enumerate(results, start=1):
                try:
                    attachments = json.loads(row['attachments'])
                    video_attachment = next(
                        (a for a in attachments if any(a.get('filename', '').lower().endswith(ext)
                                                       for ext in ('.mp4', '.mov', '.webm'))),
                        None
                    )
                    if not video_attachment:
                        continue
                    
                    desc = [
                        f"**{i}.** By **{row['author_name']}**",
                        f"🔥 {row['unique_reactor_count']} unique reactions"
                    ]
                    
                    if row['content'] and row['content'].strip():
                        desc.append(self._replace_user_mentions(row['content'][:150]))
                    
                    desc.append(video_attachment['url'])
                    desc.append(f"🔗 Original post: {row['jump_url']}")
                    msg_text = "\n".join(desc)
                    
                    await self.bot.safe_send_message(thread, msg_text)
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    self.bot.logger.error(f"Error processing generation {i}: {e}")
                    self.bot.logger.debug(traceback.format_exc())
                    continue

            self.bot.logger.info(f"Successfully posted top generations for channel {channel_id}")

        except Exception as e:
            self.bot.logger.error(f"Error in post_top_gens_for_channel: {e}")
            self.bot.logger.debug(traceback.format_exc())

    def _replace_user_mentions(self, text: str) -> str:
        """
        Replace <@123...> with @username lookups from DB for more readable messages.
        """
        cursor = self.bot.db.conn.cursor()

        def replace_mention(match):
            user_id = match.group(1)
            cursor.execute(
                """
                SELECT COALESCE(server_nick, global_name, username) as display_name 
                FROM members 
                WHERE member_id = ?
                """,
                (user_id,)
            )
            result = cursor.fetchone()
            return f"@{result[0] if result else 'unknown'}"

        escaped_content = re.sub(r'<@!?(\d+)>', replace_mention, text)
        return escaped_content

