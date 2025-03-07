import os
import sys
import json
import logging
import traceback
from typing import List, Dict, Any, Optional
import asyncio

import anthropic
from dotenv import load_dotenv

# We removed direct Discord/bot usage here since SUMMARIZER handles posting logic now.
# This class now focuses on:
#  - queries to Claude (generate_news_summary, combine_channel_summaries, etc.)
#  - chunking/formatting the prompt & returned JSON.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsSummarizer:
    def __init__(self, dev_mode=False):
        logger.info("Initializing NewsSummarizer...")

        load_dotenv()
        self.dev_mode = dev_mode
        self.logger = logger

        # Initialize Claude client
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment")
        self.claude = anthropic.Anthropic(api_key=api_key)
        logger.info("Claude client initialized")

    def format_messages_for_claude(self, messages):
        """Format messages for Claude analysis."""
        conversation = """You MUST respond with ONLY a JSON array containing news items. NO introduction text, NO explanation, NO markdown formatting.

If there are no significant news items, respond with exactly "[NO SIGNIFICANT NEWS]".
Otherwise, respond with ONLY a JSON array in this exact format:

[
 {
   "title": "BFL ship new Controlnets for FluxText",
   "mainText": "A new ComfyUI analytics node has been developed to track and analyze data pipeline components, including inputs, outputs, and embeddings. This enhancement aims to provide more controllable prompting capabilities:",
   "mainFile": "https://discord.com/channels/1076117621407223829/1138865343314530324/4532454353425342.mp4, https://discord.com/channels/1076117621407223829/1138865343314530324/4532454353425343.png",
   "messageLink": "https://discord.com/channels/1076117621407223829/1138865343314530324/4532454353425342",
   "subTopics": [
     {
       "text": "Here's another example of **Kijai** using it in combination with **Redux** - **Kijai** noted that it worked better than the previous version:",
       "file": "https://discord.com/channels/1076117621407223829/1138865343314530324/4532454353425342.png",
       "messageLink": "https://discord.com/channels/1076117621407223829/1138865343314530324/4532454353425342"
     }
   ]
 }
]

Focus on these types of content:
1. New features or tools that were announced
2. Demos or images that got a lot of attention (especially messages with many reactions)
3. Focus on the things that people seem most excited about or commented/reacted to on a lot
4. Focus on AI art and AI art-related tools
5. Call out notable achievements or demonstrations
6. Important community announcements
7. Don't avoid negative news but try to frame it in a positive way 

IMPORTANT REQUIREMENTS FOR MEDIA AND LINKS:
1. Each topic MUST have at least one Discord message link (jump_url) and should try to include multiple relevant attachments
2. AGGRESSIVELY search for related media - include ALL images, videos, or links that are part of the same discussion. For each topic, try to find at least 2-3 related images/videos/examples if they exist
3. If you find multiple related pieces of media, include them all in mainFile as a comma-separated list
4. For each subtopic that references media or a demo, you MUST include both the media link and the Discord message link
5. Prioritize messages with reactions or responses when selecting media to include
6. Be careful not to bias towards just the first messages.
7. If a topic has interesting follow-up discussions or examples, include those as subtopics even if they don't have media
8. Always end with a colon if there are attachments or links ":"
9. Don't share the same attachment or link multiple times - even across different subtopics
10. file and mainfile should always be a direct link to the file

Requirements for the response:
1. Must be valid JSON in exactly the above format
2. Each news item must have all fields: title, mainText, mainFile (can be multiple comma-separated), messageLink, and subTopics
3. subTopics can include:
   - file (can be multiple comma-separated)
   - link (external links)
   - messageLink (required for all subtopics)
   - Both file and link can be included if relevant
4. Always end with a colon if there are attachments or links ":"
5. All usernames must be in bold with ** (e.g., "**username**") - ALWAYS try to give credit to the creator or state if opinions come from a specific person
6. If there are no significant news items, respond with exactly "[NO SIGNIFICANT NEWS]"
7. Include NOTHING other than the JSON response or "[NO SIGNIFICANT NEWS]"
8. Don't repeat the same item or leave any empty fields
9. When you're referring to groups of community members, refer to them as Banodocians 
10. Don't be hyperbolic or overly enthusiastic
11. If something seems to be a subjective opinion but still noteworthy, mention it as such: "Draken felt...", etc.

Here are the messages to analyze:

"""

        for msg in messages:
            conversation += f"=== Message from {msg['author_name']} ===\n"
            conversation += f"Time: {msg['created_at']}\n"
            conversation += f"Content: {msg['content']}\n"
            if msg['reaction_count']:
                conversation += f"Reactions: {msg['reaction_count']}\n"
            if msg['attachments']:
                conversation += "Attachments:\n"
                for attach in msg['attachments']:
                    if isinstance(attach, dict):
                        url = attach.get('url', '')
                        filename = attach.get('filename', '')
                        conversation += f"- {filename}: {url}\n"
                    else:
                        conversation += f"- {attach}\n"
            if msg.get('jump_url'):
                conversation += f"Message link: {msg['jump_url']}\n"
            conversation += "\n"

        conversation += "\nRemember: Respond with ONLY the JSON array or '[NO SIGNIFICANT NEWS]'. NO other text."
        return conversation

    async def generate_news_summary(self, messages: List[Dict[str, Any]]) -> str:
        """
        Generate a news summary from a given list of messages
        by sending them to Claude in chunks if needed.
        """
        if not messages:
            self.logger.warning("No messages to analyze")
            return "[NO MESSAGES TO ANALYZE]"

        chunk_size = 1000
        chunk_summaries = []
        previous_summary = None

        for i in range(0, len(messages), chunk_size):
            chunk = messages[i:i + chunk_size]
            self.logger.info(f"Summarizing chunk {i//chunk_size + 1} of {(len(messages) + chunk_size - 1)//chunk_size}")
            
            prompt = self.format_messages_for_claude(chunk)
            if previous_summary:
                prompt = f"""Previous summary chunk(s) contained these items:
{previous_summary}

DO NOT duplicate or repeat any of the topics, ideas, or media from above.
Only include NEW and DIFFERENT topics from the messages below.
If all significant topics have already been covered, respond with "[NO SIGNIFICANT NEWS]".

{prompt}"""
            
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.claude.messages.create(
                        model="claude-3-5-sonnet-latest",
                        max_tokens=8192,
                        messages=[{"role": "user", "content": prompt}]
                    )
                )
                text = response.content[0].text.strip()
                if text and text not in ["[NOTHING OF NOTE]", "[NO SIGNIFICANT NEWS]", "[NO MESSAGES TO ANALYZE]"]:
                    chunk_summaries.append(text)
                    if previous_summary:
                        previous_summary = previous_summary + "\n\n" + text
                    else:
                        previous_summary = text
            except anthropic.BadRequestError as e:
                # Attempt reduced chunk
                if "prompt is too long" in str(e):
                    self.logger.warning("Prompt too long, retrying with last 100 messages.")
                    reduced_chunk = chunk[-100:]
                    prompt = self.format_messages_for_claude(reduced_chunk)
                    if previous_summary:
                        prompt = f"""Previous summary chunk(s) contained these items:
{previous_summary}

DO NOT duplicate or repeat any of the topics, ideas, or media from above.
Only include NEW and DIFFERENT topics from the messages below.
If all significant topics have already been covered, respond with "[NO SIGNIFICANT NEWS]".

{prompt}"""
                    try:
                        loop = asyncio.get_running_loop()
                        response = await loop.run_in_executor(
                            None,
                            lambda: self.claude.messages.create(
                                model="claude-3-5-sonnet-latest",
                                max_tokens=8192,
                                messages=[{"role": "user", "content": prompt}]
                            )
                        )
                        text = response.content[0].text.strip()
                        if text and text not in ["[NOTHING OF NOTE]", "[NO SIGNIFICANT NEWS]", "[NO MESSAGES TO ANALYZE]"]:
                            chunk_summaries.append(text)
                            if previous_summary:
                                previous_summary = previous_summary + "\n\n" + text
                            else:
                                previous_summary = text
                    except Exception as e2:
                        self.logger.error(f"Error re-processing reduced chunk: {e2}")
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Error summarizing chunk: {e}")
                self.logger.debug(traceback.format_exc())

        if not chunk_summaries:
            return "[NO SIGNIFICANT NEWS]"
        
        if len(chunk_summaries) == 1:
            return chunk_summaries[0]

        # If multiple chunk summaries, combine them
        return await self.combine_channel_summaries(chunk_summaries)

    def format_news_for_discord(self, news_items_json: str) -> List[Dict[str, str]]:
        """
        Convert the JSON string from Claude into a list of dictionaries
        each containing a 'content' field that can be posted to Discord.
        """
        if news_items_json in ["[NO SIGNIFICANT NEWS]", "[NO MESSAGES TO ANALYZE]"]:
            return [{"content": news_items_json}]

        # Attempt to parse JSON
        try:
            idx = news_items_json.find('[')
            if idx == -1:
                return [{"content": news_items_json}]

            parsed_str = news_items_json[idx:]
            items = json.loads(parsed_str)
        except json.JSONDecodeError:
            return [{"content": news_items_json}]

        messages_to_send = []
        for item in items:
            main_part = []
            main_part.append(f"## {item.get('title','No Title')}\n")
            # mainText + messageLink
            # If there's a messageLink, append it
            if item.get("messageLink") and item["messageLink"] != "unknown":
                main_part.append(f"{item.get('mainText', '')} {item['messageLink']}")
            else:
                main_part.append(item.get('mainText', ''))

            messages_to_send.append({"content": "\n".join(main_part)})

            # mainFile
            if item.get("mainFile") and item["mainFile"] not in [None, "null", "unknown", ""]:
                # Could be multiple comma-separated
                for f_url in item["mainFile"].split(","):
                    f_url = f_url.strip()
                    if f_url:
                        messages_to_send.append({"content": f_url})

            # subTopics
            subs = item.get("subTopics", [])
            if subs:
                for sub in subs:
                    text = sub.get("text", "")
                    sub_msg = []
                    if sub.get("messageLink") and sub["messageLink"] != "unknown":
                        sub_msg.append(f"• {text} {sub['messageLink']}")
                    else:
                        sub_msg.append(f"• {text}")

                    messages_to_send.append({"content": "\n".join(sub_msg)})

                    if sub.get("file") and sub["file"] not in [None, "null", "unknown", ""]:
                        for f_url in sub["file"].split(","):
                            f_url = f_url.strip()
                            if f_url:
                                messages_to_send.append({"content": f_url})

        return messages_to_send

    async def combine_channel_summaries(self, summaries: List[str]) -> str:
        """
        Combine multiple summary JSON strings into a single filtered summary
        by asking Claude which items are the most interesting.
        """
        if not summaries:
            return "[NO SIGNIFICANT NEWS]"

        # Prepare a prompt that merges them
        prompt = """You are analyzing multiple JSON summaries. 
Each summary is in the same format: an array of objects with fields:
  title, mainText, mainFile, messageLink, subTopics (which is an array of objects with text, file, messageLink).
We want to combine them into a single JSON array that contains the top 3-5 most interesting items overall.
You MUST keep each chosen item in the exact same structure (all fields) as it appeared in the original input.

If no interesting items, respond with "[NO SIGNIFICANT NEWS]".
Otherwise, respond with ONLY a JSON array. No extra text.

Here are the input summaries:
"""

        for s in summaries:
            prompt += f"\n{s}\n"

        prompt += "\nReturn just the final JSON array with the top items (or '[NO SIGNIFICANT NEWS]')."

        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.claude.messages.create(
                    model="claude-3-5-sonnet-latest",
                    max_tokens=8192,
                    messages=[{"role": "user", "content": prompt}]
                )
            )
            return response.content[0].text.strip()
        except Exception as e:
            self.logger.error(f"Error combining summaries: {e}")
            return "[NO SIGNIFICANT NEWS]"

    async def generate_short_summary(self, full_summary: str, message_count: int) -> str:
        """
        Get a short summary using Claude with proper async handling.
        """
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

        max_retries = 3
        for attempt in range(max_retries):
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.claude.messages.create(
                        model="claude-3-5-haiku-latest",
                        max_tokens=8192,
                        messages=[{"role": "user", "content": conversation}]
                    )
                )
                return response.content[0].text.strip()
            except anthropic.APIError as e:
                self.logger.error(f"Claude API error (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    self.logger.info("Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    return f"📨 __{message_count} messages sent__\n• Unable to generate short summary due to API error."
            except asyncio.TimeoutError:
                self.logger.error(f"Timeout while generating short summary (attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    self.logger.info("Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    return f"📨 __{message_count} messages sent__\n• Unable to generate short summary due to timeout."
            except Exception as e:
                self.logger.error(f"Error generating short summary: {e}")
                if attempt < max_retries - 1:
                    self.logger.info("Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    return f"📨 __{message_count} messages sent__\n• Unable to generate short summary due to error: {str(e)}"


if __name__ == "__main__":
    def main():
        pass
    main()
