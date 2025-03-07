import functools
import logging
import traceback
import os
from typing import Optional
import discord

logger = logging.getLogger('DiscordBot')

def handle_errors(operation_name: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {operation_name}: {e}")
                logger.debug(traceback.format_exc())
                # If first arg is a discord.Client or commands.Bot instance, try to notify admin
                if args and isinstance(args[0], discord.Client):
                    bot = args[0]
                    try:
                        admin_id = int(os.getenv('ADMIN_USER_ID'))
                        admin_user = await bot.fetch_user(admin_id)
                        error_msg = f"🚨 **Critical Error in {operation_name}**\n```\n{str(e)}\n```"
                        if len(error_msg) > 1900:  # Discord message length limit
                            error_msg = error_msg[:1900] + "..."
                        await admin_user.send(error_msg)
                    except Exception as notify_error:
                        logger.error(f"Failed to notify admin of error: {notify_error}")
                raise
        return wrapper
    return decorator

class ErrorHandler:
    def __init__(self, bot: Optional[discord.Client] = None, *args, **kwargs):
        self.bot = bot
        self.logger = logging.getLogger('DiscordBot')
        
    async def notify_admin(self, error: Exception, context: str = ""):
        """Send error notification to admin user"""
        try:
            if not self.bot:
                logger.error("Cannot notify admin: bot instance not provided")
                return
                
            admin_id = int(os.getenv('ADMIN_USER_ID'))
            admin_user = await self.bot.fetch_user(admin_id)
            
            # Format error message
            error_msg = f"🚨 **Critical Error**\n"
            if context:
                error_msg += f"**Context:** {context}\n"
            error_msg += f"**Error:** {str(error)}\n"
            error_msg += f"```\n{traceback.format_exc()[:1500]}...\n```"  # Truncate if too long
            
            await admin_user.send(error_msg)
            
        except Exception as e:
            logger.error(f"Failed to send error notification to admin: {e}") 