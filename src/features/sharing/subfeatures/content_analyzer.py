# Placeholder for content_analyzer functions 

import anthropic
import os
import logging
import cv2
import base64
import shutil
from pathlib import Path
from typing import List, Optional, Dict

# Import the shared client
from src.common.claude_client import ClaudeClient 

logger = logging.getLogger('DiscordBot')

# --- Helper Functions (Adapted from utils/youtube_title_generator) ---

def _image_to_base64(image_path: str) -> Optional[str]:
    """Converts an image file to a base64 encoded string."""
    try:
        with open(image_path, 'rb') as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error encoding image {image_path} to base64: {e}", exc_info=True)
        return None

def _get_media_type(file_path: str) -> Optional[str]:
    """Determines the media type based on file extension."""
    ext = Path(file_path).suffix.lower()
    if ext in ['.jpg', '.jpeg']:
        return 'image/jpeg'
    elif ext == '.png':
        return 'image/png'
    elif ext == '.gif':
        return 'image/gif'
    elif ext == '.webp':
        return 'image/webp'
    # Add other image types if needed
    else:
        logger.warning(f"Unsupported image type for base64 encoding: {ext}")
        return None

def _extract_frames(video_path: str, num_frames: int, save_dir: str) -> List[str]:
    """Extracts a specified number of evenly distributed frames from a video."""
    frame_paths = []
    if not os.path.exists(video_path):
        logger.error(f"Video file not found: {video_path}")
        return frame_paths
        
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    vidcap = cv2.VideoCapture(video_path)

    if not vidcap.isOpened():
        logger.error(f"Could not open video file: {video_path}")
        return frame_paths

    total_frames = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames < 1:
        logger.warning(f"Video has no frames or failed to read count: {video_path}")
        vidcap.release()
        return frame_paths

    # Ensure num_frames is not more than total_frames
    num_frames_to_extract = min(num_frames, total_frames)
    if num_frames_to_extract < 1:
        logger.warning(f"Cannot extract less than 1 frame from {video_path}")
        vidcap.release()
        return frame_paths
        
    # Calculate interval, avoid division by zero if only one frame requested
    frames_interval = (total_frames // num_frames_to_extract) if num_frames_to_extract > 1 else 0

    extracted_count = 0
    for i in range(num_frames_to_extract):
        frame_index = i * frames_interval
        # Ensure frame index is within bounds
        if frame_index >= total_frames:
            frame_index = total_frames - 1 
            
        vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        success, image = vidcap.read()

        if success:
            frame_filename = save_path / f"frame_{extracted_count:03d}.jpg"
            try:
                cv2.imwrite(str(frame_filename), image)
                frame_paths.append(str(frame_filename))
                extracted_count += 1
            except Exception as e:
                logger.error(f"Failed to write frame {extracted_count} for video {video_path}: {e}")
        else:
            # If reading fails, maybe try the next frame? For now, just log.
            logger.warning(f"Failed to read frame at index {frame_index} for video {video_path}")
            # Break if we can't even read the first frame requested
            if i == 0 and extracted_count == 0:
                 break

    vidcap.release()
    logger.info(f"Extracted {extracted_count} frames from {video_path} into {save_dir}")
    return frame_paths

# --- Claude Interaction ---

# Modify signature to accept ClaudeClient
async def generate_description_with_claude(
    claude_client: ClaudeClient, 
    original_content: str, 
    attachments: List[Dict], 
    user_name: Optional[str] = "the user"
) -> Optional[str]:
    """Generates a social media post description using Claude 3.5 Sonnet via the shared client."""

    prompt = f"You are generating a social media post caption for a piece of digital art shared by {user_name}. Analyze the attached media (and text, if provided) and create an engaging and concise caption (around 1-3 sentences). Focus on describing the art visually or capturing its mood. Avoid simply restating the user's original text, but incorporate its essence if relevant. Do not use hashtags unless explicitly asked. Keep it positive and suitable for a general audience."

    if original_content:
        prompt += f"\n\nOriginal text from the user (for context, do not just copy it): \"{original_content}\""
    
    prompt += "\n\nOutput ONLY the generated caption text."

    content_blocks = []
    temp_frame_dir = None

    try:
        # Prepare media blocks
        for attachment in attachments[:5]: # Limit attachments to avoid exceeding token limits
            media_path = attachment.get('local_path') # Assume local_path is added earlier
            if not media_path or not os.path.exists(media_path):
                 logger.warning(f"Attachment missing local path or file not found: {attachment.get('filename')}")
                 continue

            media_type = attachment.get('content_type', '').lower()

            # If video, extract frames
            if media_type.startswith('video/'):
                if not temp_frame_dir:
                     # Create a temporary directory for frames for this specific request
                     temp_frame_dir = Path(f"./temp_frames_{os.urandom(4).hex()}")
                     temp_frame_dir.mkdir(exist_ok=True)
                     logger.debug(f"Created temp frame dir: {temp_frame_dir}")

                frame_paths = _extract_frames(video_path=media_path, num_frames=5, save_dir=str(temp_frame_dir))
                
                for frame_path in frame_paths[:5]: # Max 5 frames per video
                     mime_type = _get_media_type(frame_path)
                     base64_data = _image_to_base64(frame_path)
                     if mime_type and base64_data:
                         content_blocks.append({
                             "type": "image",
                             "source": {"type": "base64", "media_type": mime_type, "data": base64_data}
                         })
            # If image, encode directly
            elif media_type.startswith('image/'):
                 mime_type = _get_media_type(media_path) or media_type # Fallback to original content_type
                 base64_data = _image_to_base64(media_path)
                 if mime_type and base64_data:
                     content_blocks.append({
                         "type": "image",
                         "source": {"type": "base64", "media_type": mime_type, "data": base64_data}
                     })
            else:
                 logger.warning(f"Skipping unsupported attachment type: {media_type} for file {attachment.get('filename')}")
        
        # Add the text prompt after all media blocks
        content_blocks.append({"type": "text", "text": prompt}) # Add prompt text last

        if len(content_blocks) == 1: # Only text block was added (no valid media)
             logger.warning("No valid media found to send to Claude. Cannot generate description.")
             return None

        # Use the shared client's generate_text method
        logger.info(f"Sending request to Claude 3.5 Sonnet via shared client with {len(content_blocks) - 1} media blocks.")
        generated_text = await claude_client.generate_text(
            content=content_blocks, # Pass the list of blocks
            model="claude-3-5-sonnet-20240620",
            max_tokens=200, # Keep caption relatively short
            # temperature=0.7, # Client uses default or can be added if needed
        )

        if generated_text:
            logger.info(f"Claude generated description via shared client: {generated_text}")
            return generated_text
        else:
            logger.error(f"Claude call via shared client failed to generate description.")
            return None

    except Exception as e:
        logger.error(f"Error preparing content or calling shared Claude client: {e}", exc_info=True)
        return None
    finally:
         # Clean up temporary frame directory if it was created
         if temp_frame_dir and temp_frame_dir.exists():
             try:
                 shutil.rmtree(temp_frame_dir)
                 logger.debug(f"Removed temp frame dir: {temp_frame_dir}")
             except Exception as e:
                 logger.error(f"Failed to remove temp frame directory {temp_frame_dir}: {e}") 