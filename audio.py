from moviepy import VideoFileClip
import logging

logger = logging.getLogger(__name__)


def v2a(input_video_path, output_audio_path):
    """
    Converts a video file to an audio file in WAV format.

    Args:
        input_video_path (str): Path to the input video file.
        output_audio_path (str): Path to save the extracted audio file (WAV format).

    Returns:
        None
    """
    try:
        logger.info(f"Starting video to audio conversion: {input_video_path} to {output_audio_path}")
        videoclip = VideoFileClip(input_video_path)

        # Extract audio
        audioclip = videoclip.audio

        if audioclip is not None:
            # Write audio as WAV file
            audioclip.write_audiofile(output_audio_path, codec="pcm_s16le")
        else:
            raise ValueError("No audio stream found in the video file.")

        # Close resources
        audioclip.close()
        videoclip.close()

        logger.info(f"Audio extracted successfully: {output_audio_path}")
    except Exception as e:
        logger.error(
            f"An error occurred during video to audio conversion: {e}")