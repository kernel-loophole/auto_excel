import streamlit as st
import pandas as pd
import json
import os
import re
import yt_dlp
import logging
from moviepy.editor import VideoFileClip
from app.transcriber.trancribe import transcribe_audio_directory

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Set page configuration
st.set_page_config(page_title="YouTube to Audio Converter", layout="wide")

# Title and description
st.title("YouTube to Audio Converter")
st.markdown(
    "Upload an Excel file containing YouTube links to extract, download, and convert to audio."
)


def v2a(input_video_path, output_audio_path):
    """
    Converts a video file to an audio file in WAV format.
    """
    try:
        logger.info(
            f"Starting video to audio conversion: {input_video_path} to {output_audio_path}"
        )
        if not os.path.exists(input_video_path):
            raise FileNotFoundError(f"Video file not found: {input_video_path}")
        videoclip = VideoFileClip(input_video_path)
        audioclip = videoclip.audio
        if audioclip is not None:
            audioclip.write_audiofile(output_audio_path, codec="pcm_s16le")
        else:
            raise ValueError("No audio stream found in the video file.")
        audioclip.close()
        videoclip.close()
        logger.info(f"Audio extracted successfully: {output_audio_path}")
    except Exception as e:
        logger.error(f"An error occurred during video to audio conversion: {e}")
        raise


def extract_youtube_links_from_excel(excel_file):
    """
    Extract YouTube links from an uploaded Excel file.
    """
    try:
        df = pd.read_excel(excel_file)
        video_links = []
        youtube_pattern = (
            r"(https?://(?:www\.)?youtube\.com/watch\?v=[^\s&]+(?:&pp=[^\s]+)?)"
        )
        for _, row in df.iterrows():
            for col in df.columns:
                value = str(row[col])
                matches = re.findall(youtube_pattern, value)
                video_links.extend(matches)
        logger.info(f"Extracted {len(video_links)} YouTube links from Excel file")
        return video_links
    except Exception as e:
        logger.error(f"Error reading Excel file: {str(e)}")
        st.error(f"Error reading Excel file: {str(e)}")
        return []


def save_links_to_json(video_links, json_file_path):
    """
    Save YouTube links to a JSON file.
    """
    try:
        json_data = {"youtube_links": video_links}
        with open(json_file_path, "w") as f:
            json.dump(json_data, f, indent=2)
        logger.info(f"Saved YouTube links to {json_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file {json_file_path}: {str(e)}")
        st.error(f"Error saving JSON file: {str(e)}")
        return False


def download_and_convert_youtube_videos(
    video_links,
    video_dir="downloaded_videos",
    audio_dir="extracted_audio",
    delete_videos_after_conversion=True,
):
    """
    Download YouTube videos and convert them to audio.
    """
    os.makedirs(video_dir, exist_ok=True)
    os.makedirs(audio_dir, exist_ok=True)
    results = {"successful": [], "failed": []}

    # Normalize video_dir path to avoid double directory issues
    video_dir = os.path.normpath(video_dir).replace("\\", "/")

    ydl_opts = {
        "outtmpl": os.path.join(video_dir, "%(title)s.%(ext)s").replace(
            "\\", "/"
        ),  # Fixed path handling
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "ignoreerrors": True,
        "nooverwrites": True,
        "quiet": False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in video_links:
            try:
                logger.info(f"Processing: {link}")
                st.write(f"Processing: {link}")
                logger.info(f"Downloading video: {link}")
                info_dict = ydl.extract_info(link, download=True)
                if not info_dict:
                    raise Exception("Failed to download video")

                # Construct video filename
                video_filename = (
                    ydl.prepare_filename(info_dict)
                    .replace(".webm", ".mp4")
                    .replace(".mkv", ".mp4")
                )
                video_filename = os.path.basename(
                    video_filename
                )  # Extract only the filename
                video_path = os.path.join(video_dir, video_filename).replace("\\", "/")

                if not os.path.exists(video_path):
                    raise FileNotFoundError(
                        f"Video file not found after download: {video_path}"
                    )

                audio_filename = os.path.splitext(video_filename)[0] + ".wav"
                audio_path = os.path.join(audio_dir, audio_filename).replace("\\", "/")
                logger.info(f"Converting to audio: {video_path} -> {audio_path}")
                st.write(f"Converting to audio: {audio_path}")
                v2a(video_path, audio_path)

                if delete_videos_after_conversion and os.path.exists(video_path):
                    os.remove(video_path)
                    logger.info(f"Deleted video file: {video_path}")

                results["successful"].append(link)

            except Exception as e:
                logger.error(f"Failed to process {link}: {str(e)}")
                st.error(f"Failed to process {link}: {str(e)}")
                results["failed"].append({"link": link, "error": str(e)})

    return results


# File uploader
uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])

if uploaded_file is not None:
    try:
        # Step 1: Extract links from Excel
        st.subheader("Extracting YouTube Links")
        video_links = extract_youtube_links_from_excel(uploaded_file)

        if not video_links:
            st.warning("No YouTube links found in the Excel file.")
        else:
            st.write(f"Found {len(video_links)} YouTube links:")
            st.write(video_links)

            # Step 2: Save links to JSON
            json_file_path = "youtube_links.json"
            if save_links_to_json(video_links, json_file_path):
                st.success(f"Saved links to {json_file_path}")

                # Provide download button for JSON file
                with open(json_file_path, "rb") as f:
                    st.download_button(
                        label="Download JSON File",
                        data=f,
                        file_name=json_file_path,
                        mime="application/json",
                    )

                # Step 3: Download and convert videos
                st.subheader("Downloading and Converting Videos")
                results = download_and_convert_youtube_videos(
                    video_links,
                    video_dir="downloaded_videos",
                    audio_dir="extracted_audio",
                    delete_videos_after_conversion=True,
                )
                # Step 4: Transcribe audio files
                st.subheader("Transcribing Audio Files")
                transcribe_audio_directory(
                    directory_path="extracted_audio",
                    output_excel_path="transcription_results.xlsx",
                    language_code="en-US",
                    cleanup_s3=True,
                )
                # Display results
                st.subheader("Processing Summary")
                st.write(f"Successfully processed: {len(results['successful'])} videos")
                st.write(f"Failed: {len(results['failed'])} videos")
                if results["failed"]:
                    st.write("Failed links:")
                    for fail in results["failed"]:
                        st.write(f"- {fail['link']}: {fail['error']}")

    except Exception as e:
        st.error(f"Error during processing: {str(e)}")
else:
    st.info("Please upload an Excel file to begin.")

# Instructions
st.markdown(
    """
### Instructions
1. Upload an Excel file containing YouTube links.
2. The app will extract the links, save them to a JSON file, download the videos, and convert them to WAV audio.
3. Download the generated JSON file and check the `extracted_audio` folder for WAV files.
4. Videos are deleted after conversion to save space.
"""
)
