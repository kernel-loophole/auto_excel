import os
import re
import shutil
import time
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import json
import urllib.request
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

region_name = "eu-north-1"


def transcribe_file(job_name, file_uri, language_code="en-US"):
    """
    Transcribe a single audio file using AWS Transcribe.

    Args:
        job_name: Unique name for the transcription job
        file_uri: S3 URI of the audio file
        language_code: Language code for transcription

    Returns:
        Transcription result text
    """
    transcribe_client = boto3.client("transcribe", region_name=region_name)

    # Get file format from URI
    media_format = file_uri.split(".")[-1].lower()

    # Start transcription job
    transcribe_client.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": file_uri},
        MediaFormat=media_format,
        LanguageCode=language_code,
    )

    logger.info(f"Started transcription job: {job_name}")

    # Wait for job completion
    max_tries = 60
    while max_tries > 0:
        max_tries -= 1
        job = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        job_status = job["TranscriptionJob"]["TranscriptionJobStatus"]

        if job_status in ["COMPLETED", "FAILED"]:
            logger.info(f"Job {job_name} is {job_status}.")

            if job_status == "COMPLETED":
                transcript_uri = job["TranscriptionJob"]["Transcript"][
                    "TranscriptFileUri"
                ]
                logger.info(f"Transcript available at: {transcript_uri}")

                # Download the transcript
                download_path = f"transcription_{job_name}.json"
                urllib.request.urlretrieve(transcript_uri, download_path)

                # Read the transcript
                with open(download_path, "r", encoding="utf-8") as file:
                    transcript_data = json.load(file)
                    transcript_text = transcript_data["results"]["transcripts"][0][
                        "transcript"
                    ]

                # Clean up the downloaded file
                os.remove(download_path)

                return transcript_text
            else:
                failure_reason = job["TranscriptionJob"].get(
                    "FailureReason", "Unknown error"
                )
                logger.error(f"Transcription job {job_name} failed: {failure_reason}")
                raise Exception(f"Transcription failed: {failure_reason}")
        else:
            logger.info(f"Waiting for {job_name}. Current status is {job_status}.")

        time.sleep(10)

    raise TimeoutError(f"Transcription job {job_name} timed out")


def upload_to_s3(file_path, bucket_name, s3_key):
    """
    Upload a file to S3 bucket.

    Args:
        file_path: Path to the local file
        bucket_name: S3 bucket name
        s3_key: S3 object key

    Returns:
        S3 URI of the uploaded file
    """
    s3_client = boto3.client("s3", region_name=region_name)
    s3_client.upload_file(file_path, bucket_name, s3_key)
    return f"s3://{bucket_name}/{s3_key}"


def transcribe_audio_directory(
    directory_path: str,
    output_excel_path: str,
    language_code: str = "en-US",
    cleanup_s3: bool = True,
) -> str:
    """
    Transcribe all audio files in a directory and save results to an Excel file.

    Args:
        directory_path: Path to directory containing audio files (mp3, wav)
        output_excel_path: Path where the Excel file will be saved
        language_code: Language code for transcription (default: en-US)
        cleanup_s3: Whether to delete files from S3 after transcription (default: True)

    Returns:
        Path to the created Excel file
    """
    # Get S3 bucket name from environment
    bucket_name = os.getenv("S3_BUCKET_NAME", "voxbee")

    # Get audio files
    directory = Path(directory_path)
    if not directory.exists():
        raise FileNotFoundError(f"Directory {directory_path} does not exist")

    audio_extensions = {".mp3", ".wav", ".MP3", ".WAV"}
    audio_files = []

    for file_path in directory.iterdir():
        if file_path.is_file() and file_path.suffix in audio_extensions:
            # Edit filename to satisfy regular expression pattern: ^[0-9a-zA-Z._-]+
            new_filename = re.sub(r"[^0-9a-zA-Z._-]", "_", file_path.name)
            new_file_path = file_path.with_name(new_filename)
            shutil.move(file_path, new_file_path)
            audio_files.append(new_file_path)

    logger.info(f"Found {len(audio_files)} audio files in {directory_path}")

    if not audio_files:
        raise ValueError(f"No audio files (mp3, wav) found in {directory_path}")

    results = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_client = boto3.client("s3", region_name=region_name)

    for i, audio_file in enumerate(audio_files, 1):
        logger.info(f"Processing file {i}/{len(audio_files)}: {audio_file.name}")

        try:
            # Generate unique names
            s3_key = f"transcribe_temp/{timestamp}/{audio_file.name}"
            job_name = f"transcribe_job_{timestamp}_{i}_{audio_file.stem}"[:64]

            # Upload to S3
            file_uri = upload_to_s3(str(audio_file), bucket_name, s3_key)

            # Transcribe the file
            transcription_text = transcribe_file(job_name, file_uri, language_code)

            # Store results
            results.append(
                {
                    "Filename": audio_file.name,
                    "File_Path": str(audio_file),
                    "Transcription": transcription_text,
                    "Status": "Completed",
                    "Language_Code": language_code,
                    "Completion_Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

            # Cleanup S3 file if requested
            if cleanup_s3:
                s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
                logger.info(f"Cleaned up S3 file: {s3_key}")

        except Exception as e:
            logger.error(f"Failed to process {audio_file.name}: {str(e)}")
            results.append(
                {
                    "Filename": audio_file.name,
                    "File_Path": str(audio_file),
                    "Transcription": f"Error: {str(e)}",
                    "Status": "Failed",
                    "Language_Code": language_code,
                    "Completion_Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    # Create DataFrame and save to Excel
    df = pd.DataFrame(results)

    # Ensure output directory exists
    output_path = Path(output_excel_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save to Excel with formatting
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Transcriptions", index=False)

        # Get the workbook and worksheet
        worksheet = writer.sheets["Transcriptions"]

        # Adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter

            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except Exception as e:
                    logger.error(f"Error adjusting column width: {str(e)}")

            adjusted_width = min(max_length + 2, 50)  # Max width of 50
            worksheet.column_dimensions[column_letter].width = adjusted_width

    logger.info(f"Transcription results saved to: {output_path}")
    logger.info(
        f"Processed {len(audio_files)} files. {len([r for r in results if r['Status'] == 'Completed'])} successful, {len([r for r in results if r['Status'] == 'Failed'])} failed."
    )

    return str(output_path)


def main():
    """
    Example usage of the transcribe_audio_directory function.
    """
    # Directory containing audio files
    audio_directory = "extracted_audio"

    # Output Excel file path
    output_excel = "transcriptions_output.xlsx"

    try:
        result_path = transcribe_audio_directory(
            directory_path=audio_directory,
            output_excel_path=output_excel,
            language_code="en-US",
            cleanup_s3=True,
        )

        print(f"Transcription completed! Results saved to: {result_path}")

    except Exception as e:
        print(f"Error during transcription: {str(e)}")


if __name__ == "__main__":
    main()
