import logging
import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError


load_dotenv()
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")

s3 = boto3.client("s3")

BUCKET = "voxbee"
logger = logging.getLogger(__name__)


def upload_user_file(file_name, user_id, feature_name, project_id, object_name):
    """Upload a file to an S3 bucket in a user-specific folder structure.

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param user_id: User ID to create folder with
    :param feature_name: Feature name for categorization
    :param project_id: Project ID for further organization
    :param object_name: S3 object name. If not specified then file_name is used
    :return: Tuple (Boolean success status, String public URL or None)
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Create the full path with user_id, feature_name, and project_id as folders
    object_key = f"user_{user_id}/{feature_name}/{project_id}/{object_name}"

    # Determine content type based on file extension
    content_type = "application/octet-stream"  # default
    try:
        if file_name.lower().endswith((".mp3", ".mpeg")):
            content_type = "audio/mpeg"
        elif file_name.lower().endswith(".wav"):
            content_type = "audio/wav"
        elif file_name.lower().endswith(".txt"):
            content_type = "text/plain"
        elif file_name.lower().endswith(".html"):
            content_type = "text/html; charset=utf-8"
        elif file_name.lower().endswith(".mp4"):
            content_type = "video/mp4"
        elif file_name.lower().endswith(".mov"):
            content_type = "video/quicktime"
    except Exception as e:
        logging.error(f"Error determining content type: {e}")
        return False, None
    try:
        # Upload file with public-read ACL
        s3.upload_file(
            file_name,
            BUCKET,
            object_key,
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": content_type,
                "ContentDisposition": "inline",
            },
        )

        # Generate the public URL
        public_url = f"https://{BUCKET}.s3.amazonaws.com/{object_key}"
        return True, public_url.replace("\\", "/")

    except ClientError as e:
        logging.error(f"Upload failed: {e}")
        return False, None


def download_user_file(public_url, download_path, bucket=BUCKET):
    """Download a file from an S3 bucket using its public URL.

    :param public_url: Public URL of the file to download
    :param download_path: Local path where to save the file
    :param bucket: Bucket to download from
    :return: Boolean indicating success or failure
    """
    try:
        # Extract the object key from the public URL
        # Example URL: https://voxbee.s3.amazonaws.com/user_id/feature/project/file.wav
        object_key = public_url.split(f"{bucket}.s3.amazonaws.com/")[1]

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        logger.info(f"Downloading file from bucket: {bucket}")
        logger.info(f"Object key: {object_key}")
        logger.info(f"Download path: {download_path}")

        # Download the file
        s3.download_file(bucket, object_key, download_path)

        # Verify download
        if os.path.exists(download_path):
            file_size = os.path.getsize(download_path)
            logger.info(f"Download successful. File size: {file_size} bytes")
            return True

        logger.error("File was not created")
        return False

    except ClientError as e:
        logger.error(f"Download failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return False


def delete_user_file(public_url, bucket=BUCKET):
    """Delete a file from an S3 bucket using its public URL.

    :param public_url: Public URL of the file to delete
    :param bucket: Bucket to delete from
    :return: Boolean indicating success or failure
    """
    try:
        # Extract the object key from the public URL
        object_key = public_url.split(f"{bucket}.s3.amazonaws.com/")[1]

        # Delete the file
        s3.delete_object(Bucket=bucket, Key=object_key)

        return True
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        return False
