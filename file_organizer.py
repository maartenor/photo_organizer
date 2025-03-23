import os
import sys
import platform
import shutil
import sqlite3
import datetime
import argparse
from PIL import Image
from PIL.ExifTags import TAGS
import logging
import re
from pathlib import Path
import mimetypes
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Define error and warning codes
class ErrorCodes:
    UNPROCESSABLE_FILE = 100
    MISSING_DATE = 200
    MOVE_ERROR = 300
    DATABASE_ERROR = 400

class WarningCodes:
    NO_DATE_METADATA = 10
    UNSUPPORTED_FILE = 20
    FILENAME_DATE_EXTRACTION = 30

def setup_database(db_path):
    """Set up the SQLite database for logging file operations."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create process_log table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS process_log (
            filename TEXT,
            target_folder TEXT,
            processing_timestamp_utc TEXT
        )
        ''')
        
        # Create issues table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS issues (
            filename TEXT,
            warning_code INTEGER,
            error_code INTEGER,
            issue_description TEXT,
            processing_timestamp_utc TEXT
        )
        ''')
        
        conn.commit()
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)

def log_process(conn, filename, target_folder):
    """Log successful file processing to the database."""
    try:
        cursor = conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()
        cursor.execute(
            "INSERT INTO process_log (filename, target_folder, processing_timestamp_utc) VALUES (?, ?, ?)",
            (filename, target_folder, timestamp)
        )
        conn.commit()
    except sqlite3.Error as e:
        log_issue(conn, filename, None, ErrorCodes.DATABASE_ERROR, f"Failed to log process: {e}")

def log_issue(conn, filename, warning_code=None, error_code=None, description=None):
    """Log an issue (warning or error) to the database."""
    try:
        cursor = conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()
        cursor.execute(
            "INSERT INTO issues (filename, warning_code, error_code, issue_description, processing_timestamp_utc) VALUES (?, ?, ?, ?, ?)",
            (filename, warning_code, error_code, description, timestamp)
        )
        conn.commit()
        
        # Also log to console
        if warning_code:
            logger.warning(f"Warning {warning_code}: {description} - File: {filename}")
        if error_code:
            logger.error(f"Error {error_code}: {description} - File: {filename}")
    except sqlite3.Error as e:
        logger.error(f"Database error while logging issue: {e}")

def get_image_date_taken(file_path):
    """Extract the 'Date Taken' from image metadata."""
    try:
        with Image.open(file_path) as img:
            exif_data = img._getexif()
            if not exif_data:
                return None
                
            for tag_id, value in exif_data.items():
                tag = TAGS.get(tag_id, tag_id)
                if tag == 'DateTimeOriginal':
                    # Format: 'YYYY:MM:DD HH:MM:SS'
                    date_pattern = r'(\d{4}):(\d{2}):\d{2}'
                    match = re.search(date_pattern, value)
                    if match:
                        year, month = match.groups()
                        return (year, month)
    except Exception as e:
        return None
    
    return None

def get_video_date_created(file_path):
    """Extract the 'Media Created' date from video metadata with expanded support."""
    try:
        # First try with hachoir
        parser = createParser(file_path)
        if parser:
            metadata = extractMetadata(parser)
            if metadata:
                # Try different metadata fields that might contain creation date
                for key in ['creation_date', 'datetime_original', 'creation_datetime']:
                    if hasattr(metadata, key):
                        date = getattr(metadata, key).value
                        year = date.year
                        month = f"{date.month:02d}"
                        parser.close()
                        return (str(year), month)
                parser.close()
        
        # If hachoir fails, try with ffmpeg/ffprobe if available
        try:
            import subprocess
            cmd = [
                'ffprobe', 
                '-v', 'quiet', 
                '-print_format', 'json', 
                '-show_format', 
                '-show_streams', 
                file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                
                # Try to find date in tags
                for section in ['format', 'streams']:
                    if section in data:
                        if isinstance(data[section], list):
                            items = data[section]
                        else:
                            items = [data[section]]
                            
                        for item in items:
                            if 'tags' in item:
                                tags = item['tags']
                                for date_tag in ['creation_time', 'date', 'DateTimeOriginal']:
                                    if date_tag in tags:
                                        date_str = tags[date_tag]
                                        # Try to parse date string in various formats
                                        for fmt in [
                                            '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format
                                            '%Y-%m-%d %H:%M:%S',      # Standard format
                                            '%Y:%m:%d %H:%M:%S',      # EXIF format
                                        ]:
                                            try:
                                                date = datetime.datetime.strptime(date_str, fmt)
                                                return (str(date.year), f"{date.month:02d}")
                                            except ValueError:
                                                continue
        except (ImportError, FileNotFoundError, subprocess.SubprocessError, json.JSONDecodeError) as e:
            logger.debug(f"FFprobe extraction failed: {e}")
            
        # Fall back to file stats as last resort
        if os.path.exists(file_path):
            stat = os.stat(file_path)
            # Try creation time first, then modification time
            for timestamp in [stat.st_ctime, stat.st_mtime]:
                date = datetime.datetime.fromtimestamp(timestamp)
                # Only use file stats if they seem reasonable (not default/epoch dates)
                if date.year > 1980:  # Arbitrary cutoff for reasonable file dates
                    return (str(date.year), f"{date.month:02d}")
                    
    except Exception as e:
        logger.debug(f"Error extracting video date: {e}")
        
    return None

def extract_timestamp_from_filename(filename):
    """
    Try to extract a timestamp from the filename.
    Returns (year, month) tuple if successful, None otherwise.
    """
    # Common patterns for dates in filenames
    patterns = [
        r'(\d{4})[-_](\d{2})[-_]\d{2}',  # YYYY-MM-DD or YYYY_MM_DD
        r'\d{2}[-_](\d{2})[-_](\d{4})',  # DD-MM-YYYY or DD_MM_YYYY
        r'(\d{4})(\d{2})\d{2}',          # YYYYMMDD
        r'IMG[-_](\d{4})(\d{2})\d{2}',   # IMG-YYYYMMDD or IMG_YYYYMMDD
        r'VID[-_](\d{4})(\d{2})\d{2}'    # VID-YYYYMMDD or VID_YYYYMMDD
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            year, month = match.groups()
            
            # Validate year (must be in the past)
            current_year = datetime.datetime.now().year
            current_month = datetime.datetime.now().month
            year = int(year)
            month = int(month)
            
            if (year < current_year) or (year == current_year and month <= current_month):
                # Ensure month is formatted as two digits
                return (str(year), f"{month:02d}")
            
    return None

def is_network_path(path):
    """Check if a path is a network path (UNC path on Windows)."""
    if platform.system() == "Windows":
        return path.startswith('\\\\')
    return False

def is_image_file(file_path):
    """Check if the file is an image based on its mimetype."""
    mime = mimetypes.guess_type(file_path)[0]
    return mime and mime.startswith('image/')

def is_video_file(file_path):
    """Check if the file is a video based on its mimetype."""
    mime = mimetypes.guess_type(file_path)[0]
    return mime and mime.startswith('video/')

def process_file(file_path, target_root, to_sort_folder, unprocessable_folder, conn):
    """Process a single file and move it to the appropriate location."""
    file_name = os.path.basename(file_path)
    
    try:
        if is_image_file(file_path):
            date_info = get_image_date_taken(file_path)
            file_type = "image"
        elif is_video_file(file_path):
            date_info = get_video_date_created(file_path)
            file_type = "video"
        else:
            # Not an image or video file
            target_path = os.path.join(unprocessable_folder, file_name)
            shutil.move(file_path, target_path)
            log_process(conn, file_name, target_path)
            log_issue(conn, file_name, WarningCodes.UNSUPPORTED_FILE, None, 
                      f"File is neither image nor video: {file_name}")
            return
        
        if date_info:
            year, month = date_info
            year_month_folder = os.path.join(target_root, year, month)
            
            # Create year/month folder if it doesn't exist
            if not os.path.exists(year_month_folder):
                os.makedirs(year_month_folder)
                
            target_path = os.path.join(year_month_folder, file_name)
        else:
            # No date metadata found
            target_path = os.path.join(to_sort_folder, file_name)
            log_issue(conn, file_name, WarningCodes.NO_DATE_METADATA, None, 
                      f"No date metadata found for {file_type} file: {file_name}")
        
        # Move the file
        shutil.move(file_path, target_path)
        log_process(conn, file_name, target_path)
        
    except Exception as e:
        # Handle any other errors
        try:
            # Try to move to unprocessable folder
            target_path = os.path.join(unprocessable_folder, file_name)
            shutil.move(file_path, target_path)
            log_process(conn, file_name, target_path)
        except Exception as move_error:
            log_issue(conn, file_name, None, ErrorCodes.MOVE_ERROR, 
                      f"Failed to move file to unprocessable folder: {move_error}")
        
        log_issue(conn, file_name, None, ErrorCodes.UNPROCESSABLE_FILE, 
                  f"Error processing file: {e}")

def process_unsorted_files(to_sort_folder, target_root, conn):
    """Process files in the 'to_sort' folder by extracting dates from filenames."""
    if not os.path.exists(to_sort_folder):
        logger.warning(f"'to_sort' folder does not exist: {to_sort_folder}")
        return
        
    for filename in os.listdir(to_sort_folder):
        file_path = os.path.join(to_sort_folder, filename)
        if not os.path.isfile(file_path):
            continue
            
        date_info = extract_timestamp_from_filename(filename)
        
        if date_info:
            year, month = date_info
            year_month_folder = os.path.join(target_root, year, month)
            
            # Create year/month folder if it doesn't exist
            if not os.path.exists(year_month_folder):
                os.makedirs(year_month_folder)
                
            target_path = os.path.join(year_month_folder, filename)
            
            try:
                # Move the file
                shutil.move(file_path, target_path)
                
                # Log the process
                log_process(conn, filename, target_path)
                
                # Add warning about extracted timestamp
                log_issue(conn, filename, WarningCodes.NO_DATE_METADATA, None, 
                         f"Moved based on filename timestamp: {year}-{month}")
                
                logger.info(f"Moved '{filename}' to {year_month_folder} based on filename timestamp")
            except Exception as e:
                log_issue(conn, filename, None, ErrorCodes.MOVE_ERROR, 
                         f"Failed to move file from to_sort folder: {e}")
        else:
            # Keep in to_sort folder if no date could be extracted
            logger.info(f"Couldn't extract date from filename: {filename}")

def main():
    parser = argparse.ArgumentParser(description='Organize files based on date metadata.')
    parser.add_argument('--source', '-s', required=True, help='Source directory containing files to process')
    parser.add_argument('--target', '-t', required=True, help='Target directory for organized files')
    args = parser.parse_args()
    
    source_dir = os.path.normpath(args.source)
    target_dir = os.path.normpath(args.target)
    
    # Check if (network) source_dir exists
    if not os.path.exists(source_dir):
        logger.error(f"Source directory does not exist or is not accessible: {source_dir}")
        sys.exit(1)
        
    # Check (network) target path is writable by attempting to create it
    try:
        os.makedirs(target_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"Cannot access or create target directory: {target_dir}. Error: {e}")
        sys.exit(1)

    # Create necessary folders
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, 'database')
    to_sort_folder = os.path.join(target_dir, 'to_sort')
    unprocessable_folder = os.path.join(target_dir, 'unprocessable')
    
    for folder in [db_dir, target_dir, to_sort_folder, unprocessable_folder]:
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    # Set up database
    db_path = os.path.join(db_dir, 'file_organizer.db')
    conn = setup_database(db_path)
    
    # Initialize mime types
    mimetypes.init()
    
    try:
        # Process all files in the source directory
        for root, _, files in os.walk(source_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                process_file(file_path, target_dir, to_sort_folder, unprocessable_folder, conn)
        logger.info("Main file organization completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    try:
        # Add this before conn.close() in the main function
        logger.info("Processing files in 'to_sort' folder...")
        process_unsorted_files(to_sort_folder, target_dir, conn)
        logger.info("Processing files in 'to_sort' folder completed successfully.")
    except Exception as e:
        logger.error(f"Error processing files in 'to_sort' folder: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()