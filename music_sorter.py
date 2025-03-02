import os
import shutil
import time
import logging
from watchdog.observers import Observer
from PIL import Image
from watchdog.events import FileSystemEventHandler
from mutagen import File
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen.id3 import APIC

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

class MusicFileHandler(FileSystemEventHandler):
    def find_cover_art(self, directory):
        """Find and read cover art from jpg/png files in directory"""
        for file in os.listdir(directory):
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                image_path = os.path.join(directory, file)
                try:
                    # Open and verify the image
                    with Image.open(image_path) as img:
                        # Convert to bytes
                        img_format = img.format
                        if img_format not in ('JPEG', 'PNG'):
                            continue
                        
                        # Convert to bytes
                        if img_format == 'PNG':
                            mime_type = 'image/png'
                        else:
                            mime_type = 'image/jpeg'
                            
                        with open(image_path, 'rb') as f:
                            return f.read(), mime_type
                except Exception as e:
                    print(f"Error reading cover art {image_path}: {e}")
        
        return None, None

    def __init__(self):
        super().__init__()
        self.directory_state = {}  # Track state of each directory
        self.last_file_time = {}  # Track last file addition time per directory

    def is_file_locked(self, filepath, timeout=60, check_interval=1):
        """Check if a file is locked by attempting to open it in write mode"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to open the file in write mode (append mode)
                with open(filepath, 'a+b') as _:
                    # If we can open it, the file is not locked
                    return False
            except (IOError, PermissionError) as e:
                logging.debug(f"File {filepath} is currently locked: {e}")
                # Wait before trying again
                time.sleep(check_interval)
                continue
        
        logging.warning(f"Timeout waiting for file {filepath} to be unlocked")
        return True

    def init_directory_state(self, directory):
        """Initialize or update directory state"""
        if directory not in self.directory_state:
            self.directory_state[directory] = {
                'pending_files': set(),  # Files waiting to be processed
                'processed_files': set(),  # Successfully processed files
                'failed_files': set(),  # Failed to process files
                'stable_since': None  # Timestamp when last file was added
            }
            self.last_file_time[directory] = time.time()

    def on_created(self, event):
        if event.is_directory:
            return

        filepath = event.src_path
        if filepath.endswith(('.mp3', '.flac', '.ogg', '.m4a')):
            directory = os.path.dirname(filepath)
            self.init_directory_state(directory)
            
            # Update directory state
            self.directory_state[directory]['pending_files'].add(filepath)
            self.last_file_time[directory] = time.time()
            
            # Schedule processing check
            self.check_directory_readiness(directory)

    def check_directory_readiness(self, directory):
        """Check if directory is ready for processing"""
        state = self.directory_state[directory]
        current_time = time.time()
        
        # Wait for directory to stabilize (no new files for 2 seconds)
        if current_time - self.last_file_time[directory] < 2:
            return
        
        # Check if all files are unlocked
        locked_files = []
        for filepath in state['pending_files']:
            if os.path.exists(filepath) and self.is_file_locked(filepath):
                locked_files.append(filepath)
        
        if not locked_files:
            # All files are unlocked, find cover art before processing
            cover_data = None
            mime_type = None
            try:
                cover_data, mime_type = self.find_cover_art(directory)
            except Exception as e:
                logging.error(f"Error finding cover art: {e}")

            # Process directory with cover art if found
            self.process_directory(directory, cover_data, mime_type)
        else:
            logging.info(f"Directory {directory} has {len(locked_files)} locked files, waiting...")

    def process_directory(self, directory, cover_data=None, mime_type=None):
        """Process all files in a directory"""
        state = self.directory_state[directory]
        pending_files = state['pending_files'].copy()

        for filepath in pending_files:
            if not os.path.exists(filepath):
                # File might have been moved by another process
                state['pending_files'].remove(filepath)
                continue

            # Apply cover art before moving the file
            if cover_data and mime_type:
                try:
                    # Handle MP3 files differently for cover art
                    if filepath.lower().endswith('.mp3'):
                        audio = MP3(filepath)
                        # Ensure ID3 tags exist
                        if not audio.tags:
                            audio.add_tags()
                    else:
                        audio = File(filepath)
                    
                    # Process cover art
                    if audio is not None and (hasattr(audio, 'add_picture') or hasattr(audio, 'tags')):
                        self.add_cover_art(audio, cover_data, mime_type)
                        audio.save()
                        print(f"Successfully applied cover art to {filepath}")
                except Exception as e:
                    print(f"Error adding cover art to {filepath}: {e}")

            success = self.process_music_file(filepath)
            if success:
                state['pending_files'].remove(filepath)
                state['processed_files'].add(filepath)
            else:
                state['pending_files'].remove(filepath)
                state['failed_files'].add(filepath)
                self.move_to_unknown(filepath)

        # Handle cleanup only after all files are processed and cover art is applied
        if not state['pending_files'] and directory in self.directory_state:
            self.handle_remaining_files(directory)
            del self.directory_state[directory]
            del self.last_file_time[directory]

    def has_cover_art(self, audio):
        """Check if audio file already has cover art"""
        try:
            # FLAC files
            if hasattr(audio, 'pictures'):
                for pic in audio.pictures:
                    if pic.type == 3:  # Cover (front)
                        return True
                return False
                
            # MP3 files
            elif hasattr(audio, 'tags') and audio.tags:
                # Check for APIC frame in ID3 tags
                if hasattr(audio.tags, 'getall'):
                    return bool(audio.tags.getall('APIC'))
                    
                # Alternative check for other tag formats
                for tag in audio.tags.values():
                    if 'APIC' in str(tag) or 'PICTURE' in str(tag):
                        return True
                        
            return False
            
        except Exception as e:
            print(f"Error checking cover art in {audio.filename}: {e}")
            return False

    def add_cover_art(self, audio, image_data, mime_type):
        """Add cover art to audio file based on format"""
        try:
            # Skip if cover art already exists
            if self.has_cover_art(audio):
                print(f"Cover art already exists in {audio.filename}")
                return

            # FLAC files
            if hasattr(audio, 'add_picture'):
                from mutagen.flac import Picture
                pic = Picture()
                pic.data = image_data
                pic.type = 3  # Cover (front)
                pic.mime = mime_type
                audio.add_picture(pic)
                print(f"Added cover art to FLAC file: {audio.filename}")

            # MP3 files
            elif hasattr(audio, 'tags') and audio.tags:
                if hasattr(audio.tags, 'add'):
                    audio.tags.add(APIC(encoding=3, mime=mime_type,
                                      type=3, desc='Cover', data=image_data))
                    print(f"Added cover art to MP3 file: {audio.filename}")

        except Exception as e:
            print(f"Error adding cover art: {e}")

    def move_to_unknown(self, filepath):
        """Move a file to the unknown folder structure"""
        source_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        
        # Create destination path in Unknown folder with same structure
        rel_path = os.path.relpath(source_dir, 'watch')
        dest_dir = os.path.join('unknown', rel_path)
        os.makedirs(dest_dir, exist_ok=True)
        
        # Move file to Unknown folder structure
        try:
            shutil.move(filepath, os.path.join(dest_dir, filename))
            print(f"Moved file to Unknown folder: {filename}")
        except Exception as e:
            print(f"Error moving file to unknown folder: {e}")

    def process_music_file(self, filepath):
        """Process a music file. Returns True if successful, False otherwise."""
        print(f"Processing file: {filepath}")  # Debugging
        
        def get_metadata_field(audio, field, default=None):
            """Extract metadata field with consistent handling"""
            if field not in audio:
                print(f"Missing {field} tag in {filepath}")
                return None
            try:
                value = audio[field][0]
                
                # For track numbers, handle the "5/12" format
                if field == 'tracknumber':
                    value = str(value).split('/')[0]
                    value = int(value)  # Ensure it's a valid number
                return value
            except (IndexError, ValueError) as e:
                print(f"Invalid {field} tag in {filepath}: {e}")
                return None

        try:
            # Use EasyID3 for MP3 files
            if filepath.lower().endswith('.mp3'):
                try:
                    audio = EasyID3(filepath)
                except:
                    # If no ID3 tags exist, create them
                    mp3 = MP3(filepath)
                    mp3.add_tags()
                    mp3.save()
                    audio = EasyID3(filepath)
            else:
                # For non-MP3 files, use regular File
                audio = File(filepath)
                if audio is None:
                    print(f"Could not read tags from {filepath}. Skipping.")
                    return False

            # Extract required metadata
            artist = get_metadata_field(audio, 'artist')
            album = get_metadata_field(audio, 'album')
            title = get_metadata_field(audio, 'title')
            track_num = get_metadata_field(audio, 'tracknumber')
            year = get_metadata_field(audio, 'date')

            # Validate all required fields are present
            if not all([artist, album, title, track_num, year]):
                print(f"Missing required tags in {filepath}. Moving to unknown folder.")
                return False

            # Extract year from date (e.g. "2023" from "2023-01-01")
            year = str(year)[:4]

            # Create destination paths with sanitized names
            artist = self.sanitize_filename(str(artist))
            album = self.sanitize_filename(str(album))
            title = self.sanitize_filename(str(title))

            # Create destination directory
            album_folder = f"{album} ({year})"
            destination_dir = os.path.join('sorted', artist, album_folder)
            os.makedirs(destination_dir, exist_ok=True)

            # Rename file, get the extension from original file
            file_extension = os.path.splitext(filepath)[1][1:] # remove leading dot
            new_filename = f"{track_num:02d} - {title}.{file_extension}"
            new_filepath = os.path.join(destination_dir, new_filename)

            # Add lyrics if available
            lrc_filepath = os.path.splitext(filepath)[0] + '.lrc'
            if os.path.exists(lrc_filepath):
                try:
                    with open(lrc_filepath, 'r', encoding='utf-8') as lrc_file:
                        lyrics = lrc_file.read()
                    
                    if filepath.lower().endswith('.mp3'):
                        # For MP3 files, need to use regular MP3 object for lyrics
                        mp3 = MP3(filepath)
                        from mutagen.id3 import USLT
                        mp3.tags.add(USLT(encoding=3, lang='eng', desc='', text=lyrics))
                        mp3.save()
                    else:
                        audio['lyrics'] = lyrics
                        audio.save()
                    print(f"Added lyrics from {lrc_filepath} to {filepath}")
                    # Delete the source lyrics file after successful embedding
                    os.remove(lrc_filepath)
                    print(f"Deleted lyrics file: {lrc_filepath}")
                except Exception as e:
                    print(f"Error adding lyrics from {lrc_filepath} to {filepath}: {e}")

            # Move file
            shutil.move(filepath, new_filepath)
            print(f"Moved {filepath} to {new_filepath}")

            return True

        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return False

    def sanitize_filename(self, filename):
        # Remove invalid characters for file names
        return "".join(c for c in filename if c.isalnum() or c in ['.', '_', ' ']).rstrip()
    
    def remove_empty_dirs(self, path, stop_at=None):
        """Recursively remove empty directories, return True if directory was removed"""
        if not os.path.isdir(path):
            return False
        
        # Remove empty subdirectories
        for subdir in os.listdir(path):
            full_path = os.path.join(path, subdir)
            if os.path.isdir(full_path) and not os.path.islink(full_path):
                self.remove_empty_dirs(full_path, stop_at)
                
        # Don't remove the stop_at directory
        if stop_at and os.path.samefile(path, stop_at):
            return False
            
        # Try to remove if empty
        try:
            os.rmdir(path)
            print(f"Removed empty directory: {path}")
            
            # Try to remove parent if it's empty (unless it's the stop_at directory)
            parent = os.path.dirname(path)
            if parent and (not stop_at or not os.path.samefile(parent, stop_at)):
                self.remove_empty_dirs(parent, stop_at)
                
            return True
        except OSError:  # Directory not empty
            return False

    
    def handle_remaining_files(self, processed_dir):
        """Move non-music/non-cover files to Unknown folder and clean up empty directories"""
        music_extensions = ('.mp3', '.flac', '.ogg', '.m4a')
        image_extensions = ('.jpg', '.jpeg', '.png')
        # Handle remaining files: delete cover art, move others to unknown
        
        for root, dirs, files in os.walk(processed_dir):
            if files:  # If there are any files in this directory
                for file in files:
                    # Skip music files as they should have been processed already
                    if file.endswith(music_extensions):
                        continue
                    
                    # Delete cover art files after processing
                    if file.endswith(image_extensions):
                        os.remove(os.path.join(root, file))
                        print(f"Removed cover art file: {file}")
                        continue

                    source_file = os.path.join(root, file)
                    rel_path = os.path.relpath(os.path.dirname(source_file), 'watch')
                    dest_dir = os.path.join('unknown', rel_path)
                    os.makedirs(dest_dir, exist_ok=True)
                    
                    # Move file to Unknown folder structure
                    shutil.move(source_file, os.path.join(dest_dir, file))
                    print(f"Moved unprocessed file to Unknown folder: {file}")
        
        # After moving files, clean up this processed directory if empty
        try:
            # This will remove the directory and its parent directories if they're empty
            watch_path = os.path.abspath('watch')
            self.remove_empty_dirs(processed_dir, stop_at=watch_path)
        except Exception as e:
            print(f"Error removing directory {processed_dir}: {e}")

if __name__ == "__main__":
    watch_folder = 'watch'  # Replace with your watch folder
    if not os.path.exists(watch_folder):
        os.makedirs(watch_folder)
    
    sorted_folder = 'sorted' # The folder where sorted music will go
    if not os.path.exists(sorted_folder):
        os.makedirs(sorted_folder)

    unknown_folder = 'unknown' # The folder for unprocessed files
    if not os.path.exists(unknown_folder):
        os.makedirs(unknown_folder)

    event_handler = MusicFileHandler()
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=True)
    observer.start()

    try:
        while True:
            # Check all directories periodically
            for directory in list(event_handler.directory_state.keys()):
                event_handler.check_directory_readiness(directory)
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
