import os
import shutil
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from mutagen import File

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

class MusicFileHandler(FileSystemEventHandler):
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
            # All files are unlocked, process the directory
            self.process_directory(directory)
        else:
            logging.info(f"Directory {directory} has {len(locked_files)} locked files, waiting...")

    def process_directory(self, directory):
        """Process all files in a directory"""
        state = self.directory_state[directory]
        pending_files = state['pending_files'].copy()

        for filepath in pending_files:
            if not os.path.exists(filepath):
                # File might have been moved by another process
                state['pending_files'].remove(filepath)
                continue

            if self.process_music_file(filepath):
                state['pending_files'].remove(filepath)
                state['processed_files'].add(filepath)
            else:
                state['pending_files'].remove(filepath)
                state['failed_files'].add(filepath)
                self.move_to_unknown(filepath)

        # If no more pending files, handle cleanup
        if not state['pending_files']:
            self.handle_remaining_files(directory)
            # Clean up tracking
            del self.directory_state[directory]
            del self.last_file_time[directory]

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
            audio = File(filepath)
            if audio is None:
                print(f"Could not read tags from {filepath}. Skipping.")
                return False
            
            print(f"Audio tags: {audio.tags}")  # Debugging

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
    
    def handle_remaining_files(self, processed_dir):
        """Move any non-music files to Unknown folder structure and delete the processed directory"""
        music_extensions = ('.mp3', '.flac', '.ogg', '.m4a')
        for root, dirs, files in os.walk(processed_dir):
            if files:  # If there are any files in this directory
                for file in files:
                    # Skip music files as they should have been processed already
                    if file.endswith(music_extensions):
                        continue
                    source_file = os.path.join(root, file)
                    # Create relative path structure starting from watch folder
                    rel_path = os.path.relpath(root, processed_dir)
                    # Create destination path in Unknown folder with same structure
                    dest_dir = os.path.join('unknown', rel_path)
                    os.makedirs(dest_dir, exist_ok=True)
                    
                    # Move file to Unknown folder structure
                    shutil.move(source_file, os.path.join(dest_dir, file))
                    print(f"Moved unprocessed file to Unknown folder: {file}")
        
        # After moving all files, remove the processed directory
        try:
            shutil.rmtree(processed_dir)
            print(f"Removed processed directory: {processed_dir}")
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
