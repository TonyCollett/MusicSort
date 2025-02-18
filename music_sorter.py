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
    def on_created(self, event):
        if event.is_directory:
            return
        
        filepath = event.src_path
        if filepath.endswith(('.mp3', '.flac', '.ogg', '.m4a')):
            self.process_music_file(filepath)

    def process_music_file(self, filepath):
        print(f"Processing file: {filepath}")  # Debugging
        retries = 3
        wait_time = 1  # seconds

        for i in range(retries):
            try:
                # Check if the file can be opened for reading
                with open(filepath, 'r'):
                    pass  # Just try to open and close the file

                audio = File(filepath)
                if audio is None:
                    print(f"Could not read tags from {filepath}. Skipping.")
                    return
                print(f"Audio tags: {audio.tags}")  # Debugging
                artist = audio.get('artist', ['Unknown Artist'])[0]
                album = audio.get('album', ['Unknown Album'])[0]
                title = audio.get('title', ['Unknown Title'])[0]
                year = audio.get('date', ['Unknown Year'])[0][:4]  # Extract year from date

                if not artist or not album or not title:
                    print(f"Missing tags in {filepath}. Skipping.")
                    return

                # Sanitize file names
                artist = self.sanitize_filename(artist)
                album = self.sanitize_filename(album)
                title = self.sanitize_filename(title)

                # Create destination directory
                album_folder = f"{album} ({year})"
                destination_dir = os.path.join('sorted', artist, album_folder)
                os.makedirs(destination_dir, exist_ok=True)

                # Rename file, get the extension from original file
                file_extension = os.path.splitext(filepath)[1][1:] # remove leading dot
                new_filename = f"{title}.{file_extension}"
                new_filepath = os.path.join(destination_dir, new_filename)

                # Move file
                shutil.move(filepath, new_filepath)
                print(f"Moved {filepath} to {new_filepath}")
                return  # Success, exit retry loop

            except IOError as e:  # Catch file opening errors
                if i < retries - 1:
                    print(f"Could not read file {filepath}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Error processing {filepath}: {e}")
                    return
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
                return

    def sanitize_filename(self, filename):
        # Remove invalid characters for file names
        return "".join(c for c in filename if c.isalnum() or c in ['.', '_', ' ']).rstrip()

if __name__ == "__main__":
    watch_folder = 'watch'  # Replace with your watch folder
    if not os.path.exists(watch_folder):
        os.makedirs(watch_folder)
    
    sorted_folder = 'sorted' # The folder where sorted music will go
    if not os.path.exists(sorted_folder):
        os.makedirs(sorted_folder)

    event_handler = MusicFileHandler()
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
