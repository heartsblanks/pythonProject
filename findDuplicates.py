import os
import hashlib
from collections import defaultdict
import shutil

def get_file_checksum(file_path, block_size=65536):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read(block_size)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(block_size)
    return hasher.hexdigest()

def move_duplicates(folder_path):
    file_checksums = defaultdict(list)
    total_files = 0
    total_duplicates = 0

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                total_files += 1
                checksum = get_file_checksum(file_path)
                mtime = os.path.getmtime(file_path)
                file_checksums[(checksum, mtime)].append(file_path)

    for (checksum, mtime), paths in file_checksums.items():
        if len(paths) > 1:
            total_duplicates += len(paths)
            print(f'Duplicate Files (Checksum: {checksum}, Modified Time: {mtime}):')
            duplicate_folder = os.path.join(folder_path, 'duplicates')
            os.makedirs(duplicate_folder, exist_ok=True)

            for idx, path in enumerate(paths):
                # Add a suffix to the duplicate file name
                base_name, extension = os.path.splitext(os.path.basename(path))
                new_name = f"{base_name} ({idx + 1}){extension}"

                # Construct the destination path in the 'duplicates' folder
                destination_path = os.path.join(duplicate_folder, new_name)

                # Move the duplicate file to the 'duplicates' folder
                shutil.move(path, destination_path)

                print(f'  Moved: {path} to {destination_path}')

    print(f'Total Files: {total_files}')
    print(f'Total Duplicates: {total_duplicates}')

if __name__ == "__main__":
    folder_path = "/Volumes/Magnith WD/PhoneBackup/Bengaluru, 6 September 2020"
    move_duplicates(folder_path)
