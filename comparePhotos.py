import os
import exifread

def get_photo_metadata(file_path):
    with open(file_path, 'rb') as f:
        tags = exifread.process_file(f)
    return tags

def compare_photos(photo_folder, external_hard_drive_folder):
    photo_files = set()
    external_hard_drive_files = set()

    for root, dirs, files in os.walk(photo_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.heic')):
                photo_files.add(get_photo_metadata(file_path))

    for root, dirs, files in os.walk(external_hard_drive_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.heic')):
                external_hard_drive_files.add(get_photo_metadata(file_path))

    common_files = photo_files.intersection(external_hard_drive_files)

    print(f'Total Photos in Photos app: {len(photo_files)}')
    print(f'Total Photos in External Hard Drive: {len(external_hard_drive_files)}')
    print(f'Common Photos: {len(common_files)}')

if __name__ == "__main__":
    photos_app_folder = "/Users/viniththomas/Pictures/Photos Library.photoslibrary"  # Replace with the actual path to your Photos app library
    external_hard_drive_folder = "/Volumes/Magnith WD/PhoneBackup"  # Replace with the actual path to your external hard drive
    compare_photos(photos_app_folder, external_hard_drive_folder)
