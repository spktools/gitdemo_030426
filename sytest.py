import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

# ---------------- CONFIG ----------------
SOURCE_DIR = r"\\10.0.2.63\SJ_RenderImages\Lighting\SJ314B"
DEST_DIR   = r"\\30.0.0.92\SPKRender\SJ_RenderImages\Lighting\SJ314B"

EXCLUDE_FOLDERS = {"backup", "Backup", "_backup", ".backup"}
MAX_WORKERS = 5
RETRY_COUNT = 3
RETRY_DELAY = 1  # seconds
# ----------------------------------------


def is_excluded(path):
    """Check if path contains excluded folders"""
    parts = path.lower().split(os.sep)
    return any(folder.lower() in parts for folder in EXCLUDE_FOLDERS)


def should_copy(src, dest):
    """Check if file should be copied"""
    if not os.path.exists(dest):
        return True

    try:
        return (
            os.path.getmtime(src) > os.path.getmtime(dest) or
            os.path.getsize(src) != os.path.getsize(dest)
        )
    except Exception:
        return True


def is_file_ready(path):
    """Ensure file is not being written"""
    try:
        size1 = os.path.getsize(path)
        time.sleep(0.5)
        size2 = os.path.getsize(path)
        return size1 == size2
    except Exception:
        return False


def safe_copy(src, dest):
    """Copy with retry (important for network paths)"""
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    for attempt in range(RETRY_COUNT):
        try:
            if is_file_ready(src):
                shutil.copy2(src, dest)
                print(f"[COPIED] {src} -> {dest}")
                return
        except Exception as e:
            print(f"[RETRY {attempt+1}] {src} | {e}")
            time.sleep(RETRY_DELAY)

    print(f"[FAILED] {src}")


def sync_file(src_path):
    """Process single file"""
    if is_excluded(src_path):
        return

    try:
        relative = os.path.relpath(src_path, SOURCE_DIR)
        dest_path = os.path.join(DEST_DIR, relative)

        if should_copy(src_path, dest_path):
            safe_copy(src_path, dest_path)
        else:
            print(f"[SKIPPED] {src_path}")

    except Exception as e:
        print(f"[ERROR] {src_path} | {e}")


def sync_folders():
    """Main sync function (multi-threaded)"""
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for root, dirs, files in os.walk(SOURCE_DIR):

            # Remove excluded directories from traversal
            dirs[:] = [d for d in dirs if d not in EXCLUDE_FOLDERS]

            for file in files:
                src_path = os.path.join(root, file)
                tasks.append(executor.submit(sync_file, src_path))

        # Wait for all tasks
        for task in tasks:
            task.result()


def remove_extra_files():
    """Optional: remove files not present in source (mirror sync)"""
    for root, dirs, files in os.walk(DEST_DIR):
        for file in files:
            dest_path = os.path.join(root, file)

            relative = os.path.relpath(dest_path, DEST_DIR)
            src_path = os.path.join(SOURCE_DIR, relative)

            if not os.path.exists(src_path):
                try:
                    os.remove(dest_path)
                    print(f"[REMOVED] {dest_path}")
                except Exception as e:
                    print(f"[DELETE ERROR] {dest_path} | {e}")


if __name__ == "__main__":
    start = time.time()

    print("Starting sync...")
    sync_folders()

    # Uncomment if you want mirror behavior
    # remove_extra_files()

    print(f"Sync completed in {round(time.time() - start, 2)} seconds")
