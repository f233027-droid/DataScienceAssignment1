
import requests
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

try:
    from assignment01.config import RAW_DATA_DIR
except ImportError:
    try:
        # Fallback if running as script from ingestion folder
        from ..config import RAW_DATA_DIR
    except ImportError:
        # Fallback absolute
        RAW_DATA_DIR = Path("data/raw")

def download_file(filename: str, url: str, output_dir: Path) -> str:
    """Download a single file if it doesn't represent."""
    filepath = output_dir / filename
    if filepath.exists():
        print(f"Skipping {filename}, already exists.")
        return str(filepath)
    
    print(f"Downloading {filename}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {filename}")
        return str(filepath)
    except Exception as e:
        print(f"Failed to download {filename}: {e}")
        if filepath.exists():
             filepath.unlink() # Remove partial file
        return None

def download_files(links: Dict[str, str], max_workers: int = 4):
    """Downloads multiple files in parallel."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for filename, url in links.items():
            futures.append(executor.submit(download_file, filename, url, RAW_DATA_DIR))
        
        for future in futures:
            future.result()

if __name__ == "__main__":
    # Dummy test
    pass
