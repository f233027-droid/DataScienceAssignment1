
import os
import sys
import requests
from pathlib import Path
import shutil

def setup_dependencies():
    base_dir = Path(__file__).parent
    bin_dir = base_dir / "bin"
    bin_dir.mkdir(exist_ok=True)
    
    # URLs for Hadoop 3.X binaries (compatible with recent Spark)
    files = {
        "winutils.exe": "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe",
        "hadoop.dll": "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
    }
    
    print("\n--- Checking Hadoop Binaries ---")
    for name, url in files.items():
        filepath = bin_dir / name
        if not filepath.exists():
            print(f"Downloading {name}...")
            try:
                r = requests.get(url)
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    f.write(r.content)
                print(f"Downloaded {name}")
            except Exception as e:
                print(f"Failed to download {name}: {e}")
        else:
            print(f"{name} already exists.")
            
    print(f"Hadoop binaries located in: {bin_dir.resolve()}")
    
    # Check for Java
    print("\n--- Checking Java ---")
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"JAVA_HOME is set to: {java_home}")
        return java_home
        
    print("JAVA_HOME not set. Searching common locations...")
    common_paths = [
        r"C:\Program Files\Java",
        r"C:\Program Files (x86)\Java",
        r"C:\Program Files\Eclipse Adoptium",
        r"C:\Program Files\Microsoft\jdk",
        r"C:\Program Files\Zulu"
    ]
    
    found_java = None
    for path_str in common_paths:
        p = Path(path_str)
        if p.exists():
            # Look for subdirectories (e.g. jdk-11...)
            for child in p.iterdir():
                if child.is_dir() and (child.name.startswith("jdk") or child.name.startswith("jre")):
                    # Check for bin/java.exe
                    if (child / "bin" / "java.exe").exists():
                        found_java = child
                        break
        if found_java:
            break
            
    if found_java:
        print(f"Found Java at: {found_java}")
        return str(found_java)
    else:
        print("WARNING: Java installation not found in common locations.")
        print("Please install JDK 11 or 17 manually.")
        return None

if __name__ == "__main__":
    setup_dependencies()
