# list_cache_files.py
import os
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def list_directory_contents(path, indent=""):
    """Recursively list contents of a directory with indentation."""
    try:
        # Get the directory from environment variable
        cache_dir = Path(path)
        
        if not cache_dir.exists():
            logger.error(f"❌ Directory does not exist: {cache_dir}")
            return
            
        for item in sorted(cache_dir.iterdir()):
            if item.is_file():
                logger.info(f"{indent}📄 {item.name}")
            elif item.is_dir():
                logger.info(f"{indent}📁 {item.name}/")
                list_directory_contents(item, indent + "  ")
                
    except Exception as e:
        logger.error(f"❌ Error listing directory contents: {e}")

def main():
    cache_dir = os.environ.get("DATA_CACHE_DIR")
    
    if not cache_dir:
        logger.error("❌ DATA_CACHE_DIR environment variable not set")
        return
        
    logger.info(f"📂 Listing contents of {cache_dir}")
    list_directory_contents(cache_dir)

if __name__ == "__main__":
    main()

