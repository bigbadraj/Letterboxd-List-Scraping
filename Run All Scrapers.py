import subprocess
import time
from datetime import datetime
import sys
import io
import os
import platform
import shutil
import zipfile

# Set console output encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Phase 1: Data Scraping
SCRAPING_SCRIPTS = [
    ("BoxOfficeMojo 250s.py", "Box Office Mojo Scraper"),
    ("Top 250 Anything.py", "Letterboxd Min Filtering Scraper"),
    ("Comedy 100.py", "Letterboxd Comedy List Scraper"),
    ("5000 Pop and Top.py", "Letterboxd 5000 Pop and Top Films Scraper"),
    ("Genre 250s.py", "Top 250 Genres Scraper"),
]

# Phase 2: Data Processing and Updates
PROCESSING_SCRIPTS = [
    ("Update Letterboxd Lists.py", "Update Lists on Letterboxd"),
]

# Phase 3: Extension Building
ENABLE_EXTENSION_BUILD = True

# Phase 4: Extension Packaging
ENABLE_EXTENSION_PACKAGING = True

# =============================================================================

# Detect operating system and set appropriate Python command
def get_python_command():
    """Return the appropriate Python command for the current OS."""
    system = platform.system()
    
    if system == "Darwin":  # macOS
        return "python3"
    else:  # Windows, Linux, etc.
        return "python"

# Get the appropriate Python command
PYTHON_CMD = get_python_command()

def format_time(seconds):
    """Format time in a human-readable format"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def run_script(script_name, description, *args):
    """Run a Python script and track its execution"""
    print(f"\n{f' Running {description} ':=^100}")
    start_time = time.time()
    
    try:
        # Set environment variable for UTF-8 encoding
        env = dict(os.environ, PYTHONIOENCODING='utf-8')
        
        # Add encoding parameters to handle special characters
        process = subprocess.Popen(
            [PYTHON_CMD, '-u', script_name] + list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            encoding='utf-8',
            errors='replace',
            env=env  # Add environment variables
        )

        # Print output in real-time with proper encoding
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                # Don't add extra newlines for progress bars
                if '\r' in output:
                    print(output.strip(), end='\r', flush=True)
                else:
                    print(output.strip(), flush=True)

        # Wait for the process to complete
        process.poll()

        # Calculate execution time
        execution_time = time.time() - start_time

        if process.returncode == 0:
            print(f"\n[+] {description} completed successfully")
        else:
            print(f"\n[-] {description} failed with return code {process.returncode}")

        print(f"⏱️ Execution time: {format_time(execution_time)}")
        return True

    except Exception as e:
        print(f"\n[-] Error running {description}: {str(e)}")
        return False

def run_node_script(script_path, description):
    """Run a Node.js script and track its execution"""
    print(f"\n{f' Running {description} ':=^100}")
    start_time = time.time()
    
    try:
        # Check if Node.js is available
        try:
            subprocess.run(['node', '--version'], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"\n❌ Node.js is not installed or not in PATH")
            print(f"Please install Node.js from https://nodejs.org/")
            print(f"Skipping extension building phase...")
            return False
        
        # Run the Node.js script from the main directory
        original_dir = os.getcwd()
        
        # Run the Node.js script
        process = subprocess.Popen(
            ['node', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            encoding='utf-8',
            errors='replace'
        )

        # Print output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                if '\r' in output:
                    print(output.strip(), end='\r', flush=True)
                else:
                    print(output.strip(), flush=True)

        # Wait for the process to complete
        process.poll()

        # Calculate execution time
        execution_time = time.time() - start_time

        if process.returncode == 0:
            print(f"\n[+] {description} completed successfully")
        else:
            print(f"\n[-] {description} failed with return code {process.returncode}")

        print(f"⏱️ Execution time: {format_time(execution_time)}")
        
        return True

    except Exception as e:
        print(f"\n[-] Error running {description}: {str(e)}")
        return False

def create_extension_zip():
    """Create a zip file of the extension for Chrome Web Store upload"""
    print(f"\n{f' Creating Extension Zip File ':=^100}")
    start_time = time.time()
    
    try:
        # Create Extension Versions directory if it doesn't exist
        versions_dir = "Extension Versions"
        if not os.path.exists(versions_dir):
            os.makedirs(versions_dir)
            print(f"📁 Created {versions_dir} directory")
        
        # Create zip file name with timestamp
        zip_name = f"Betterboxd-Extension-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
        zip_path = os.path.join(versions_dir, zip_name)
        
        print(f"📦 Creating extension zip file: {zip_name}")
        
        # Create zip file with all files from MyExtension directory
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk('MyExtension'):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Get relative path from MyExtension directory
                    arcname = os.path.relpath(file_path, 'MyExtension')
                    zipf.write(file_path, arcname)
                    print(f"  📦 Added: {arcname}")
        
        execution_time = time.time() - start_time
        print(f"\n[+] Extension zip file created successfully: {zip_path}")
        print(f"⏱️ Execution time: {format_time(execution_time)}")
        return True
        
    except Exception as e:
        print(f"\n[-] Error creating extension zip file: {str(e)}")
        return False


def main():
    start_time = time.time()
    current_date = datetime.now().strftime("%B %d, %Y")
    
    print(f"\n{'='*100}")
    print(f"Starting Complete Automation Pipeline - {current_date}".center(100))
    print(f"{'='*100}")
    
    # Show which phases are enabled
    print(f"\n📋 ENABLED PHASES:")
    print(f"  Phase 1 - Data Scraping: {len(SCRAPING_SCRIPTS)} scripts")
    for i, (script_file, description) in enumerate(SCRAPING_SCRIPTS, 1):
        print(f"    {i}. {description}")
    
    print(f"  Phase 2 - Data Processing: {len(PROCESSING_SCRIPTS)} scripts")
    for i, (script_file, description) in enumerate(PROCESSING_SCRIPTS, 1):
        print(f"    {i}. {description}")
    
    print(f"  Phase 3 - Extension Building: {'ENABLED' if ENABLE_EXTENSION_BUILD else 'DISABLED'}")
    print(f"  Phase 4 - Extension Packaging: {'ENABLED' if ENABLE_EXTENSION_PACKAGING else 'DISABLED'}")
    print(f"{'='*100}\n")

    # Phase 1: Data Scraping
    print(f"\n{'='*100}")
    print(f"PHASE 1: DATA SCRAPING".center(100))
    print(f"{'='*100}")

    total_scraping_scripts = len(SCRAPING_SCRIPTS)
    completed_scraping_scripts = 0

    for script_file, description, *args in SCRAPING_SCRIPTS:
        completed_scraping_scripts += 1
        print(f"\nScraping Progress: {completed_scraping_scripts}/{total_scraping_scripts} scripts")
        
        if not run_script(script_file, description, *args):
            print(f"\n⚠️ Stopping execution due to error in {description}")
            return

    # Phase 2: Data Processing and Updates
    print(f"\n{'='*100}")
    print(f"PHASE 2: DATA PROCESSING & UPDATES".center(100))
    print(f"{'='*100}")

    total_processing_scripts = len(PROCESSING_SCRIPTS)
    completed_processing_scripts = 0

    for script_file, description, *args in PROCESSING_SCRIPTS:
        completed_processing_scripts += 1
        print(f"\nProcessing Progress: {completed_processing_scripts}/{total_processing_scripts} scripts")
        
        if not run_script(script_file, description, *args):
            print(f"\n⚠️ Stopping execution due to error in {description}")
            return

    # Phase 3: Extension Building
    if ENABLE_EXTENSION_BUILD:
        print(f"\n{'='*100}")
        print(f"PHASE 3: EXTENSION BUILDING".center(100))
        print(f"{'='*100}")

        # Build the extension with local JSON data
        extension_built = run_node_script('build.js', 'Extension Build Process')
        
        if not extension_built:
            print(f"\n⚠️ Extension building skipped or failed")
            print(f"📝 Data scraping and processing completed successfully")
            print(f"🔧 Install Node.js and run the build manually if needed")
            return
    else:
        print(f"\n{'='*100}")
        print(f"PHASE 3: EXTENSION BUILDING (SKIPPED)".center(100))
        print(f"{'='*100}")
        print(f"📝 Extension building is disabled in configuration")

    # Phase 4: Extension Packaging
    if ENABLE_EXTENSION_PACKAGING:
        print(f"\n{'='*100}")
        print(f"PHASE 4: EXTENSION PACKAGING".center(100))
        print(f"{'='*100}")

        # Create extension zip package
        if not create_extension_zip():
            print(f"\n⚠️ Extension packaging failed")
            print(f"📝 Data scraping, processing, and building completed successfully")
            print(f"🔧 You can manually zip the MyExtension folder")
            return
    else:
        print(f"\n{'='*100}")
        print(f"PHASE 4: EXTENSION PACKAGING (SKIPPED)".center(100))
        print(f"{'='*100}")
        print(f"📝 Extension packaging is disabled in configuration")

    # Phase 5: Completion
    print(f"\n{'='*100}")
    print(f"PHASE 5: COMPLETION".center(100))
    print(f"{'='*100}")

    print(f"\n✅ All phases completed successfully!")
    print(f"📦 Extension zip package is ready for use")
    print(f"📁 Check Extension Versions directory for the new extension zip file")

    # Calculate and display total execution time
    total_time = time.time() - start_time
    print(f"\n{'='*100}")
    print(f"COMPLETE AUTOMATION PIPELINE FINISHED".center(100))
    print(f"{'='*100}")
    print(f"Total execution time: {format_time(total_time)}".center(100))
    print(f"{'='*100}")
    print(f"\n🎉 All phases completed successfully!")
    print(f"📦 Extension zip package is ready for Chrome Web Store upload")
    print(f"📁 Check Extension Versions directory for the new extension zip file")
    print(f"🚀 Your extension is ready to be published!")

if __name__ == "__main__":
    main()