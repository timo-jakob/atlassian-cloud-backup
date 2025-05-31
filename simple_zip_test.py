#!/usr/bin/env python3
"""
Simple test to understand the ZIP compression issue.
"""

import zipfile
import tempfile
import os

def create_problematic_zip():
    """Create a ZIP that would trigger the individual entry ratio issue."""
    
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
        zip_path = temp_zip.name
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        # Add a file that compresses extremely well (like log files with repetitive content)
        # This could easily be found in Jira backups - log files, database dumps with repeating patterns, etc.
        repetitive_content = "ERROR: Database connection failed\n" * 10000  # ~340KB of repetitive log content
        zf.writestr("logs/application.log", repetitive_content)
        
        # Add normal content
        normal_content = "Some normal backup content with mixed data."
        zf.writestr("data/backup.json", normal_content)
    
    return zip_path

def analyze_individual_ratios(zip_path):
    """Show what individual compression ratios look like."""
    print("Individual file compression ratios in ZIP:")
    print("-" * 50)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            if not info.filename.endswith('/') and info.compress_size > 0:
                ratio = info.file_size / info.compress_size
                print(f"{info.filename}")
                print(f"  Uncompressed: {info.file_size:,} bytes")
                print(f"  Compressed: {info.compress_size:,} bytes")
                print(f"  Ratio: {ratio:.2f}x {'⚠️ EXCEEDS 300' if ratio > 300 else '✅ OK'}")
                print()

def main():
    zip_path = create_problematic_zip()
    try:
        analyze_individual_ratios(zip_path)
    finally:
        os.unlink(zip_path)

if __name__ == "__main__":
    main()
