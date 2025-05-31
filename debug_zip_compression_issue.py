#!/usr/bin/env python3
"""
Debug script to reproduce the ZIP compression ratio issue mentioned in the conversation.
"""

import sys
import os
import zipfile
import tempfile

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery

def create_test_zip_with_high_compression():
    """Create a ZIP file that might trigger the 512.53 compression ratio issue."""
    
    # Create a temporary ZIP file
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
        zip_path = temp_zip.name
    
    # Create ZIP with highly compressible content
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        # Add a file with highly repetitive content (compresses very well)
        large_content = "A" * 50000  # 50KB of repeating 'A'
        zf.writestr("large_file.txt", large_content)
        
        # Add a small file
        small_content = "small content"
        zf.writestr("small_file.txt", small_content)
    
    return zip_path

def analyze_zip_compression_ratios(zip_path):
    """Analyze individual file compression ratios within a ZIP."""
    print(f"🔍 ANALYZING ZIP FILE: {zip_path}")
    print("-" * 60)
    
    total_uncompressed = 0
    total_compressed = 0
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            if not info.filename.endswith('/'):  # Skip directories
                ratio = info.file_size / info.compress_size if info.compress_size > 0 else 0
                print(f"File: {info.filename}")
                print(f"  Uncompressed: {info.file_size:,} bytes")
                print(f"  Compressed: {info.compress_size:,} bytes") 
                print(f"  Ratio: {ratio:.2f}x")
                print()
                
                total_uncompressed += info.file_size
                total_compressed += info.compress_size
    
    overall_ratio = total_uncompressed / total_compressed if total_compressed > 0 else 0
    print(f"OVERALL ZIP COMPRESSION:")
    print(f"  Total uncompressed: {total_uncompressed:,} bytes")
    print(f"  Total compressed: {total_compressed:,} bytes")
    print(f"  Overall ratio: {overall_ratio:.2f}x")
    print()

def test_filesystem_discovery_validation(zip_path):
    """Test the FilesystemDiscovery validation on our ZIP file."""
    print(f"🧪 TESTING FILESYSTEM DISCOVERY VALIDATION")
    print("-" * 60)
    
    discovery = FilesystemDiscovery()
    
    # Check if the file validates
    result = discovery._check_zip_bomb_safety(
        zipfile.ZipFile(zip_path, 'r'), 
        zip_path,
        threshold_entries=1000000,    # 1M entries
        threshold_size=1000000000000, # 1TB
        threshold_ratio=300           # 300x compression ratio
    )
    
    print(f"Validation result: {'✅ PASSED' if result else '❌ FAILED'}")
    return result

def main():
    print("=" * 70)
    print("DEBUG: ZIP COMPRESSION RATIO ISSUE")
    print("=" * 70)
    print()
    
    # Create test ZIP
    zip_path = create_test_zip_with_high_compression()
    
    try:
        # Analyze the ZIP compression ratios
        analyze_zip_compression_ratios(zip_path)
        
        # Test validation
        test_filesystem_discovery_validation(zip_path)
        
    finally:
        # Clean up
        if os.path.exists(zip_path):
            os.unlink(zip_path)

if __name__ == "__main__":
    main()
