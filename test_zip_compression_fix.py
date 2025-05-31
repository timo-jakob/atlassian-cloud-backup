#!/usr/bin/env python3
"""
Test the ZIP compression ratio fix.
"""

import sys
import os
import zipfile
import tempfile

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery

def create_legitimate_zip_with_high_individual_ratios():
    """Create a ZIP that has individual files with high compression but reasonable overall ratio."""
    
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
        zip_path = temp_zip.name
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        # Add a highly compressible file (like logs)
        repetitive_content = "ERROR: Database connection failed\n" * 10000  # ~340KB
        zf.writestr("logs/application.log", repetitive_content)
        
        # Add some less compressible content to balance the overall ratio
        mixed_content = os.urandom(50000)  # 50KB of random data (compresses poorly)
        zf.writestr("data/random_data.bin", mixed_content)
        
        # Add normal text content
        normal_content = "Normal backup content " * 1000  # ~23KB
        zf.writestr("backup/data.txt", normal_content)
    
    return zip_path

def analyze_compression_details(zip_path):
    """Analyze both individual and overall compression ratios."""
    print(f"📊 COMPRESSION ANALYSIS: {os.path.basename(zip_path)}")
    print("-" * 60)
    
    total_uncompressed = 0
    total_compressed = 0
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        print("INDIVIDUAL FILE RATIOS:")
        for info in zf.infolist():
            if not info.filename.endswith('/') and info.compress_size > 0:
                ratio = info.file_size / info.compress_size
                total_uncompressed += info.file_size
                total_compressed += info.compress_size
                
                status = "⚠️ HIGH" if ratio > 300 else "✅ OK"
                print(f"  {info.filename}: {ratio:.1f}x {status}")
        
        print(f"\nOVERALL ZIP RATIO:")
        overall_ratio = total_uncompressed / total_compressed if total_compressed > 0 else 0
        overall_status = "⚠️ HIGH" if overall_ratio > 300 else "✅ OK"
        print(f"  Total uncompressed: {total_uncompressed:,} bytes")
        print(f"  Total compressed: {total_compressed:,} bytes")
        print(f"  Overall ratio: {overall_ratio:.1f}x {overall_status}")
        print()

def test_validation(zip_path):
    """Test the updated validation logic."""
    print(f"🧪 VALIDATION TEST")
    print("-" * 60)
    
    discovery = FilesystemDiscovery()
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        result = discovery._check_zip_bomb_safety(
            zf, 
            zip_path,
            threshold_entries=1000000,    # 1M entries
            threshold_size=1000000000000, # 1TB
            threshold_ratio=300           # 300x compression ratio
        )
    
    print(f"Validation result: {'✅ PASSED' if result else '❌ FAILED'}")
    return result

def main():
    print("=" * 70)
    print("ZIP COMPRESSION RATIO FIX VALIDATION")
    print("=" * 70)
    print()
    
    print("🎯 GOAL: Fix false positives where individual files have high compression")
    print("   but overall ZIP compression ratio is reasonable.")
    print()
    
    # Create test ZIP
    zip_path = create_legitimate_zip_with_high_individual_ratios()
    
    try:
        # Analyze compression details
        analyze_compression_details(zip_path)
        
        # Test validation
        result = test_validation(zip_path)
        
        print("📋 SUMMARY:")
        print("-" * 60)
        if result:
            print("✅ SUCCESS: ZIP with high individual file ratios now validates correctly!")
            print("   The fix changes validation from per-file to overall compression ratio.")
        else:
            print("❌ FAILURE: ZIP still being rejected. More investigation needed.")
            
    finally:
        # Clean up
        if os.path.exists(zip_path):
            os.unlink(zip_path)

if __name__ == "__main__":
    main()
