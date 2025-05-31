#!/usr/bin/env python3
"""
Demonstration of the refactored URL reconstruction functionality.

This script shows how the _reconstruct_url_from_folder_name method
has been refactored to reduce cognitive complexity while maintaining
the same functionality.
"""

import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery


def demo_url_reconstruction():
    """Demonstrate the refactored URL reconstruction functionality."""
    print("=" * 70)
    print("URL Reconstruction Refactoring Demo")
    print("=" * 70)
    print()
    
    discovery = FilesystemDiscovery()
    
    # Test cases covering different scenarios
    test_cases = [
        # Direct Atlassian matches (complete domains)
        ("mycompany.atlassian.net", "Direct Atlassian domain (.net)"),
        ("test.atlassian.com", "Direct Atlassian domain (.com)"),
        ("company_name.atlassian.net", "Underscore in company name"),
        
        # Incomplete Atlassian matches (need reconstruction)
        ("mycompany_atlassian", "Missing domain extension"),
        ("test_site_atlassian", "Missing domain extension"),
        
        # Generic domain patterns
        ("example.com", "Generic domain"),
        ("my-site.example.org", "Complex generic domain"),
        
        # Invalid cases
        ("justtext", "No domain pattern"),
        ("no_dots_here", "No dots"),
        ("", "Empty string"),
    ]
    
    print("Testing refactored URL reconstruction method:")
    print("-" * 70)
    
    for folder_name, description in test_cases:
        result = discovery._reconstruct_url_from_folder_name(folder_name)
        status = "✓" if result else "✗"
        result_str = result if result else "None"
        
        print(f"{status} {folder_name:25} -> {result_str:35} | {description}")
    
    print()
    print("=" * 70)
    print("Refactoring Benefits:")
    print("=" * 70)
    print("✓ Reduced cognitive complexity by breaking down into smaller methods")
    print("✓ Each method has a single, clear responsibility:")
    print("  - _try_direct_atlassian_match(): Handle complete Atlassian domains")
    print("  - _try_incomplete_atlassian_match(): Handle partial Atlassian domains")
    print("  - _extract_base_domain(): Extract base domain from complex strings")
    print("  - _try_generic_domain_match(): Handle generic domain patterns")
    print("✓ Improved readability and maintainability")
    print("✓ Easier to test individual components")
    print("✓ Same functionality, better structure")
    print()
    
    # Show the method structure
    print("Method Structure:")
    print("-" * 70)
    print("Original method: Single complex method with nested conditionals")
    print("Refactored approach:")
    print("  _reconstruct_url_from_folder_name() [Main orchestrator]")
    print("  ├── _try_direct_atlassian_match()")
    print("  ├── _try_incomplete_atlassian_match()")
    print("  │   └── _extract_base_domain()")
    print("  └── _try_generic_domain_match()")
    print()


if __name__ == "__main__":
    demo_url_reconstruction()
