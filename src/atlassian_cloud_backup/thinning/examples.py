"""
Simple examples of the backup thinning trigger.

This demonstrates the streamlined approach: just trigger thinning of exactly
one file based on strategy, instance directory, and backup type.
"""

from pathlib import Path

try:
    # Try absolute import (when installed as package)
    from atlassian_cloud_backup.thinning import (
        BackupDeleter,
        DeletionConfig,
    )
except ImportError:
    # Fallback to relative import (when running directly)
    from manager import BackupDeleter, DeletionConfig


def example_simple_deletion_trigger():
    """Example of the simple thinning trigger - the main use case."""
    print("=== Simple Thinning Trigger ===")
    
    # Configure deletion strategy (default is retention_ladder)
    config = DeletionConfig()
    
    deleter = BackupDeleter(config)
    
    # Example: Thin one JIRA backup from instance 'abc'
    instance_dir = Path("/backups/abc")
    backup_type = "jira"
    
    print(f"Triggering thinning in: {instance_dir}")
    print(f"Backup type: {backup_type}")
    print(f"Strategy: {config.deletion_strategy}")
    
    # This is the main method - the trigger
    deleted_file = deleter.delete_one_backup(instance_dir, backup_type)
    
    if deleted_file:
        print(f"✅ Deleted: {deleted_file.name}")
    else:
        print("⚠️  No file was deleted")


def example_different_strategies():
    """Example showing different thinning strategies."""
    print("\n=== Different Thinning Strategies ===")
    
    # Retention ladder strategy (default)
    print("\n--- Retention Ladder Strategy (recommended) ---")
    print("Logic:")
    print("  • Within 1 month: Keep latest backup of each week")
    print("  • Within 1 year: Keep latest backup of each month")
    print("  • Beyond 1 year: Keep latest backup of each year")
    print("Trigger: deleter.delete_one_backup(instance_dir, 'jira')")
    print("Result: Intelligently thins backups while preserving important milestones")
    
    # Oldest first strategy
    print("\n--- Oldest First Strategy (simple) ---")
    print("Logic: Always deletes the oldest backup file")
    print("Usage: DeletionConfig(deletion_strategy='oldest_first')")
    print("Result: Maintains a rolling window of recent backups")


def example_filename_patterns():
    """Example showing supported filename patterns."""
    print("\n=== Filename Patterns ===")
    
    config = DeletionConfig(deletion_strategy="oldest_first")
    deleter = BackupDeleter(config)
    
    # Example filenames for both backup types
    example_files = [
        ("665805-jira-backup-2025-07-19", "jira"),
        ("789012-jira-backup-2025-07-18", "jira"),
        ("confluence-backup-2025-07-20", "confluence"),
        ("confluence-backup-2025-07-18", "confluence"),
        ("invalid-filename.zip", "jira"),  # This won't match
    ]
    
    print("Supported filename patterns:")
    print("  JIRA:       <task_id>-jira-backup-<year>-<month>-<day>")
    print("  Confluence: confluence-backup-<year>-<month>-<day>")
    print("\nExamples:")
    
    for filename, backup_type in example_files:
        # Show which files would be recognized
        is_match = deleter._is_backup_file(filename, backup_type)
        
        if is_match:
            task_id = deleter._extract_task_id(filename, backup_type)
            if backup_type == "jira":
                print(f"  {filename} → JIRA backup, Task ID: {task_id}")
            else:
                print(f"  {filename} → Confluence backup")
        else:
            print(f"  {filename} → Not a recognized {backup_type} backup file")


def example_real_world_usage():
    """Example showing real-world usage pattern."""
    print("\n=== Real-World Usage ===")
    
    print("Scenario: External system needs to thin one backup before storing a new one")
    print()
    
    # Setup - just show the approach
    instance_name = "abc"
    backup_type = "jira"
    
    print("# External system triggers thinning")
    print(f"instance_dir = Path('/backups/{instance_name}')")
    print(f"backup_type = '{backup_type}'")
    print("")
    print("# Thin exactly one file using retention ladder")
    print("config = DeletionConfig()  # Uses retention_ladder by default")
    print("deleter = BackupDeleter(config)")
    print("deleted_file = deleter.delete_one_backup(instance_dir, backup_type)")
    print("")
    print("# Result: One file deleted (or None if strategy says keep all)")
    print("# Now safe to store new backup file")
    
    # Show what would happen
    print("\nWhat happens with retention ladder:")
    print(f"1. Scans /backups/{instance_name} for {backup_type} backup files")
    print("2. Groups backups by time periods (recent, monthly, yearly)")
    print("3. Identifies redundant backups within each group")
    print("4. Selects oldest redundant backup for deletion")
    print("5. Returns path of deleted file (or None)")


def example_retention_ladder_detail():
    """Detailed example of how the retention ladder strategy works."""
    print("\n=== Retention Ladder Strategy Detail ===")
    
    print("The retention ladder maintains backups with different granularities:")
    print()
    
    print("📅 Recent Period (last 30 days):")
    print("   • Keep the latest backup from each week")
    print("   • Example: If you have daily backups, keep only the latest from each week")
    print()
    
    print("📅 Medium Term (30 days to 1 year ago):")
    print("   • Keep the latest backup from each month")
    print("   • Example: Keep end-of-month backups, delete mid-month ones")
    print()
    
    print("📅 Long Term (over 1 year ago):")
    print("   • Keep the latest backup from each year")
    print("   • Example: Keep end-of-year backups, delete others")
    print()
    
    print("🎯 Benefits:")
    print("   • Fine-grained recovery for recent issues")
    print("   • Coarse-grained recovery for historical issues")
    print("   • Automatic space optimization")
    print("   • Preserves important milestone backups")
    
    print()
    print("Example timeline after retention ladder thinning:")
    print("   Today: ✅ backup-2025-07-26")
    print("   Last week: ✅ backup-2025-07-19 (latest of that week)")
    print("   2 weeks ago: ✅ backup-2025-07-12 (latest of that week)")
    print("   Last month: ✅ backup-2025-06-30 (latest of June)")
    print("   2 months ago: ✅ backup-2025-05-31 (latest of May)")
    print("   Last year: ✅ backup-2024-12-31 (latest of 2024)")
    print("   2 years ago: ✅ backup-2023-12-31 (latest of 2023)")


if __name__ == "__main__":
    example_simple_deletion_trigger()
    example_different_strategies()
    example_filename_patterns()
    example_real_world_usage()
    example_retention_ladder_detail()
