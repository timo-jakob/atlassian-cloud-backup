# Filesystem Discovery Documentation

## Overview

The Filesystem Discovery feature makes the Atlassian Cloud Backup application resilient and self-learning by automatically scanning the target directory for existing backup files when no consolidated backup status file exists. This enables the application to reconstruct its internal state from the filesystem, making it more robust and user-friendly.

## Key Features

### 1. Automatic Site Detection
- Scans the backup target directory for site-specific subfolders
- Reconstructs Atlassian URLs from sanitized folder names using reverse engineering
- Identifies sites based on common Atlassian Cloud patterns (*.atlassian.net, *.atlassian.com)

### 2. Backup File Discovery
- Parses backup filenames to extract service type (Jira/Confluence) and dates
- Supports multiple file extensions (.zip, .tar.gz)
- Selects the most recent backup when multiple files exist for the same service
- Uses pattern matching: `{service}-backup-{YYYY-MM-DD}.{extension}`

### 3. Status Reconstruction
- Creates datetime objects with 00:00:00 time (since only dates are in filenames)
- Builds consolidated status structure compatible with existing application logic
- Automatically saves discovered status to avoid future discovery overhead

### 4. Integration with Existing Flow
- Seamlessly integrates with the existing `FileManager.load_consolidated_status()` method
- Triggers discovery when consolidated status file is missing or corrupted
- Provides fallback mechanism for resilient operation

## File Structure Requirements

The discovery expects the following directory structure:

```
backup_target_directory/
├── consolidated_backup_status.json  # Main status file (created if missing)
├── site1.atlassian.net/            # Site-specific folders
│   ├── jira-backup-2024-12-15.zip
│   └── confluence-backup-2024-12-20.zip
├── site2.atlassian.net/
│   └── jira-backup-2024-12-10.zip
└── dev-team.atlassian.net/
    └── confluence-backup-2024-12-18.tar.gz
```

## Supported Backup Filename Patterns

- `jira-backup-YYYY-MM-DD.zip`
- `confluence-backup-YYYY-MM-DD.zip`
- `jira-backup-YYYY-MM-DD.tar.gz`
- `confluence-backup-YYYY-MM-DD.tar.gz`

## URL Reconstruction Logic

The system reconstructs Atlassian URLs from folder names using these heuristics:

1. **Direct Pattern Matching**: Looks for `.atlassian.net` or `.atlassian.com` patterns
2. **Underscore Replacement**: Converts underscores back to dots for common domain patterns
3. **Atlassian Detection**: Identifies folders containing "atlassian" and applies appropriate transformations
4. **HTTPS Prefix**: Adds `https://` prefix to reconstructed URLs

### Examples:
- `mycompany.atlassian.net` → `https://mycompany.atlassian.net`
- `test_org.atlassian.net` → `https://test.org.atlassian.net`
- `dev-team.atlassian.net` → `https://dev-team.atlassian.net`

## API Reference

### FilesystemDiscovery Class

#### `__init__(backup_target_directory=None)`
Initializes the discovery with the specified backup directory.

#### `discover_sites_and_backups()`
Main discovery method that returns a consolidated status dictionary.

**Returns:**
```python
{
    "https://site1.atlassian.net": {
        "last_jira_backup": datetime(2024, 12, 15, 0, 0, 0),
        "jira_file": "/path/to/jira-backup-2024-12-15.zip",
        "last_confluence_backup": datetime(2024, 12, 20, 0, 0, 0),
        "confluence_file": "/path/to/confluence-backup-2024-12-20.zip"
    },
    ...
}
```

#### `get_backup_statistics(discovered_status)`
Generates statistics about discovered backups.

**Returns:**
```python
{
    "total_sites": 3,
    "sites_with_jira": 2,
    "sites_with_confluence": 2,
    "sites_with_both": 1,
    "total_backup_files": 4,
    "oldest_backup": datetime(2024, 12, 10, 0, 0, 0),
    "newest_backup": datetime(2024, 12, 20, 0, 0, 0)
}
```

### FileManager Integration

The `FileManager.load_consolidated_status()` method has been enhanced to:

1. **Primary Path**: Load existing consolidated status file
2. **Discovery Path**: If file missing, trigger filesystem discovery
3. **Fallback Path**: If file corrupted, use discovery as fallback
4. **Auto-Save**: Save discovered status to prevent future discovery overhead

## Usage Examples

### Programmatic Usage

```python
from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery
from atlassian_cloud_backup.utils.file_utils import FileManager

# Direct discovery
discovery = FilesystemDiscovery("/path/to/backups")
discovered = discovery.discover_sites_and_backups()
stats = discovery.get_backup_statistics(discovered)

# Through FileManager (recommended)
file_manager = FileManager("https://mysite.atlassian.net", "/path/to/backups")
status = file_manager.load_consolidated_status()  # Triggers discovery if needed
```

### Command Line Usage

The discovery integrates transparently with existing backup operations:

```bash
# If consolidated_backup_status.json is missing, discovery will run automatically
python -m atlassian_cloud_backup --url https://mysite.atlassian.net --backup-dir /backups
```

## Logging and Monitoring

The discovery provides comprehensive logging:

```
INFO: Starting filesystem discovery in: /path/to/backups
INFO: Discovered site: https://mysite.atlassian.net with 2 backup files
INFO: Filesystem discovery completed. Found 3 sites.
INFO: Filesystem discovery found 3 sites with 4 total backup files
INFO: Backup date range: 2024-12-10 to 2024-12-20
INFO: Saving discovered status to consolidated status file
```

## Error Handling

The discovery is designed to be robust:

- **Missing Directory**: Returns empty status gracefully
- **Permission Errors**: Logs warnings and continues with accessible directories
- **Invalid Filenames**: Skips files that don't match expected patterns
- **Corrupted Files**: Doesn't attempt to validate file contents (only filenames)
- **URL Reconstruction Failures**: Logs debug messages for unrecognizable folder names

## Performance Considerations

- **Caching**: Discovered status is saved to avoid repeated filesystem scans
- **Lazy Loading**: Discovery only runs when consolidated status file is unavailable
- **Minimal I/O**: Only reads directory listings and filenames, not file contents
- **Early Returns**: Stops processing at first sign of unavailable resources

## Testing

The feature includes comprehensive test coverage:

- **17 test cases** covering all major functionality
- **Unit tests** for individual discovery methods
- **Integration tests** with FileManager
- **Edge case handling** for invalid inputs and error conditions
- **Statistics validation** for complex scenarios

Run tests with:
```bash
python -m pytest tests/test_filesystem_discovery.py -v
```

## Benefits

### For Users
- **Resilience**: Application recovers from missing or corrupted status files
- **Migration Support**: Easy to move backup directories between systems
- **Self-Healing**: Automatically reconstructs state from existing backups
- **Transparency**: Clear logging shows what was discovered

### For Developers
- **Separation of Concerns**: Discovery logic isolated in dedicated module
- **Testability**: Comprehensive test coverage with mock filesystem scenarios
- **Maintainability**: Clean API and well-documented functionality
- **Extensibility**: Easy to add support for new backup patterns or file types

## Limitations

1. **URL Reconstruction**: Heuristic-based approach may not handle all edge cases perfectly
2. **Time Information**: Only dates available from filenames, so times default to 00:00:00
3. **File Validation**: Doesn't verify backup file integrity or completeness
4. **Service Detection**: Relies on filename patterns; non-standard names won't be recognized

## Future Enhancements

Potential improvements for future versions:

1. **Metadata Files**: Support for companion metadata files with additional backup information
2. **File Validation**: Optional integrity checking of discovered backup files
3. **Custom Patterns**: Configuration support for non-standard filename patterns
4. **Performance Optimization**: Parallel directory scanning for large backup structures
5. **Reporting**: Enhanced statistics and discovery reports
