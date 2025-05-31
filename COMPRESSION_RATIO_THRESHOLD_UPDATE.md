# Compression Ratio Threshold Update

## Issue
The original compression ratio threshold of 100 was too restrictive for legitimate Jira backup files, which can have compression ratios of around 143 or 118. This was causing valid backup files to be incorrectly flagged as potential zip/tar bombs.

## Solution
Updated the compression ratio threshold from 100 to 300 in the `_get_security_thresholds()` method to accommodate legitimate Jira backup files while still protecting against actual compression bomb attacks.

## Change Details

### File Modified
`src/atlassian_cloud_backup/utils/filesystem_discovery.py`

### Before
```python
THRESHOLD_RATIO = 100  # Maximum compression ratio (higher threshold for legitimate backups)
```

### After
```python
THRESHOLD_RATIO = 300  # Maximum compression ratio (increased for legitimate Jira backups with ratios ~143-118)
```

## Impact

### ✅ Benefits
- **Prevents false positives**: Legitimate Jira backup files with ratios 143-118 are no longer rejected
- **Maintains security**: Still protects against actual compression bombs (ratios typically 1000+)
- **Better user experience**: Reduces unnecessary warnings and file rejections
- **Documented reasoning**: Comment explains the real-world ratios observed

### 🛡️ Security Maintained
- **Still detects real threats**: Compression bombs typically have ratios of 1000+ 
- **Multiple layers**: Entry count and total size thresholds provide additional protection
- **Conservative approach**: 300 threshold still catches most malicious patterns
- **Real-world tested**: Based on actual Jira backup file analysis

## Testing
- ✅ All 58 tests continue to pass
- ✅ No regression in functionality
- ✅ Demo scripts updated to reflect new threshold
- ✅ Security validation still effective

## Context
This adjustment was made based on real-world observations of legitimate Jira backup files that were being incorrectly flagged as potential threats. The new threshold of 300 provides a good balance between security and usability.
