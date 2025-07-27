# Atlassian Cloud Backup

A rock-solid backup solution for Atlassian Cloud services (Jira and Confluence)

<img alt="Quality Gate Status" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=alert_status"> <img alt="Maintainability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_rating"> <img alt="Reliability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=reliability_rating"> <img alt="Security Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=security_rating"> <img alt="Coverage" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=coverage">

<img alt="Lines of Code" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=ncloc"> <img alt="Bugs" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=bugs"> <img alt="Code Smells" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=code_smells"> <img alt="Technical Debt" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_index">

## 💪 Rock-Solid Backup Solution

This application is designed to be an extremely reliable backup solution for **Atlassian Cloud services** (Jira and Confluence). It's built with robustness as the primary goal to handle challenging scenarios:

- **Network Resilience**: Implements retry mechanisms with exponentially increasing wait times for handling network interruptions
- **Download Resumption**: Uses HTTP partial content requests to resume interrupted downloads where they left off
- **Intelligent Disk Space Management**: Automatically monitors disk space during downloads and removes old backup files when space runs low, ensuring downloads never fail due to insufficient storage
- **Fail-Safe Operation**: Always times out after 1 hour in non-interactive mode (e.g., cron jobs) to ensure system responsiveness
- **Error Recovery**: Gracefully handles and logs errors, maintaining audit trails of all operations
- **Missing Configuration**: Guides users through setup or exits cleanly in automated environments

> ⚠️ **Note**: This application supports Atlassian Cloud services only. On-premise Atlassian installations are not supported.

## 🚀 Easy Installation & Usage

The application intentionally has minimal dependencies, requiring only Python and the packages specified in `requirements.txt`:

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the application
python src/main.py
```

No Docker or other container environments are needed. The application is designed to be easily used from very basic setups. When run for the first time, it will interactively guide you through configuration, requesting and then storing required credentials in the user's home directory.

## ⚙️ Configuration Options

The application supports configuration through environment variables and a properties file located at `~/.atlassian-cloud-backup/backup.properties`. Environment variables take precedence over properties file values.

### Required Configuration

| Environment Variable | Properties File Key | Description |
|---------------------|-------------------|-------------|
| `ATLASSIAN_INSTANCES` | `instances` | Comma-separated list of instance names (e.g., "company1,company2") |
| `ATLASSIAN_USERNAME` | `username` | Username for Atlassian authentication |
| `ATLASSIAN_API_TOKEN` | `api_token` | API token for Atlassian authentication |

### Optional Configuration

| Environment Variable | Properties File Key | Default | Description |
|---------------------|-------------------|---------|-------------|
| `POLL_INTERVAL_SECONDS` | `poll_interval_seconds` | `30` | Seconds to wait between API polling requests |
| `BACKUP_TARGET_DIRECTORY` | `backup_target_directory` | Current directory | Base directory where backup files will be stored |
| `DELETION_STRATEGY` | `deletion_strategy` | `oldest_first` | Strategy for managing disk space when storage is low |

### Example Configuration

**Environment Variables:**
```bash
export ATLASSIAN_INSTANCES="mycompany,testorg"
export ATLASSIAN_USERNAME="user@example.com"
export ATLASSIAN_API_TOKEN="your-api-token-here"
export BACKUP_TARGET_DIRECTORY="/backups/atlassian"
export DELETION_STRATEGY="retention_ladder"
```

**Properties File** (`~/.atlassian-cloud-backup/backup.properties`):
```properties
instances=mycompany,testorg
username=user@example.com
api_token=your-api-token-here
backup_target_directory=/backups/atlassian
deletion_strategy=retention_ladder
```

## ✨ Fresh Backup Strategy

The application implements a **fresh backup approach** that prioritizes new backups over discovering existing files:

- **Fresh Backup Strategy**: Always attempts to create new backups
- **Status Tracking**: Uses metadata from filenames to track successful backup operations. No dependency on status files.

## ⚙️ Smart Backup Management

The application is designed to work intelligently with Atlassian's backup frequency limitations:

- **Frequency Limit Handling**: Respects Atlassian's backup frequency limits while maximizing backup freshness
- **Clear Audit Information**: Provides detailed audit logs explaining each backup decision and outcome
- **Most Efficient Strategy**: If Atlassian blocks triggering a new backup but has a newer backup available, this one is used instead.
- **Backup Verification**: Every downloaded backup is thoroughly verified to ensure data integrity

## 💾 Intelligent Disk Space Management

The application includes advanced disk space management to ensure reliable operation even in storage-constrained environments:

- **Proactive Space Monitoring**: Continuously monitors available disk space during downloads, checking before each data chunk
- **Smart Cleanup Strategy**: Automatically removes old backup files when space runs low, using configurable deletion strategies
- **Safety Buffer**: Maintains a 10x chunk size buffer (typically ~80KB) to prevent mid-download failures
- **Type-Aware Deletion**: Intelligently identifies backup types (Jira/Confluence) and only deletes files of the same type as the current download
- **Configurable Strategies**: Choose between different deletion strategies to match your backup retention needs
- **Fail-Safe Limits**: Prevents infinite deletion loops with built-in safety limits
- **Detailed Logging**: Provides comprehensive logging of all space management activities

> 💡 **Smart Feature**: The system will never delete backup files unless absolutely necessary, and when it does, it intelligently preserves your most recent backups while making room for new ones.

### 🗂️ Deletion Strategy Configuration

The application supports configurable deletion strategies for managing disk space when storage runs low. You can specify which strategy to use through environment variables or configuration files.

#### Configuration Methods

**Environment Variable:**
```bash
export DELETION_STRATEGY="oldest_first"
```

**Configuration File** (`~/.atlassian-cloud-backup/backup.properties`):
```properties
deletion_strategy=oldest_first
```

**Command Line** (environment variable takes precedence):
```bash
DELETION_STRATEGY="retention_ladder" python src/main.py
```

#### Available Strategies

**`oldest_first` (Default)**
- Deletes the oldest backup files first when space is needed
- Simple and predictable - always preserves the most recent backups
- Best for: Regular backup schedules where you want to keep recent files
- Example: If you run daily backups, this keeps your most recent days and removes older ones

**`retention_ladder`**
- Implements a sophisticated retention ladder strategy
- Keeps more recent backups and progressively fewer older backups
- Maintains strategic retention points (daily recent, weekly older, monthly ancient)
- Best for: Long-term backup retention with intelligent space usage
- Example: Keeps daily backups for recent weeks, weekly for months, monthly for years

#### Strategy Selection Guide

Choose `oldest_first` if you:
- Want simple, predictable behavior
- Run regular backups (daily/weekly)
- Primarily care about recent backup history
- Prefer straightforward space management

Choose `retention_ladder` if you:
- Need long-term backup retention
- Want to optimize storage efficiency
- Require strategic historical backup points
- Have varying backup frequency needs

#### Default Behavior

If no deletion strategy is configured, the system defaults to `oldest_first` for reliable, predictable behavior that works well for most use cases.

## 🌐 Multi-Instance Support

Currently, the application supports:
- Backup of multiple Atlassian Cloud instances
- One user authentication (multi-user support planned for future versions)

## 🔮 Vision

The primary goal is to provide the most up-to-date backups possible within Atlassian's limits, with maximum efficiency and reliability. By intelligently working with the platform's limitations, managing storage resources automatically, and providing clear feedback, the application ensures you always have the freshest possible backups of your valuable Atlassian data without worrying about storage constraints.

### 🎯 Quality Standards

As the owner of this repository, maintaining high quality standards is essential to me:

- **Clean SonarCloud Ratings**: All aspects including Security, Reliability, and Maintainability must maintain top ratings
- **Zero Security Hotspots**: All security hotspots are addressed promptly
- **No Vulnerabilities**: Ensuring no vulnerabilities are detected by Snyk security scanning
- **High Test Coverage**: Maintaining comprehensive test coverage across all components