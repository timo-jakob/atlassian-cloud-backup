# Atlassian Cloud Backup

A rock-solid backup solution for Atlassian Cloud services (Jira and Confluence)

<img alt="Quality Gate Status" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=alert_status"> <img alt="Maintainability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_rating"> <img alt="Reliability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=reliability_rating"> <img alt="Security Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=security_rating"> <img alt="Coverage" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=coverage">

<img alt="Lines of Code" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=ncloc"> <img alt="Bugs" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=bugs"> <img alt="Code Smells" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=code_smells"> <img alt="Technical Debt" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_index">

## 💪 Rock-Solid Backup Solution

This application is designed to be an extremely reliable backup solution for **Atlassian Cloud services** (Jira and Confluence). It's built with robustness as the primary goal to handle challenging scenarios:

- **Network Resilience**: Implements retry mechanisms with exponentially increasing wait times for handling network interruptions
- **Download Resumption**: Uses HTTP partial content requests to resume interrupted downloads where they left off
- **Data Integrity**: Verifies backup ZIP files to ensure they aren't corrupted and contain valid data
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

No Docker or other container environments are needed. The application is designed to be easily used from the terminal. When run for the first time, it will interactively guide you through configuration, requesting and securely storing required credentials.

## ✨ Fresh Backup Strategy

The application implements a **fresh backup approach** that prioritizes new backups over discovering existing files:

- **Automatic Recovery**: When no consolidated backup status file exists, the application starts with an empty status and triggers fresh backups
- **Fresh Backup Strategy**: Always attempts to create new backups without relying on previous backup status discovery
- **Status Tracking**: Maintains a consolidated status file to track successful backup operations across multiple sites
- **Seamless Integration**: Works transparently with existing backup operations

## ⚙️ Smart Backup Management

The application is designed to work intelligently with Atlassian's backup frequency limitations:

- **Frequency Limit Handling**: Respects Atlassian's backup frequency limits while maximizing backup freshness
- **Clear Audit Information**: Provides detailed audit logs explaining each backup decision and outcome
- **Most Efficient Strategy**: If Atlassian blocks triggering a new backup but has a newer server backup available, the application automatically downloads the newer version
- **Backup Verification**: Every downloaded backup is thoroughly verified to ensure data integrity

## 🌐 Multi-Instance Support

Currently, the application supports:
- Backup of multiple Atlassian Cloud instances
- Single user authentication per execution (multi-user support planned for future versions)

## 🔮 Vision

The primary goal is to provide the most up-to-date backups possible within Atlassian's limits, with maximum efficiency and reliability. By intelligently working with the platform's limitations and providing clear feedback, the application ensures you always have the freshest possible backups of your valuable Atlassian data.