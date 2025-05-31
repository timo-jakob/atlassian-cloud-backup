# atlassian-cloud-backup
Small python script to automate backup of jira and confluence cloud instances

<img alt="Quality Gate Status" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=alert_status"> <img alt="Maintainability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_rating"> <img alt="Reliability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=reliability_rating"> <img alt="Security Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=security_rating"> <img alt="Coverage" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=coverage">

<img alt="Lines of Code" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=ncloc"> <img alt="Bugs" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=bugs"> <img alt="Code Smells" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=code_smells"> <img alt="Technical Debt" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_index">

## ✨ New: Filesystem Discovery Feature

The application now includes **filesystem discovery** functionality that makes it resilient and self-learning:

- **Automatic Recovery**: When no consolidated backup status file exists, the application scans the target directory for existing backup files and reconstructs the status
- **Site Detection**: Identifies sites from folder names and detects which services (Jira/Confluence) were backed up
- **Smart Parsing**: Extracts timestamps from backup filenames (using 00:00:00 as default time since only dates are in filenames)
- **Seamless Integration**: Works transparently with existing backup operations

For detailed information, see [FILESYSTEM_DISCOVERY.md](FILESYSTEM_DISCOVERY.md).