# atlassian-cloud-backup
Small python script to automate backup of jira and confluence cloud instances

<img alt="Quality Gate Status" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=alert_status"> <img alt="Maintainability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_rating"> <img alt="Reliability Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=reliability_rating"> <img alt="Security Rating" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=security_rating"> <img alt="Coverage" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=coverage">

<img alt="Lines of Code" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=ncloc"> <img alt="Bugs" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=bugs"> <img alt="Code Smells" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=code_smells"> <img alt="Technical Debt" src="https://sonarcloud.io/api/project_badges/measure?project=timo-jakob_atlassian-cloud-backup&amp;metric=sqale_index">

## ✨ Fresh Backup Strategy

The application implements a **fresh backup approach** that prioritizes new backups over discovering existing files:

- **Automatic Recovery**: When no consolidated backup status file exists, the application starts with an empty status and triggers fresh backups
- **Fresh Backup Strategy**: Always attempts to create new backups without relying on previous backup status discovery
- **Status Tracking**: Maintains a consolidated status file to track successful backup operations across multiple sites
- **Seamless Integration**: Works transparently with existing backup operations

## 🔍 SonarCloud Integration

This project uses SonarCloud for code quality and test coverage analysis. The GitHub Actions workflow automatically sends test coverage data to SonarCloud when tests are run.

### For Contributors

If you're forking this repository and want to use SonarCloud:

1. Go to [SonarCloud](https://sonarcloud.io/) and sign up/login with your GitHub account
2. Add your fork as a new project 
3. Set up a new repository secret in your GitHub repository:
   - Go to your repository → Settings → Secrets and variables → Actions
   - Add a new repository secret with the name `SONAR_TOKEN` and the value of your SonarCloud token

The GitHub Actions workflow will automatically run tests and send coverage data to SonarCloud on each push and pull request.