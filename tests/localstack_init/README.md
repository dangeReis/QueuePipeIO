# LocalStack Initialization Scripts

This directory contains initialization scripts that are automatically executed when LocalStack starts up.

## Scripts

- `01-create-buckets.sh` - Creates test S3 buckets for integration testing

## Usage

Scripts in this directory are automatically executed by LocalStack in alphabetical order when the container starts. They use the `awslocal` CLI which is pre-configured to connect to LocalStack services.

## Adding New Scripts

To add a new initialization script:
1. Create a new `.sh` file with a numeric prefix (e.g., `02-setup-data.sh`)
2. Make it executable: `chmod +x your-script.sh`
3. Use `awslocal` commands instead of `aws` commands