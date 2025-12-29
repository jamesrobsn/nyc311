#!/bin/bash

# NYC 311 Databricks Asset Bundle Deployment Script

set -e

# Load environment variables from .env file if it exists
load_env_file() {
    if [[ -f ".env" ]]; then
        echo "Loading environment variables from .env file..."
        set -a
        source .env 2>/dev/null || true
        set +a
    fi
}

# Load .env file first
load_env_file

# Convert .env variables to BUNDLE_VAR_ format for Databricks
setup_bundle_vars() {
    # Convert NOTIFICATION_EMAIL to BUNDLE_VAR_notification_email if set
    if [[ -n "${NOTIFICATION_EMAIL:-}" ]]; then
        export BUNDLE_VAR_notification_email="${NOTIFICATION_EMAIL}"
        print_status "Set BUNDLE_VAR_notification_email from .env file" 2>/dev/null || true
    fi
    
    # DATABRICKS_HOST is used directly (no conversion needed)
    # Databricks CLI automatically picks it up from environment
}

# Setup bundle variables
setup_bundle_vars

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required environment variables are set (optional - profiles preferred)
check_env_vars() {
    print_status "Checking for optional environment variables..."
    
    # Check for workspace host (used by Databricks CLI)
    if [[ -n "${DATABRICKS_HOST:-}" ]]; then
        print_status "Workspace host configured: ${DATABRICKS_HOST}"
    else
        print_warning "DATABRICKS_HOST not set - will use profile configuration"
    fi
    
    # Check for bundle variables
    if [[ -n "${BUNDLE_VAR_notification_email:-}" ]]; then
        print_status "Email notifications enabled: ${BUNDLE_VAR_notification_email}"
    else
        print_warning "No notification email set - email notifications will be skipped"
        print_warning "To enable: add NOTIFICATION_EMAIL to .env file"
    fi
    
    # Environment variables are now optional since we use profiles
    if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
        print_status "Databricks token set (will be used alongside profiles)"
    else
        print_status "Using profile-based authentication (recommended)"
    fi
}

# Check CLI version meets minimum requirement
check_cli_version() {
    local version=$(databricks --version 2>&1 | grep -oP 'Databricks CLI v\K[0-9.]+' || echo "0.0.0")
    local min_version="0.205.0"
    
    if [[ "$(printf '%s\n' "$min_version" "$version" | sort -V | head -n1)" != "$min_version" ]]; then
        print_warning "Databricks CLI version $version detected. Recommended: $min_version+"
        print_warning "Some features may not work correctly with older versions"
    else
        print_status "Databricks CLI version $version (meets minimum requirement)"
    fi
}

# Check if Databricks CLI is installed and authentication is configured
check_databricks_cli() {
    print_status "Checking Databricks CLI installation and authentication..."
    
    if ! command -v databricks &> /dev/null; then
        print_error "Databricks CLI is not installed"
        echo ""
        echo "Install the new Databricks CLI:"
        echo "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        echo ""
        echo "Then authenticate:"
        echo "databricks auth login"
        exit 1
    fi
    
    # Check CLI version
    check_cli_version
    
    # Check if bundle command is available (new CLI)
    if ! databricks bundle --help &> /dev/null; then
        print_error "You are using the legacy Databricks CLI which doesn't support bundles"
        echo ""
        echo "Please install the new Databricks CLI:"
        echo "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        exit 1
    fi
    
    # Check if user is authenticated
    if ! databricks auth profiles &> /dev/null; then
        print_error "No authentication profiles found"
        echo ""
        echo "Please authenticate first:"
        echo "databricks auth login"
        echo ""
        echo "Then verify with:"
        echo "databricks auth profiles"
        exit 1
    fi
    
    # Verify workspace access
    print_status "Verifying workspace access..."
    if ! databricks workspace list / &> /dev/null; then
        print_error "Cannot access workspace. Authentication may have expired."
        echo ""
        echo "Please re-authenticate:"
        echo "databricks auth login"
        exit 1
    fi
    
    print_status "Databricks CLI is installed and authenticated successfully"
}

# Validate bundle configuration
validate_bundle() {
    print_status "Validating bundle configuration..."
    
    if [[ ! -f "databricks.yml" ]]; then
        print_error "databricks.yml not found in current directory"
        exit 1
    fi
    
    databricks bundle validate
    print_status "Bundle configuration is valid"
}

# Deploy to specified environment
deploy_bundle() {
    local environment=${1:-dev}
    
    # Production safety check
    if [[ "$environment" == "prod" ]]; then
        print_warning "WARNING: You are about to deploy to PRODUCTION"
        print_warning "This will update production pipelines and may affect live data"
        echo ""
        print_warning "Continue with production deployment? (yes/no)"
        read -r -t 30 response || response="no"
        
        if [[ ! "$response" =~ ^[Yy][Ee][Ss]$ ]]; then
            print_status "Production deployment cancelled"
            exit 0
        fi
    fi
    
    print_status "Deploying to $environment environment..."
    
    # Deploy the bundle
    databricks bundle deploy --target "$environment"
    
    print_status "Bundle deployed successfully to $environment"
}

# Run the pipeline (optional)
run_pipeline() {
    local environment=${1:-dev}
    
    # Check if running in non-interactive mode (CI/CD)
    if [[ ! -t 0 ]]; then
        print_status "Non-interactive mode detected. Skipping pipeline execution."
        print_status "Run manually with: databricks bundle run nyc311_pipeline --target $environment"
        return 0
    fi
    
    print_warning "Do you want to run the NYC 311 pipeline now? (y/n)"
    read -r -t 30 response || response="n"
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_status "Running NYC 311 Data Pipeline (all three tasks will execute sequentially)..."
        databricks bundle run nyc311_pipeline --target "$environment"
        
        print_status "Pipeline completed successfully!"
        echo ""
        echo "The pipeline executed these tasks in sequence:"
        echo "1. Bronze Ingest - Raw data from NYC 311 API"
        echo "2. Silver Transform - Data cleaning and standardization"
        echo "3. Gold Star Schema - Analytics-ready tables"
    else
        print_status "Skipping pipeline execution. You can run it manually later with:"
        echo "databricks bundle run nyc311_pipeline --target $environment"
    fi
}

# Main deployment function
main() {
    local environment=${1:-dev}
    
    # Set profile to match target environment
    export DATABRICKS_CONFIG_PROFILE="$environment"
    
    echo "======================================"
    echo "NYC 311 Databricks Bundle Deployment"
    echo "======================================"
    echo ""
    
    print_status "Starting deployment to $environment environment"
    print_status "Using Databricks profile: $DATABRICKS_CONFIG_PROFILE"
    
    # Pre-deployment checks
    check_env_vars
    check_databricks_cli
    validate_bundle
    
    # Deploy the bundle
    deploy_bundle "$environment"
    
    # Optionally run pipeline
    run_pipeline "$environment"
    
    echo ""
    print_status "Deployment completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Check the Databricks workspace for deployed pipeline job"
    echo "2. Monitor pipeline execution in the Databricks UI"
    echo "3. Access the gold layer tables for analytics and Power BI"
    echo "4. Connect Power BI to gold.nyc311 schema for reporting"
    echo ""
    echo "Happy analyzing!"
}

# Help function
show_help() {
    echo "Usage: $0 [ENVIRONMENT]"
    echo ""
    echo "Deploy the NYC 311 Databricks Asset Bundle"
    echo ""
    echo "Arguments:"
    echo "  ENVIRONMENT    Target environment (dev or prod, defaults to dev)"
    echo ""
    echo "Examples:"
    echo "  $0              # Deploy to dev environment"
    echo "  $0 dev          # Deploy to dev environment"
    echo "  $0 prod         # Deploy to prod environment"
    echo ""
    echo "Authentication:"
    echo "  Requires Databricks CLI with profile-based authentication"
    echo "  Run 'databricks auth login' if not already authenticated"
}

# Parse command line arguments
case "${1:-}" in
    -h|--help)
        show_help
        exit 0
        ;;
    dev|prod|"")
        main "${1:-dev}"
        ;;
    *)
        print_error "Invalid environment: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
