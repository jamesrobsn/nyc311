#!/bin/bash

# NYC 311 Databricks Asset Bundle Deployment Script

set -e

# Load environment variables from .env file if it exists
load_env_file() {
    if [[ -f ".env" ]]; then
        echo "Loading environment variables from .env file..."
        export $(grep -v '^#' .env | grep -v '^$' | xargs)
    fi
}

# Load .env file first
load_env_file

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

# Check if required environment variables are set
check_env_vars() {
    print_status "Checking environment variables..."
    
    required_vars=(
        "DATABRICKS_HOST"
        "DATABRICKS_TOKEN"
    )
    
    missing_vars=()
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "Please set these variables either:"
        echo "1. In a .env file (recommended):"
        echo "   DATABRICKS_HOST=https://your-workspace.databricks.com"
        echo "   DATABRICKS_TOKEN=your-access-token"
        echo ""
        echo "2. Or as environment variables:"
        echo "   export DATABRICKS_HOST='https://your-workspace.databricks.com'"
        echo "   export DATABRICKS_TOKEN='your-access-token'"
        exit 1
    fi
    
    print_status "Environment variables are set correctly"
}

# Check if Databricks CLI is installed and is the new version
check_databricks_cli() {
    print_status "Checking Databricks CLI installation..."
    
    if ! command -v databricks &> /dev/null; then
        print_error "Databricks CLI is not installed"
        echo ""
        echo "Install the new Databricks CLI from:"
        echo "https://docs.databricks.com/en/dev-tools/cli/install.html"
        echo ""
        echo "Quick install options:"
        echo "1. Using curl (Linux/macOS):"
        echo "   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        echo ""
        echo "2. Using Homebrew (macOS):"
        echo "   brew tap databricks/tap && brew install databricks"
        echo ""
        echo "3. Download from GitHub releases:"
        echo "   https://github.com/databricks/cli/releases"
        exit 1
    fi
    
    # Check if bundle command is available (new CLI)
    if ! databricks bundle --help &> /dev/null; then
        print_error "You are using the legacy Databricks CLI which doesn't support bundles"
        echo ""
        echo "Please install the new Databricks CLI from:"
        echo "https://docs.databricks.com/en/dev-tools/cli/install.html"
        echo ""
        echo "Quick install (will replace current installation):"
        echo "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        exit 1
    fi
    
    print_status "Databricks CLI (new version) is installed"
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
    
    print_status "Deploying to $environment environment..."
    
    # Deploy the bundle
    databricks bundle deploy --target "$environment"
    
    print_status "Bundle deployed successfully to $environment"
}

# Run the jobs (optional)
run_jobs() {
    local environment=${1:-dev}
    
    print_warning "Do you want to run the NYC 311 jobs now? (y/n)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_status "Running NYC 311 Bronze Ingest job..."
        databricks bundle run nyc311_bronze_ingest_job --target "$environment"
        
        print_status "Waiting 5 minutes before running Silver Transform job..."
        sleep 300
        
        print_status "Running NYC 311 Silver Transform job..."
        databricks bundle run nyc311_silver_transform_job --target "$environment"
        
        print_status "Waiting 5 minutes before running Gold Star Schema job..."
        sleep 300
        
        print_status "Running NYC 311 Gold Star Schema job..."
        databricks bundle run nyc311_gold_star_schema_job --target "$environment"
        
        print_status "All jobs completed successfully"
    else
        print_status "Skipping job execution. You can run them manually later."
    fi
}

# Main deployment function
main() {
    local environment=${1:-dev}
    
    echo "======================================"
    echo "NYC 311 Databricks Bundle Deployment"
    echo "======================================"
    echo ""
    
    print_status "Starting deployment to $environment environment"
    
    # Pre-deployment checks
    check_env_vars
    check_databricks_cli
    validate_bundle
    
    # Deploy the bundle
    deploy_bundle "$environment"
    
    # Optionally run jobs
    run_jobs "$environment"
    
    echo ""
    print_status "Deployment completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Check the Databricks workspace for deployed jobs"
    echo "2. Configure any required secrets in Databricks Secret Scopes"
    echo "3. Monitor job execution in the Databricks UI"
    echo "4. Access the gold layer tables for analytics and Power BI"
    echo ""
    echo "Happy analyzing! 🎉"
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
    echo "Required environment variables:"
    echo "  DATABRICKS_HOST    Databricks workspace URL"
    echo "  DATABRICKS_TOKEN   Databricks access token"
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
