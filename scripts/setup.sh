#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status messages
print_status() {
    echo -e "${GREEN}[✓] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[!] $1${NC}"
}

print_error() {
    echo -e "${RED}[✗] $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    print_status "All prerequisites satisfied"
}

# Create virtual environment
setup_venv() {
    print_status "Setting up Python virtual environment..."
    
    python3 -m venv venv
    source venv/bin/activate
    
    print_status "Installing dependencies..."
    pip install -r requirements.txt
}

# Configure environment
setup_env() {
    print_status "Configuring environment..."
    
    # Create .env file
    cat > .env << EOL
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ship_telemetry
DUCKDB_PATH=ship_telemetry.db
EXPORT_PATH=./export
CHECKPOINT_PATH=./checkpoints
EOL
    
    # Create required directories
    mkdir -p export/raw_ship_snapshot checkpoints
}

# Start services
start_services() {
    print_status "Starting services..."
    
    # Start Docker services
    docker-compose up -d
    
    # Wait for services to be ready
    print_status "Waiting for services to start..."
    sleep 30
}

# Generate test data
generate_data() {
    print_status "Generating test data..."
    
    # Activate virtual environment if not already activated
    if [ -z "$VIRTUAL_ENV" ]; then
        source venv/bin/activate
    fi
    
    python simulate_sensors.py
}

# Main setup process
main() {
    print_status "Starting Ship Telemetry Monitoring System setup..."
    
    # Check prerequisites
    check_prerequisites
    
    # Setup virtual environment
    setup_venv
    
    # Configure environment
    setup_env
    
    # Start services
    start_services
    
    # Generate test data
    generate_data
    
    print_status "Setup completed successfully!"
    print_status "To start the dashboard, run: streamlit run monitoring_dashboard.py"
}

# Run main function
main 