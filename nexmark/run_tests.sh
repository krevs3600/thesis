
# global variables
REDPANDA_COMPOSE="./docker-compose.yaml"
EVENT_GENERATOR="./kafka_event_gen/target/release/kafka_event_gen"
BACKENDS=("flink" "renoir" "risingwave")


# Function: Check if Docker is installed
check_docker_installed() {
    if ! command -v docker &>/dev/null; then
        echo "Docker is not installed. Please install Docker and try again."
        exit 1
    fi
}

# Function: Check if Docker is running
check_docker_running() {
    if ! systemctl is-active --quiet docker; then
        echo "Docker is not running. Attempting to start Docker..."
        if sudo systemctl start docker; then
            echo "Docker started successfully."
        else
            echo "Failed to start Docker. Please start Docker manually and try again."
            exit 1
        fi
    else
        echo "Docker is running."
    fi
}

start_redpanda() {
    echo "Starting Redpanda with Docker Compose..."
    docker compose -f $REDPANDA_COMPOSE up -d
}

# Main script logic
main() {
    check_docker_installed
    check_docker_running
    start_redpanda
    echo "docker and kafka started succesfully"
}

main