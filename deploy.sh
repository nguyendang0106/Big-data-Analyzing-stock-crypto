#!/bin/bash
# deploy.sh - Automated K8s Deployment Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
# REGISTRY="${REGISTRY:-gcr.io/decoded-tribute-474915-u9}"
REGISTRY=docker.io/helloimgnud

NAMESPACE="crypto-pipeline"
K8S_DIR="k8s"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Crypto Big Data Pipeline K8s Deployment${NC}"
echo -e "${GREEN}========================================${NC}"

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Check prerequisites
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

if ! command -v kubectl &> /dev/null; then
    print_error "kubectl not found. Please install kubectl."
    exit 1
fi
print_status "kubectl found"

if ! command -v docker &> /dev/null; then
    print_error "docker not found. Please install docker."
    exit 1
fi
print_status "docker found"

if ! command -v helm &> /dev/null; then
    print_error "helm not found. Please install helm."
    exit 1
fi
print_status "helm found"

# Check cluster connectivity
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster. Please configure kubectl."
    exit 1
fi
print_status "Connected to Kubernetes cluster"

# Create k8s directory structure
mkdir -p ${K8S_DIR}

# Function to create Dockerfiles
create_dockerfiles() {
    echo -e "\n${YELLOW}Creating Dockerfiles...${NC}"
    
    # Producer Dockerfile
    cat > StreamingLayer-Spark-Kafka/producer/Dockerfile <<EOF
FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python websocket-client
COPY binance_producer.py .
CMD ["python", "binance_producer.py"]
EOF
    print_status "Created Producer Dockerfile"
    
    # Spark Streaming Dockerfile
    cat > StreamingLayer-Spark-Kafka/Dockerfile.streaming <<EOF
# Stage 1: Build the JAR using a Maven image with full JDK
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the package (skipping tests to speed up)
RUN mvn clean package -DskipTests

# Stage 2: Create the runtime image
FROM apache/spark:3.5.0
USER root
WORKDIR /app

# Copy the compiled JAR from the builder stage to the final image
COPY --from=builder /app/target/stream-kafka-spark-1-jar-with-dependencies.jar /app/target/stream-kafka-spark-1-jar-with-dependencies.jar

CMD ["/opt/spark/bin/spark-submit", \
     "--class", "tn.insat.tp3.SparkStructuredStreamingCrypto", \
     "--master", "k8s://https://kubernetes.default.svc:443", \
     "--deploy-mode", "client", \
     "--conf", "spark.kubernetes.container.image=docker.io/helloimgnud/spark-streaming:latest", \
     "--conf", "spark.kubernetes.namespace=crypto-pipeline", \
     "/app/target/stream-kafka-spark-1-jar-with-dependencies.jar"]
EOF
    print_status "Created Spark Streaming Dockerfile"
    
    # Spark Batch Dockerfile
    cat > spark/Dockerfile.batch <<EOF
FROM apache/spark:3.5.0-python3
USER root
WORKDIR /opt/spark
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY jars/gcs-connector-hadoop3-latest.jar /opt/spark/jars/
COPY jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar /opt/spark/jars/
COPY code/write_to_big_query.py /opt/spark/work-dir/
COPY start_date.txt /opt/spark/
CMD ["/opt/spark/bin/spark-submit", \\
     "--master", "k8s://https://kubernetes.default.svc:443", \\
     "--deploy-mode", "client", \\
     "--name", "Spark-Batch-ETL", \\
     "--conf", "spark.kubernetes.container.image=${REGISTRY}/spark-batch:latest", \\
     "--conf", "spark.kubernetes.namespace=${NAMESPACE}", \\
     "--conf", "spark.executor.instances=3", \\
     "--conf", "spark.executor.memory=2g", \\
     "--conf", "spark.executor.cores=2", \\
     "--jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar", \\
     "/opt/spark/work-dir/write_to_big_query.py"]
EOF
    print_status "Created Spark Batch Dockerfile"
    
    # FastAPI Dockerfile
    cat > StreamingLayer-Spark-Kafka/Dockerfile.fastapi <<EOF
FROM python:3.11-slim
WORKDIR /app
RUN pip install fastapi uvicorn pymongo python-dotenv
COPY server.py .
EXPOSE 8000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
EOF
    print_status "Created FastAPI Dockerfile"
}

# Function to build and push images
build_and_push() {
    echo -e "\n${YELLOW}Building and pushing Docker images...${NC}"
    
    # Build Producer
    echo "Building binance-producer..."
    docker build -t ${REGISTRY}/binance-producer:latest StreamingLayer-Spark-Kafka/producer/
    docker push ${REGISTRY}/binance-producer:latest
    print_status "Built and pushed binance-producer"
    
    # Build Spark Streaming
    echo "Building spark-streaming..."
    docker build -f StreamingLayer-Spark-Kafka/Dockerfile.streaming -t ${REGISTRY}/spark-streaming:latest StreamingLayer-Spark-Kafka/
    docker push ${REGISTRY}/spark-streaming:latest
    print_status "Built and pushed spark-streaming"
    
    # Build Spark Batch
    echo "Building spark-batch..."
    docker build -f spark/Dockerfile.batch -t ${REGISTRY}/spark-batch:latest spark/
    docker push ${REGISTRY}/spark-batch:latest
    print_status "Built and pushed spark-batch"
    
    # Build Airflow
    echo "Building airflow-custom..."
    docker build -t ${REGISTRY}/airflow-custom:latest airflow/
    docker push ${REGISTRY}/airflow-custom:latest
    print_status "Built and pushed airflow-custom"
    
    # Build FastAPI
    echo "Building fastapi-crypto..."
    docker build -f StreamingLayer-Spark-Kafka/Dockerfile.fastapi -t ${REGISTRY}/fastapi-crypto:latest StreamingLayer-Spark-Kafka/
    docker push ${REGISTRY}/fastapi-crypto:latest
    print_status "Built and pushed fastapi-crypto"
}

# Function to update K8s manifests with registry
update_manifests() {
    echo -e "\n${YELLOW}Updating K8s manifests with registry...${NC}"
    
    # Update all YAML files with actual registry
    find ${K8S_DIR} -name "*.yaml" -type f -exec sed -i "s|<your-registry>|${REGISTRY}|g" {} \;
    print_status "Updated manifests with registry: ${REGISTRY}"
}

# Function to deploy to K8s
deploy_to_k8s() {
    echo -e "\n${YELLOW}Deploying to Kubernetes...${NC}"
    
    # Create namespace
    kubectl apply -f ${K8S_DIR}/namespace.yaml
    print_status "Created namespace"
    
    # Apply secrets (user should edit this file first)
    if [ -f ${K8S_DIR}/secrets.yaml ]; then
        print_warning "Make sure you've updated secrets.yaml with actual base64-encoded values!"
        read -p "Press enter to continue..."
        kubectl apply -f ${K8S_DIR}/secrets.yaml
        print_status "Applied secrets"
    else
        print_error "secrets.yaml not found. Please create it first."
        exit 1
    fi
    
    # Deploy Kafka infrastructure
    kubectl apply -f ${K8S_DIR}/kafka-zookeeper.yaml
    print_status "Deployed Kafka and Zookeeper"
    
    echo "Waiting for Kafka to be ready..."
    kubectl wait --for=condition=ready pod -l app=kafka -n ${NAMESPACE} --timeout=300s || true
    print_status "Kafka is ready"
    
    # Deploy Producer
    kubectl apply -f ${K8S_DIR}/binance-producer.yaml
    print_status "Deployed Binance Producer"
    
    # Deploy Spark Streaming
    kubectl apply -f ${K8S_DIR}/spark-streaming.yaml
    print_status "Deployed Spark Streaming"
    
    # Deploy FastAPI
    kubectl apply -f ${K8S_DIR}/fastapi-service.yaml
    print_status "Deployed FastAPI"
    
    # Deploy Grafana
    kubectl apply -f ${K8S_DIR}/grafana.yaml
    print_status "Deployed Grafana"
    
    # Deploy Spark Batch CronJob
    kubectl apply -f ${K8S_DIR}/spark-batch-job.yaml
    print_status "Deployed Spark Batch Job"
    
    # Deploy Airflow with Helm
    echo "Deploying Airflow with Helm..."
    helm repo add apache-airflow https://airflow.apache.org
    helm repo update
    
    # Update airflow-values.yaml with registry
    sed -i "s|<your-registry>|${REGISTRY}|g" ${K8S_DIR}/airflow-values.yaml
    
    helm upgrade --install airflow apache-airflow/airflow \
        -f ${K8S_DIR}/airflow-values.yaml \
        -n ${NAMESPACE} \
        --wait
    print_status "Deployed Airflow"
}

# Function to check deployment status
check_status() {
    echo -e "\n${YELLOW}Checking deployment status...${NC}"
    
    echo -e "\n${GREEN}Pods:${NC}"
    kubectl get pods -n ${NAMESPACE}
    
    echo -e "\n${GREEN}Services:${NC}"
    kubectl get svc -n ${NAMESPACE}
    
    echo -e "\n${GREEN}StatefulSets:${NC}"
    kubectl get statefulsets -n ${NAMESPACE}
    
    echo -e "\n${GREEN}Jobs:${NC}"
    kubectl get jobs -n ${NAMESPACE}
    
    echo -e "\n${GREEN}CronJobs:${NC}"
    kubectl get cronjobs -n ${NAMESPACE}
}

# Function to print access information
print_access_info() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Deployment Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    echo -e "\nTo access services, get their external IPs:"
    echo -e "  kubectl get svc -n ${NAMESPACE}"
    
    echo -e "\nOr use port forwarding:"
    echo -e "  kubectl port-forward svc/airflow-webserver 8080:8080 -n ${NAMESPACE}"
    echo -e "  kubectl port-forward svc/grafana-service 3000:3000 -n ${NAMESPACE}"
    echo -e "  kubectl port-forward svc/fastapi-service 8000:80 -n ${NAMESPACE}"
    
    echo -e "\nDefault credentials:"
    echo -e "  Airflow: admin/admin"
    echo -e "  Grafana: admin/admin"
    
    echo -e "\nTo view logs:"
    echo -e "  kubectl logs -f deployment/binance-producer -n ${NAMESPACE}"
    echo -e "  kubectl logs -f deployment/fastapi -n ${NAMESPACE}"
    
    echo -e "\nTo scale services:"
    echo -e "  kubectl scale deployment fastapi --replicas=3 -n ${NAMESPACE}"
    
    echo -e "\n${YELLOW}Don't forget to:${NC}"
    echo -e "1. Configure Grafana datasource to point to FastAPI"
    echo -e "2. Import Grafana dashboards"
    echo -e "3. Trigger Airflow DAG manually for initial run"
    echo -e "4. Monitor resource usage and adjust limits as needed"
}

# Main execution
main() {
    echo "Registry: ${REGISTRY}"
    echo "Namespace: ${NAMESPACE}"
    echo ""
    read -p "Continue with deployment? (y/n) " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Deployment cancelled."
        exit 0
    fi
    
    # Execute deployment steps
    create_dockerfiles
    
    read -p "Build and push Docker images? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_and_push
    fi
    
    update_manifests
    
    read -p "Deploy to Kubernetes? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        deploy_to_k8s
        sleep 5
        check_status
        print_access_info
    fi
}

# Run main
main