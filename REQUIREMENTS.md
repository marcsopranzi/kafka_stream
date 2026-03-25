Project Goal: Refactor the existing PySpark/Kafka ingestion pipeline to be production-ready, secure, and scalable.

Actionable Requirements:

    Containerization: Containerize the entire application (producer and consumer) using Docker and docker-compose. The goal is to ensure local reproducibility for debugging production issues.

    API Integration & Security: Update the producer to pull data from a mock external API. Since the current setup assumes the API is on the same network without authentication, implement robust error handling for connection timeouts or dropped packets.

    Schema Evolution & PII Masking: The source API schema is dynamic and will soon include a full_name field and potentially other PII. Update the ingestion logic to handle schema drift. Critically, implement a masking or hashing function for any field identified as PII before it is written downstream.

    Scalability & Throttling: The source API is prone to heavy throttling. Optimize the PySpark consumer script to handle variable throughput gracefully. Prepare the consumer architecture to be deployed and scaled horizontally on Kubernetes.

    Fault Tolerance: Implement Dead Letter Queues (DLQs). Any malformed records or data failing validation must be routed to a separate DLT rather than crashing the pipeline.

    Dependency Management & Security: The current dependencies have high-risk CVEs. Migrate the dependency management to use uv. Update the requirements.txt or Pipfile to secure, modern versions of the required libraries.