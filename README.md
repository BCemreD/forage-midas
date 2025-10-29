# Midas
Project repo for the JPMC Advanced Software Engineering Forage program
## Key Features
- Architected the data flow by implementing a Kafka Producer for robust data ingestion and a Kafka Listener for asynchronous consumption, ensuring resilience against invalid data formats (e.g., handling newline characters).
- Designed and implemented the transaction processing logic within the Consumer component, utilizing Spring Data JPA and an H2 in-memory database for user balance updates and detailed transaction recording.
- Integrated an external Incentive API via RestTemplate POST requests to apply incentives after transaction validation, demonstrating proficiency in decoupled microservice communication and contract-driven development.
- Developed a new REST API endpoint (/balance) to securely expose user balance data in JSON format, demonstrating core skills in extending a service with robust, queryable interfaces on a non-default port (33400).
- Diagnosed and resolved critical NumberFormatException errors at the data source level by implementing defensive data cleaning (.trim()) in the Producer, ensuring data integrity before message queuing.
