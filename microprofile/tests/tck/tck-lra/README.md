# Running the LRA TCKs

1. Run the coordinator from the `lra` directory and make sure it runs on port 8070
```java -Dlra.logging.enabled=false -jar coordinator/target/lra-coordinator-helidon-2.3.0-SNAPSHOT.jar```

4. Run the TCKs by typing ```mvn test```