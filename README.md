# GCP-BigTable-Weather

# Bigtable Sensor Data Assignment

This project connects to Google Cloud Bigtable, loads weather sensor data from CSV files, and performs various queries.

## ✅ Project Structure

```
BigtableAssignment/
├── bin/
│   └── data/
│       ├── seatac.csv
│       ├── vancouver.csv
│       └── portland.csv
├── pom.xml
├── src/
│   └── main/
│       └── java/
│           └── Bigtable.java
└── README.md
```

## ✅ Prerequisites

- Java 11 or later (Recommended Java 17+)
- Maven installed and added to PATH
- Google Cloud SDK installed and initialized
- Bigtable instance created in Google Cloud
- Required IAM permissions:
  - `roles/bigtable.admin` for your account or service account

## ✅ Setup Instructions

### 1. Authenticate with Google Cloud

```powershell
gcloud auth login
gcloud auth application-default login
gcloud config set project <your-project-id>
gcloud bigtable instances list  # Verify instance exists
gcloud auth list                 # Verify active account
```

### 2. Set JAVA_HOME and Maven in PATH (if not done)

Example for Java 17:

```powershell
setx JAVA_HOME "C:\Program Files\Java\jdk-17"
setx PATH "%JAVA_HOME%\bin;%PATH%"
```

Example for Maven:

```powershell
setx PATH "C:\apache-maven-3.9.x\bin;%PATH%"
```

### 3. Install Project Dependencies and Compile

```powershell
cd <path-to-BigtableAssignment>
mvn clean compile
```

### 4. Run the Project

```powershell
mvn exec:java
```

## ✅ Helpful Google Cloud Commands

Grant Bigtable permissions:

```powershell
gcloud projects add-iam-policy-binding <project-id> \
  --member="user:your-email@gmail.com" \
  --role="roles/bigtable.admin"
```

Check active credentials:

```powershell
gcloud auth list
type %APPDATA%\gcloud\application_default_credentials.json
```

List Bigtable instances:

```powershell
gcloud bigtable instances list
```

## ✅ Notes

- The Bigtable instance ID and project ID are configured inside `Bigtable.java`. Modify them as per your setup.
- The project exits with status 0 on success or status 1 on error.
- CSV files must be pre-cleaned and formatted as per provided samples.
- Ensure your Google account or service account has Bigtable permissions.

## ✅ License

This project is for academic and demonstration purposes.
