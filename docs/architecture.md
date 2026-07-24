# NHS Healthcare Data Platform

## Sprint 1 – Project Foundation

### Objective

Establish the project structure and create a maintainable architecture before writing business logic.

### What was built

- Created a modular project structure.
- Created the `src` package.
- Created utility modules.
- Created configuration management using YAML.
- Created logging utility.
- Created `main.py` as the application entry point.

### Design Decisions

- Configuration was separated from the code using `config.yaml`.
- Logging was centralized using a reusable logger.
- Business logic was separated into different modules following the Single Responsibility Principle (SRP).
- Folder structure was designed to support future Medallion Architecture implementation.

### Lessons Learned

- Enterprise applications should separate configuration from business logic.
- Small design decisions early make future development easier.

---

# Sprint 2 – Data Extraction

## Objective

Build a reusable extraction module capable of reading patient admission data into Spark.

## What was built

- Created `extract_patients.py`.
- Built a reusable extraction function.
- Integrated SparkSession.
- Added exception handling.
- Added extraction logging.
- Successfully loaded patient records from the Landing layer.

## Design Decisions

- Extraction is responsible only for reading data.
- Validation and transformation were deliberately excluded from this module.
- Errors are logged and re-raised to allow orchestration tools to detect failures.

## Lessons Learned

- Every module should have a single responsibility.
- Logging every major operation improves troubleshooting.

---

# Sprint 3 – Data Validation

## Objective

Prevent poor-quality data from entering the platform.

## What was built

- Required column validation.
- Missing Patient ID validation.
- Validation module.
- Quality Rules module.
- Validation Runner.

## Validation Rules

- Missing Patient ID
- Missing Hospital
- Future Admission Date
- Invalid Admission Type
- Duplicate Patient ID

## Design Decisions

- Each validation rule was implemented as an independent function.
- Rules return both valid and invalid DataFrames.
- Validation Runner executes all rules sequentially.

## Lessons Learned

- Data quality is one of the most important responsibilities of a Data Engineer.
- Enterprise systems isolate invalid data instead of deleting it.

---

# Sprint 4 – Reusable Validation Framework

## Objective

Create an extensible validation framework that supports future business requirements.

## What was built

- Validation Runner
- Rule registration mechanism
- Extensible quality framework

## Design Decisions

- New validation rules are added only to:
    - `quality_rules.py`
    - `validation_runner.py`
- `main.py` remains unchanged.

This follows the Open/Closed Principle (OCP):

- Open for extension
- Closed for modification

## Lessons Learned

- Frameworks are easier to maintain than hard-coded validation logic.
- Enterprise applications evolve continuously; the architecture should support growth.

---

# Sprint 5 – Quarantine Layer

## Objective

Persist rejected records instead of losing them after validation.

## What was built

- Quarantine Loader
- Rule-based quarantine folders
- Parquet output
- Failed Rule metadata

## Quarantine Structure

data/
└── quarantine/
    ├── missing_patient_id/
    ├── missing_hospital/
    ├── future_admission_date/
    ├── invalid_admission_type/
    └── duplicate_patient_id/

## Design Decisions

- Invalid records are written as Parquet.
- Every rejected record includes the validation rule that caused the rejection.
- Data is organised by validation rule to simplify troubleshooting.

## Lessons Learned

- Enterprise platforms never discard business data.
- Quarantine enables auditing, investigation and future correction.
- Parquet provides better performance than CSV for analytical workloads.

---

# Current Architecture

Hospital CSV

↓

Landing Layer

↓

Extraction Module

↓

Validation Framework

↓

┌──────────────┴──────────────┐

↓

Valid Records                Invalid Records

↓

Silver Layer              Quarantine Layer

---

# Technologies Used

- Python
- PySpark
- YAML
- Logging
- Apache Spark
- Parquet
- Git
- GitHub
- VS Code

---

# Enterprise Concepts Learned

- Single Responsibility Principle (SRP)
- Open/Closed Principle (OCP)
- Modular Design
- Configuration Management
- Logging
- Exception Handling
- Data Quality
- Quarantine Pattern
- Medallion Architecture (Foundation)
- Reusable Framework Design

---

# Current Status

✅ Project Structure

✅ Configuration

✅ Logging

✅ Data Extraction

✅ Data Validation

✅ Reusable Validation Framework

✅ Quarantine Layer

🚧 Next Sprint

- Bronze Layer
- Silver Layer
- Medallion Architecture
- Incremental Loading