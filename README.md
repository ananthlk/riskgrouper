# Project Title: Risk Grouper

## Overview
This project aims to develop a risk stratification model to predict the likelihood of emergency department (ED) visits within 30, 60, and 90-day periods. The model leverages a combination of claims data (medical, behavioral, and pharmacy), care team notes, and operational data to generate predictions.

## Technical Architecture
The project is built with the following technical stack:
- **Data Preparation & Feature Engineering**: Snowflake is used for data preparation and feature engineering. The SQL scripts for these processes are located in the `scripts/sql` directory. The pipeline is designed to be point-in-time correct, handling data lags for different sources (e.g., medical vs. pharmacy claims) to prevent data leakage.
- **Machine Learning Model**: A Python-based XGBoost model is used for prediction. The main model training and evaluation script is `src/RiskGrouper.py`.
- **Database Connector**: A Python script (`src/snowflake_connector.py`) handles the connection to Snowflake using SSO authentication and environment variables.
- **Orchestration**: The main `main.py` script orchestrates the execution of the SQL pipeline and the ML model.
- **Validation**: A data validation script (`scripts/validation/run_validation.py`) checks for issues like claim latency and data consistency post-pipeline execution.

## Project Structure
```
.
├── docs/
├── scripts/
│   ├── sql/
│   │   ├── events.sql
│   │   └── daily_aggregation.sql
│   ├── utils/
│   └── validation/
├── src/
│   ├── RiskGrouper.py
│   └── snowflake_connector.py
├── .env
├── .gitignore
├── main.py
├── README.md
└── requirements.txt
```

## Setup and Installation
1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```
2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install the required Python packages:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Environment Variables:**
    Create a `.env` file in the root directory and add your Snowflake credentials.
    ```
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_ACCOUNT=your_account
    SNOWFLAKE_WAREHOUSE=your_warehouse
    SNOWFLAKE_DATABASE=your_database
    SNOWFLAKE_SCHEMA=your_schema
    ```

## Usage
1.  **Run the full pipeline:**
    Execute the main Python script to run the SQL data preparation pipeline and then train the model and generate predictions:
    ```bash
    python main.py
    ```
2.  **Run Data Validation:**
    After the pipeline has created the tables, you can run the validation script:
    ```bash
    python scripts/validation/run_validation.py
    ```
