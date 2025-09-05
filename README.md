# Project Title: Risk Grouper

## Overview
This project aims to develop a risk stratification model to predict the likelihood of emergency department (ED) visits within 30, 60, and 90-day periods. The model leverages a combination of claims data (medical, behavioral, and pharmacy), care team notes, and operational data to generate predictions.

## Technical Architecture
The project is built with the following technical stack:
- **Data Preparation & Feature Engineering**: Snowflake is used for data preparation, feature engineering, and for invoking Large Language Model (LLM) calls to stratify care team notes. The SQL scripts for these processes are located in the `scripts/sql` directory.
- **Machine Learning Model**: A Python-based XGBoost model is used for prediction. The main model training and evaluation script is `src/main.py`.
- **Database Connector**: A Python script (`src/snowflake_connector.py`) handles the connection to Snowflake using SSO authentication.
- **Scoring**: The final scoring mechanism to identify high-risk individuals is yet to be determined (TBD) and will be implemented either in Snowflake or Python.

## Project Structure
```
.
├── docs/
├── scripts/
│   └── sql/
│       ├── baselines.sql
│       ├── daily_aggegation.sql
│       ├── data_prep.sql
│       ├── data_split.sql
│       ├── events.sql
│       ├── Notes_agentic.sql
│       └── Prompts.sql
├── src/
│   ├── main.py
│   └── snowflake_connector.py
├── .gitignore
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
4.  **Configure Snowflake Connection:**
    Update the `SNOWFLAKE_CONFIG` dictionary in `src/snowflake_connector.py` with your credentials if they are different from the current setup.

## Usage
1.  **Data Preparation in Snowflake:**
    Execute the SQL scripts in the `scripts/sql` directory in the recommended order to prepare the data in your Snowflake environment.
2.  **Run the ML Model:**
    Execute the main Python script to train the model and generate predictions:
    ```bash
    python src/main.py
    ```
    The script will prompt you to choose the type of analysis to run.
