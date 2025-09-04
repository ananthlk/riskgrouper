"""
Risk Grouper ML Model Runner

This script serves as the main entry point for training and evaluating machine learning
models to predict healthcare risk. It provides a command-line interface for users to
select from different predefined datasets, fetches the corresponding data from Snowflake,
and then runs a comprehensive modeling pipeline for multiple target variables.

The pipeline includes:
1.  **Data Fetching**: Connects to Snowflake using the `SnowflakeConnector` to pull
    one of three datasets: Master, Engaged Group, or Claims-only.
2.  **Data Preparation**: Splits the data into training and validation sets based on a
    predefined column, handles missing values in the target variable, and separates
    features from non-feature columns.
3.  **Baseline Modeling**: Trains a Logistic Regression model as a baseline for performance
    comparison.
4.  **Advanced Modeling (XGBoost)**:
    - Handles class imbalance using `scale_pos_weight`.
    - Performs hyperparameter tuning using `GridSearchCV` to find the best model
      parameters for `n_estimators`, `max_depth`, and `learning_rate`.
    - Trains a final XGBoost model with the best parameters.
5.  **Evaluation**: Evaluates the XGBoost model on the validation set, calculating
    accuracy, AUC score, confusion matrix, and a detailed classification report.
6.  **Output Generation**: Saves multiple artifacts for each run, including:
    - Model predictions on the validation set.
    - Feature importances.
    - A SHAP summary plot for model interpretability.
    - A list of the top individuals most at risk.
    - A text file summarizing the key performance metrics.
7.  **Logging and Auditing**:
    - Creates a detailed, timestamped log file for each analysis run.
    - Appends a one-line summary of each run to a master CSV log (`run_summary_log.csv`),
      tracking key parameters and results for audit and review.

The script is designed to be run iteratively for different prediction windows (30, 60, 90 days)
and event types (ED, IP, Any), making it a powerful tool for comprehensive risk analysis.

Usage:
    Run the script from the command line and follow the interactive prompts:
    $ python src/RiskGrouper.py
"""
import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
import os
import shap
from snowflake_connector import SnowflakeConnector
import logging
from datetime import datetime

# Define the directory for storing log files.
# This helps in organizing and retaining historical run data.
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Define the master log file for summarizing all runs.
# This provides a high-level audit trail of all analyses performed.
log_summary_file = os.path.join(LOG_DIR, 'run_summary_log.csv')

def setup_logger(analysis_name):
    """
    Sets up a dedicated logger for a specific analysis run.

    This function creates a unique logger instance that writes to both a timestamped
    log file and the console. This ensures that all details of a run are captured
    for debugging and auditing, while also providing real-time feedback to the user.

    Args:
        analysis_name (str): The name of the analysis, used to name the log file.

    Returns:
        tuple: A tuple containing the configured logger instance and the path to the log file.
    """
    # Generate a unique, timestamped filename for the log.
    log_filename = datetime.now().strftime(f'{analysis_name.replace(" ", "_")}_%Y-%m-%d_%H-%M-%S.log')
    log_filepath = os.path.join(LOG_DIR, log_filename)

    # Create a unique logger name to avoid conflicts between different runs.
    logger = logging.getLogger(f"{analysis_name}-{datetime.now().strftime('%H%M%S')}")
    logger.setLevel(logging.INFO)

    # Configure handlers for file and console output.
    file_handler = logging.FileHandler(log_filepath)
    console_handler = logging.StreamHandler()

    # Define a standard format for log messages.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger, ensuring they are not added multiple times.
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger, log_filepath


def prepare_and_run_models(df, analysis_name, target='ip_event_next_30d', base_output='model_output.csv', logger=None, run_summary=None):
    """
    Prepares data, trains, evaluates, and saves models and their outputs.

    This is the core function of the script, performing the end-to-end modeling pipeline
    for a given dataset and target variable.

    Args:
        df (pd.DataFrame): The input DataFrame containing all features and metadata.
        analysis_name (str): A descriptive name for the specific analysis run (e.g., "Master Dataset - y_ed_30d").
        target (str): The name of the column to be used as the dependent variable.
        base_output (str): The base path for all output files for this run.
        logger (logging.Logger): The logger instance for detailed logging.
        run_summary (dict): A dictionary to accumulate summary metrics for the master log.
    """
    if df.empty:
        logger.warning(f"No data to analyze for '{analysis_name}'. Skipping model training.")
        return

    logger.info(f"--- Running Analysis for: {analysis_name} ---")

    # A) Basic data insights and preparation
    logger.info("--- Basic Data Insights ---")
    logger.info(f"Initial DataFrame shape: {df.shape}")
    run_summary['initial_rows'] = df.shape[0]
    
    # Split data into training and testing sets based on the 'dataset_split' column.
    train_df = df[df['dataset_split'] == 'TRAIN'].copy()
    val_df = df[df['dataset_split'] == 'TEST'].copy()
    
    # Drop rows with missing values in the target variable to ensure clean training.
    initial_train_rows = len(train_df)
    initial_val_rows = len(val_df)
    train_df.dropna(subset=[target], inplace=True)
    val_df.dropna(subset=[target], inplace=True)
    logger.info(f"Dropped {initial_train_rows - len(train_df)} rows from training set due to NaN in target '{target}'.")
    logger.info(f"Dropped {initial_val_rows - len(val_df)} rows from validation set due to NaN in target '{target}'.")
    run_summary['training_rows_after_cleaning'] = len(train_df)
    run_summary['validation_rows_after_cleaning'] = len(val_df)

    # Dynamically identify feature columns by excluding identifiers, metadata, and all target variables.
    non_feature_cols = [
        'member_id', 'event_date', 'dob', 'dataset_split', 'period',
        'gender', 'engagement_group', 'normalized_coverage_category',
        'y_ed_30d', 'y_ed_60d', 'y_ed_90d', 'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
        'y_any_30d', 'y_any_60d', 'y_any_90d', 'has_care_notes_post_period'
    ] + [col for col in df.columns if '_next_' in col]
    
    features = [col for col in df.columns if col not in non_feature_cols]
    # Ensure only numeric types are used for modeling and fill any remaining NaNs with 0.
    X_train = train_df[features].select_dtypes(include=['number']).fillna(0)
    y_train = train_df[target]
    
    # Ensure validation set has the same columns as the training set.
    X_val = val_df[features].select_dtypes(include=['number']).fillna(0)
    y_val = val_df[target]

    # Align columns - crucial for preventing feature mismatch errors
    train_cols = X_train.columns
    X_val = X_val.reindex(columns=train_cols, fill_value=0)

    if X_val.empty:
        logger.warning(f"Validation set is empty for target '{target}' after cleaning. Skipping evaluation.")
        run_summary['status'] = 'COMPLETED_NO_VALIDATION_DATA'
        return
    
    logger.info(f"Training features shape: {X_train.shape}")
    logger.info(f"Validation features shape: {X_val.shape}")
    run_summary['feature_count'] = X_train.shape[1]

    # B) Running a Baseline Model (Logistic Regression) for comparison
    logger.info("--- Running Logistic Regression Baseline ---")
    lr_model = LogisticRegression(solver='liblinear', random_state=42)
    lr_model.fit(X_train, y_train)
    lr_pred = lr_model.predict(X_val)
    
    lr_accuracy = accuracy_score(y_val, lr_pred)
    logger.info(f"Logistic Regression Accuracy: {lr_accuracy:.4f}")
    logger.info("Classification Report (Logistic Regression):\n" + classification_report(y_val, lr_pred, zero_division=0))
    run_summary['lr_accuracy'] = lr_accuracy

    # C) Train the primary XGBoost model with hyperparameter tuning
    logger.info("--- Training XGBoost model with Hyperparameter Tuning ---")
    # Calculate scale_pos_weight to handle class imbalance, which is common in risk prediction.
    scale_pos_weight = y_train.value_counts()[0] / y_train.value_counts()[1] if y_train.value_counts()[1] > 0 else 1
    logger.info(f"Calculated scale_pos_weight for imbalance: {scale_pos_weight:.2f}")

    # Define a small grid of parameters for efficient tuning.
    param_grid = {
        'n_estimators': [50, 100], 'max_depth': [3, 5], 'learning_rate': [0.1, 0.01],
    }
    model = XGBClassifier(objective='binary:logistic', eval_metric='auc', use_label_encoder=False, scale_pos_weight=scale_pos_weight, random_state=42)
    
    # Use GridSearchCV to find the best model parameters based on ROC AUC.
    grid_search = GridSearchCV(estimator=model, param_grid=param_grid, scoring='roc_auc', cv=2, verbose=1, n_jobs=2)
    logger.info("Starting hyperparameter grid search...")
    grid_search.fit(X_train, y_train)
    logger.info("Hyperparameter grid search complete.")
    best_model = grid_search.best_estimator_
    logger.info(f"Hyperparameter Tuning Complete. Best parameters found: {grid_search.best_params_}")
    run_summary['xgb_best_params'] = str(grid_search.best_params_)

    # D) Evaluate the tuned model on the validation set
    logger.info("--- Validation Set Performance (XGBoost) ---")
    y_val_pred_proba = best_model.predict_proba(X_val)[:, 1]
    y_val_pred = best_model.predict(X_val)
    xgb_accuracy = accuracy_score(y_val, y_val_pred)
    xgb_auc = roc_auc_score(y_val, y_val_pred_proba)
    logger.info(f"XGBoost Accuracy: {xgb_accuracy:.4f}")
    logger.info(f"XGBoost AUC Score: {xgb_auc:.4f}")
    logger.info("Confusion Matrix (XGBoost):\n" + str(pd.DataFrame(confusion_matrix(y_val, y_val_pred), index=['Actual Negative', 'Actual Positive'], columns=['Predicted Negative', 'Predicted Positive'])))
    logger.info("Classification Report (XGBoost):\n" + classification_report(y_val, y_val_pred, zero_division=0))
    run_summary['xgb_accuracy'] = xgb_accuracy
    run_summary['xgb_auc'] = xgb_auc

    # Save model predictions and feature importances to files
    logger.info("Saving model output and feature importances...")
    output_path = base_output
    importances_path = base_output.replace('.csv', '_feature_importances.csv')
    val_df_copy = val_df.copy()
    val_df_copy['predicted_proba'] = best_model.predict_proba(X_val)[:, 1]
    val_df_copy['predicted_label'] = best_model.predict(X_val)
    importances_df = pd.DataFrame({'feature': X_val.columns, 'importance': best_model.feature_importances_}).sort_values(by='importance', ascending=False)
    val_df_copy.to_csv(output_path, index=False)
    importances_df.to_csv(importances_path, index=False)
    logger.info(f"Model predictions saved to {output_path}")
    logger.info(f"Feature importances saved to {importances_path}")
    run_summary['predictions_file'] = output_path
    run_summary['feature_importance_file'] = importances_path

    # E) Perform SHAP analysis for model interpretability
    logger.info("--- Running SHAP Analysis ---")
    explainer = shap.TreeExplainer(best_model)
    shap_values = explainer.shap_values(X_val)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    shap.summary_plot(shap_values, X_val, show=False, plot_size="auto")
    shap_path = os.path.splitext(base_output)[0] + f"_{target}_SHAP.png"
    plt.savefig(shap_path, bbox_inches='tight')
    plt.close()
    logger.info(f"SHAP summary plot saved to {shap_path}")
    run_summary['shap_plot_file'] = shap_path

    # F) Identify and save the top N individuals with the highest predicted risk
    top_n = 20 if len(val_df_copy) > 20 else len(val_df_copy)
    val_df_copy['fh_id'] = val_df_copy['predicted_proba']
    top_individuals = val_df_copy[['fh_id', 'predicted_proba']].sort_values('predicted_proba', ascending=False)
    top_out = os.path.splitext(base_output)[0] + f"_{target}_top.csv"
    top_individuals.head(top_n).to_csv(top_out, index=False)
    logger.info(f"Top {top_n} individuals for {target} saved to {top_out}")
    run_summary['top_individuals_file'] = top_out

    # G) Save a comprehensive text summary of the results
    summary_path = os.path.splitext(base_output)[0] + f"_{target}_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"Analysis Name: {analysis_name}\n")
        f.write(f"Dependent variable: {target}\n")
        f.write("\n--- Logistic Regression Results (Validation) ---\n")
        f.write(f"Accuracy: {lr_accuracy:.4f}\n")
        f.write("Classification Report:\n")
        f.write(classification_report(y_val, lr_pred, zero_division=0))
        f.write("\n\n--- XGBoost Validation Results ---\n")
        f.write(f"Best Parameters: {grid_search.best_params_}\n")
        f.write(f"Accuracy: {xgb_accuracy:.4f}\n")
        f.write(f"AUC Score: {xgb_auc:.4f}\n")
        f.write("Confusion Matrix:\n")
        f.write(str(pd.DataFrame(confusion_matrix(y_val, y_val_pred), index=['Actual Negative', 'Actual Positive'], columns=['Predicted Negative', 'Predicted Positive'])))
        f.write("\nClassification Report:\n")
        f.write(classification_report(y_val, y_val_pred, zero_division=0))
    logger.info(f"Summary results for {target} saved to {summary_path}")
    run_summary['summary_report_file'] = summary_path


def main():
    """
    Main function to orchestrate the model training and evaluation pipeline.

    This function provides an interactive command-line interface to guide the user
    through selecting an analysis type, running models for multiple
    targets, and logging the results. It is optimized to be memory-efficient
    by fetching data from Snowflake for one target at a time.
    """
    # --- Setup for a specific run ---
    run_timestamp = datetime.now()
    
    # --- Step 1: Prompt the user for the analysis type ---
    print("Risk Grouper ML Model Runner (XGBoost)")
    print("=" * 40)
    choice = None
    while choice not in ['1', '2', '3', '4']:
        print("\nWhat type of analysis would you like to run?")
        print("1. Master Dataset (Claims + Notes + All Groups)")
        print("2. Engaged Group (Claims + Notes)")
        print("3. Claims Only (All Groups)")
        print("4. Exit")
        choice = input("Enter your choice (1, 2, 3, or 4): ")

    if choice == '4':
        print("\nExiting script.")
        return

    analysis_name_map = {
        '1': "Master Dataset Analysis", '2': "Engaged Group Analysis", '3': "Claims Only Analysis"
    }
    analysis_name = analysis_name_map.get(choice)
    
    # Setup the logger for this specific run.
    logger, log_filepath = setup_logger(analysis_name)
    
    # Initialize a base summary dictionary that will be copied for each target.
    base_run_summary = {
        'run_timestamp': run_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'analysis_type': analysis_name,
        'log_file': log_filepath,
        'status': 'STARTED'
    }

    try:
        # --- Step 2: Define the Snowflake query and target variables ---
        query_map = {
            '1': "SELECT * FROM TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_MASTER",
            '2': "SELECT * FROM TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_NOTES_ONLY",
            '3': "SELECT * FROM TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_NOTES_ONLY",
            'notes_only': "SELECT * FROM TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_NOTES_ONLY"
        }
        query = query_map.get(choice)

        target_vars = [
           'ed_event_next_30d', 'ed_event_next_60d', 'ed_event_next_90d',
            'ip_event_next_30d', 'ip_event_next_60d', 'ip_event_next_90d'
        ]

        # Create a unique output directory for this run to keep artifacts organized.
        base_output_dir = f"output/{analysis_name.replace(' ', '_')}_{run_timestamp.strftime('%Y%m%d_%H%M%S')}"
        if not os.path.exists(base_output_dir):
            os.makedirs(base_output_dir)
        logger.info(f"All outputs will be saved in the directory: {base_output_dir}")

        # --- Step 3: Loop through each target, fetch data, and run the model ---
        # This approach loads data one target at a time to conserve memory.
        for target in target_vars:
            logger.info(f"--- Starting process for target: {target} ---")
            
            # Create a fresh copy of the run summary for this specific target.
            target_run_summary = base_run_summary.copy()
            target_run_summary['target_variable'] = target

            # Fetch data from Snowflake for the current target analysis.
            df = pd.DataFrame()
            logger.info(f"Fetching data from Snowflake for: {analysis_name} using streaming...")
            data_chunks = []
            with SnowflakeConnector() as sf:
                if sf.connection:
                    for chunk in sf.streaming(query):
                        data_chunks.append(chunk)

            df = pd.concat(data_chunks, ignore_index=True) if data_chunks else pd.DataFrame()
            
            if df is None or df.empty:
                logger.error(f"No data returned from Snowflake for target {target}. Skipping.")
                target_run_summary['status'] = 'FAILED - NO DATA'
                # Log the failure and continue to the next target.
                summary_df = pd.DataFrame([target_run_summary])
                if os.path.exists(log_summary_file):
                    summary_df.to_csv(log_summary_file, mode='a', header=False, index=False)
                else:
                    summary_df.to_csv(log_summary_file, mode='w', header=True, index=False)
                continue

            # Standardize column names to lower case for consistency.
            df.columns = [col.lower() for col in df.columns]
            logger.info(f"Successfully loaded {len(df)} records for target {target}.")

            # Define the output file for this specific target's predictions.
            base_output_file = os.path.join(base_output_dir, f"{target}_predictions.csv")
            
            # Run the modeling pipeline.
            prepare_and_run_models(
                df, 
                f"{analysis_name} - {target}", 
                target=target, 
                base_output=base_output_file, 
                logger=logger, 
                run_summary=target_run_summary
            )
            
            # After the run, update status and log the summary for this target.
            target_run_summary['status'] = 'COMPLETED'
            summary_df = pd.DataFrame([target_run_summary])
            if os.path.exists(log_summary_file):
                summary_df.to_csv(log_summary_file, mode='a', header=False, index=False)
            else:
                summary_df.to_csv(log_summary_file, mode='w', header=True, index=False)
            
            # Explicitly free memory to prevent crashes on the next loop
            logger.info(f"Releasing memory after processing target: {target}")
            del df
            import gc
            gc.collect()
            logger.info("Memory cleanup complete.")

    except Exception as e:
        logger.error(f"An unhandled exception occurred in main loop: {e}", exc_info=True)
        base_run_summary['status'] = f'FAILED - {e}'
        # Log the failure to the summary file.
        summary_df = pd.DataFrame([base_run_summary])
        if os.path.exists(log_summary_file):
            summary_df.to_csv(log_summary_file, mode='a', header=False, index=False)
        else:
            summary_df.to_csv(log_summary_file, mode='w', header=True, index=False)
    finally:
        logger.info(f"All analyses complete. Master run summary saved to {log_summary_file}")


if __name__ == "__main__":
    main()
