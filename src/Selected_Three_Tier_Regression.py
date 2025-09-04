import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, precision_recall_curve, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.calibration import CalibratedClassifierCV
import os
import shap
from snowflake_connector import SnowflakeConnector
import logging
from datetime import datetime
from imblearn.over_sampling import SMOTE
import matplotlib.pyplot as plt

# --- Configuration & Setup ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
log_summary_file = os.path.join(LOG_DIR, 'run_summary_log.csv')

def setup_logger(analysis_name):
    """Sets up a logger for the analysis."""
    log_filename = datetime.now().strftime(f'{analysis_name.replace(" ", "_")}_%Y-%m-%d_%H-%M-%S.log')
    log_filepath = os.path.join(LOG_DIR, log_filename)
    logger = logging.getLogger(f"{analysis_name}-{datetime.now().strftime('%H%M%S')}")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filepath)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger, log_filepath

def prepare_data(df, target, features, logger):
    """
    Prepares data for modeling by splitting into train, validation, and test sets.
    """
    logger.info("--- Starting Data Preparation ---")
    if df.empty:
        logger.warning("DataFrame is empty. Cannot prepare data.")
        return None, None, None

    # Member-Level Splitting to prevent data leakage
    unique_member_ids = df['fh_id'].unique()
    train_members, test_members = train_test_split(unique_member_ids, test_size=0.2, random_state=42)
    train_members, val_members = train_test_split(train_members, test_size=0.25, random_state=42)
    
    logger.info(f"Number of unique member IDs: {len(unique_member_ids)}")
    logger.info(f"Training members: {len(train_members)}, Validation members: {len(val_members)}, Test members: {len(test_members)}")

    # Create dataframes for each split
    train_df = df[df['fh_id'].isin(train_members)].copy().dropna(subset=[target])
    val_df = df[df['fh_id'].isin(val_members)].copy().dropna(subset=[target])
    test_df = df[df['fh_id'].isin(test_members)].copy().dropna(subset=[target])

    logger.info(f"Training shape: {train_df.shape}, Validation shape: {val_df.shape}, Test shape: {test_df.shape}")

    # Separate features and target
    X_train = train_df[features].select_dtypes(include=['number']).fillna(0)
    y_train = train_df[target]
    
    X_val = val_df[features].select_dtypes(include=['number']).fillna(0).reindex(columns=X_train.columns, fill_value=0)
    y_val = val_df[target]
    
    X_test = test_df[features].select_dtypes(include=['number']).fillna(0).reindex(columns=X_train.columns, fill_value=0)
    y_test = test_df[target]

    logger.info(f"Final shapes: Train {X_train.shape}, Val {X_val.shape}, Test {X_test.shape}")
    return (X_train, y_train), (X_val, y_val), (X_test, y_test), train_df, val_df, test_df

def apply_smote(X_train, y_train, logger):
    """Applies SMOTE to the training data to handle class imbalance."""
    logger.info("Applying SMOTE to balance the training data.")
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    logger.info(f"Original training set size: {X_train.shape}, Resampled size: {X_train_resampled.shape}")
    return X_train_resampled, y_train_resampled

def train_models(X_train, y_train, logger):
    """Trains and returns the Logistic Regression and XGBoost models."""
    # Logistic Regression
    logger.info("Training Logistic Regression model.")
    logistic_model = LogisticRegression(solver='liblinear', random_state=42)
    logistic_model.fit(X_train, y_train)

    # XGBoost with GridSearchCV
    logger.info("Training XGBoost model with GridSearchCV.")
    xgb_model = XGBClassifier(objective='binary:logistic', eval_metric='auc', use_label_encoder=False, random_state=42)
    param_grid = {'n_estimators': [50, 100], 'max_depth': [3, 5], 'learning_rate': [0.1, 0.01]}
    grid_search = GridSearchCV(estimator=xgb_model, param_grid=param_grid, scoring='roc_auc', cv=2, verbose=1, n_jobs=-1)
    grid_search.fit(X_train, y_train)
    best_xgb_model = grid_search.best_estimator_
    logger.info(f"Best XGBoost parameters: {grid_search.best_params_}")

    return logistic_model, best_xgb_model

def optimize_thresholds(y_val, val_probs, logger):
    """Finds the optimal threshold for the XGBoost model using a Precision-Recall Curve."""
    precision, recall, thresholds = precision_recall_curve(y_val, val_probs)
    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-10)
    optimal_idx = np.argmax(f1_scores)
    optimal_threshold = thresholds[optimal_idx]

    logger.info(f"Optimal Threshold: {optimal_threshold:.4f} (F1-score: {f1_scores[optimal_idx]:.4f})")
    
    return optimal_threshold

def classify_and_report(y_true, y_pred, name, logger):
    """Generates and logs a classification report."""
    report = classification_report(y_true, y_pred, zero_division=0)
    logger.info(f"--- {name} Classification Report ---\n{report}")
    print(f"--- {name} Classification Report ---\n{report}")
    return report

def generate_individual_report(df_reclassified, explainer, base_output_dir, logger, features):
    """
    Generates a detailed, patient-level report for the last 6 months.
    """
    logger.info("--- Generating Individualized Patient Report ---")

    # Filter data for the last 6 months (2024-07-01 to 2024-12-01)
    start_date_ts = pd.Timestamp('2024-07-01')
    end_date_ts = pd.Timestamp('2024-12-01')
    df_reclassified['effective_month_start'] = pd.to_datetime(df_reclassified['effective_month_start'])
    df_recent = df_reclassified[(df_reclassified['effective_month_start'] >= start_date_ts) & 
                                (df_reclassified['effective_month_start'] <= end_date_ts)].copy()
    
    if df_recent.empty:
        logger.warning("No data found for the latest 6 months. Skipping report generation.")
        return

    # Extract features for SHAP explanation
    X_recent = df_recent[features].select_dtypes(include=['number']).fillna(0)

    # Compute SHAP values for recent data
    shap_values_recent = explainer.shap_values(X_recent)
    
    # Store explanations
    shap_explanations_list = []
    for i in range(len(X_recent)):
        explanation = {feature: float(shap_value) for feature, shap_value in zip(features, shap_values_recent[i])}
        shap_explanations_list.append(explanation)

    df_recent['local_shap_factors'] = shap_explanations_list
    
    def get_key_factors(row):
        shap_exp = row['local_shap_factors']
        sorted_shap = sorted(shap_exp.items(), key=lambda item: abs(item[1]), reverse=True)
        return {feature: value for feature, value in sorted_shap[:3]}

    df_recent['key_drivers'] = df_recent.apply(get_key_factors, axis=1)

    report_cols = ['fh_id', 'effective_month_start', 'predicted_risk', 'risk_score', 'key_drivers', 'local_shap_factors']
    final_report = df_recent[report_cols].sort_values(by=['fh_id', 'effective_month_start'])

    report_filename = os.path.join(base_output_dir, 'individual_risk_report_latest_6_months.csv')
    final_report.to_csv(report_filename, index=False)
    logger.info(f"Individual patient report saved to {report_filename}")

def run_pipeline(df, analysis_name, target, base_output_dir, logger):
    """Orchestrates the entire ML pipeline for a given target variable."""
    logger.info(f"--- Starting pipeline for target: {target} ---")

    # Step 1: Prepare data
    non_feature_cols = ['fh_id', 'effective_month_start', 'any_event_next_90d', 'ed_event_next_30d', 'ed_event_next_60d', 'ed_event_next_90d',
                        'ip_event_next_30d', 'ip_event_next_60d', 'ip_event_next_90d']
    features = [col for col in df.columns if col not in non_feature_cols and df[col].dtype in ['int64', 'float64']]

    prepared_data = prepare_data(df, target, features, logger)
    if not prepared_data:
        return
    (X_train, y_train), (X_val, y_val), (X_test, y_test), train_df, val_df, test_df = prepared_data

    # Step 2: Handle class imbalance
    X_train_resampled, y_train_resampled = apply_smote(X_train, y_train, logger)

    # Step 3: Train models (only once)
    logistic_model, best_xgb_model = train_models(X_train_resampled, y_train_resampled, logger)

    # --- ACTIONABLE OUTPUT A: Low-Risk Patient Explanation (Logistic Regression) ---
    logger.info("--- Interpreting Low-Risk Patients (Logistic Regression) ---")
    log_coeffs = pd.Series(logistic_model.coef_[0], index=X_train.columns)
    logger.info(f"Logistic Regression Coefficients:\n{log_coeffs.sort_values(ascending=False).to_string()}")
    print("Logistic Regression Coefficients for Low-Risk Patients:\n"
          "The magnitude of the coefficient indicates the feature's impact on risk. "
          "A positive value increases risk, while a negative value decreases it.\n"
          f"{log_coeffs.sort_values(ascending=False).to_string()}")

    # --- MODIFIED CODE FOR CUMULATIVE PLOT WITH CALIBRATION ---
    print("--- Analyzing Calibrated Probabilities for Low-Risk Cutoff ---")
    
    # Declare the start and end dates as Timestamp objects once
    start_date_ts = pd.Timestamp('2024-07-01')
    end_date_ts = pd.Timestamp('2024-12-01')
    
    # Calibrate the logistic model using the validation data
    calibrated_logistic_model = CalibratedClassifierCV(logistic_model, method='isotonic', cv="prefit")
    calibrated_logistic_model.fit(X_val, y_val)

    # Filter validation set for plotting
    val_df['effective_month_start'] = pd.to_datetime(val_df['effective_month_start'])
    X_val_recent = val_df[(val_df['effective_month_start'] >= start_date_ts) & (val_df['effective_month_start'] <= end_date_ts)]
    X_val_recent = X_val_recent[features].select_dtypes(include=['number']).fillna(0).reindex(columns=X_train.columns, fill_value=0)
    
    # Get calibrated probabilities
    calibrated_probs = calibrated_logistic_model.predict_proba(X_val_recent)[:, 1]
    
    # Calculate and plot the cumulative distribution function (CDF)
    sorted_probs = np.sort(calibrated_probs)
    cumulative_probs = np.arange(1, len(sorted_probs) + 1) / len(sorted_probs)

    plt.figure(figsize=(10, 6))
    plt.plot(sorted_probs, cumulative_probs, marker='.', linestyle='none', markersize=2, alpha=0.7)
    plt.title('Cumulative Distribution of Calibrated Probabilities')
    plt.xlabel('Calibrated Predicted Probability of Adverse Event')
    plt.ylabel('Cumulative Percentage of Patients')
    plt.grid(True)
    plt.axvline(x=0.2, color='r', linestyle='--', label='Example Cutoff (0.2)')
    plt.legend()
    plt.show()

    while True:
        try:
            low_risk_threshold = float(input("Based on the plot, please enter the low-risk probability threshold (e.g., 0.15): "))
            if 0 < low_risk_threshold < 1:
                break
            else:
                print("Please enter a valid number between 0 and 1.")
        except ValueError:
            print("Invalid input. Please enter a number.")
    print(f"Low-risk threshold set to: {low_risk_threshold}")
    # --- END MODIFIED CODE ---

    # Step 4: Predict probabilities on validation data to optimize thresholds
    val_probs = best_xgb_model.predict_proba(X_val.reindex(columns=X_train.columns, fill_value=0))[:, 1]
    optimal_threshold = optimize_thresholds(y_val, val_probs, logger)
    
    # Step 5: Final evaluation and reporting on the test set
    test_probs = best_xgb_model.predict_proba(X_test.reindex(columns=X_train.columns, fill_value=0))[:, 1]
    test_preds = (test_probs >= optimal_threshold).astype(int)
    
    classify_and_report(y_test, test_preds, "Test", logger)

    # --- ACTIONABLE OUTPUT B: High-Risk Patient Explanation (Population Level) ---
    logger.info("--- Interpreting High-Risk Patients (XGBoost) ---")
    xgb_importances = pd.Series(best_xgb_model.feature_importances_, index=X_train.columns)
    logger.info(f"XGBoost Global Feature Importances:\n{xgb_importances.sort_values(ascending=False).to_string()}")
    print("XGBoost Global Feature Importances for High-Risk Patients:\n"
          "This shows which features are most important for the model's predictions overall.\n"
          f"{xgb_importances.sort_values(ascending=False).to_string()}")

    # Step 6: Generate final patient report
    df_reclassified = test_df.copy()
    df_reclassified['risk_score'] = test_probs
    df_reclassified['predicted_risk'] = ['High' if p >= optimal_threshold else 'Low' for p in test_probs]

    # --- ACTIONABLE OUTPUT C: Individual Patient Report ---
    explainer = shap.TreeExplainer(best_xgb_model)
    generate_individual_report(df_reclassified, explainer, base_output_dir, logger, features)

    # --- Final Summary of Key Metrics ---
    print("\n--- Final Summary for Recent Data (2024-07-01 to 2024-12-01) ---")
    
    # Filter the final reclassified dataframe for the summary
    df_reclassified['effective_month_start'] = pd.to_datetime(df_reclassified['effective_month_start'])
    
    # Use the same Timestamp variables to filter the test set
    df_reclassified_recent = df_reclassified[(df_reclassified['effective_month_start'] >= start_date_ts) & 
                                             (df_reclassified['effective_month_start'] <= end_date_ts)].copy()
                                             
    if not df_reclassified_recent.empty:
        total_effective_months = len(df_reclassified_recent)
        risk_counts = df_reclassified_recent['predicted_risk'].value_counts()
        
        low_risk_count = risk_counts.get('Low', 0)
        high_risk_count = risk_counts.get('High', 0)
        
        medium_risk_count = 0
        
        # Original events from the test set for the same time period
        y_test_recent = test_df