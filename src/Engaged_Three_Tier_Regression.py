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
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [5, 7, 9],
        'learning_rate': [0.1, 0.05, 0.01]
    }
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
    # Exclude 'is_disenrolled' from features
    features = [col for col in df.columns if col not in non_feature_cols and df[col].dtype in ['int64', 'float64'] and col != 'is_disenrolled']

    # Exclude 'ed_event_next_90d' and 'ip_event_next_90d' from features
    features = [
        col for col in df.columns
        if col not in non_feature_cols and df[col].dtype in ['int64', 'float64']
        and col not in ['ed_event_next_90d', 'ip_event_next_90d']
    ]

    # Consolidated exclusion list for features
    exclude_from_features = [
        'is_active_next_90d', 'ed_event_next_90d', 'ip_event_next_90d',
        'is_disenrolled', 'ed_events_last_3_months', 'ip_events_last_3_months',
        'ed_events_in_month', 'is_reinstated', 'ip_events_in_month',
        'is_enrolled'
    ]
    # Exclude all is_market_xxxx flags
    exclude_from_features += [col for col in df.columns if col.startswith('is_market_')]
    # Exclude all columns ending with '_treated_this_month'
    exclude_from_features += [col for col in df.columns if col.endswith('_treated_this_month')]
    features = [
        col for col in df.columns
        if col not in non_feature_cols and df[col].dtype in ['int64', 'float64']
        and col not in exclude_from_features
    ]

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

    # --- Evaluate Logistic Regression on Test Set ---
    logger.info("--- Evaluating Logistic Regression on Test Set ---")
    test_probs_logistic = logistic_model.predict_proba(X_test.reindex(columns=X_train.columns, fill_value=0))[:, 1]
    test_preds_logistic = (test_probs_logistic >= optimal_threshold).astype(int)
    classify_and_report(y_test, test_preds_logistic, "Logistic Regression Test", logger)

    # --- Evaluate XGBoost on Test Set ---
    logger.info("--- Evaluating XGBoost on Test Set ---")
    test_probs_xgb = best_xgb_model.predict_proba(X_test.reindex(columns=X_train.columns, fill_value=0))[:, 1]
    test_preds_xgb = (test_probs_xgb >= optimal_threshold).astype(int)
    classify_and_report(y_test, test_preds_xgb, "XGBoost Test", logger)

    # --- Final Validation as Done Now ---
    logger.info("--- Final Validation ---")
    test_preds_final = (test_probs >= optimal_threshold).astype(int)
    classify_and_report(y_test, test_preds_final, "Final Test", logger)

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
    
    # Ensure start_date_ts and end_date_ts are converted to Timestamps for comparison
    start_date_ts = pd.Timestamp(start_date_ts)
    end_date_ts = pd.Timestamp(end_date_ts)

    # Filter test_df for the recent time period
    # Ensure 'effective_month_start' in test_df is converted to pd.Timestamp
    test_df['effective_month_start'] = pd.to_datetime(test_df['effective_month_start'])

    # Filter test_df for the recent time period
    y_test_recent = test_df[(test_df['effective_month_start'] >= start_date_ts) &
                            (test_df['effective_month_start'] <= end_date_ts)][target].copy()

    # Define df_reclassified_recent by filtering df_reclassified for the specified date range
    df_reclassified_recent = df_reclassified[(pd.to_datetime(df_reclassified['effective_month_start']) >= start_date_ts) &
                                             (pd.to_datetime(df_reclassified['effective_month_start']) <= end_date_ts)].copy()

    if not df_reclassified_recent.empty:
        total_effective_months = len(df_reclassified_recent)
        risk_counts = df_reclassified_recent['predicted_risk'].value_counts()
        
        low_risk_count = risk_counts.get('Low', 0)
        high_risk_count = risk_counts.get('High', 0)
        
        medium_risk_count = 0
        
        # Original events from the test set for the same time period
        y_test_recent = test_df[(test_df['effective_month_start'] >= start_date_ts) & 
                                (test_df['effective_month_start'] <= end_date_ts)][target].copy()

        actual_events = y_test_recent.sum()
        predicted_events = high_risk_count

        print(f"Total Effective Months: {total_effective_months}")
        print(f"Individuals Classified Low Risk: {low_risk_count}")
        print(f"Individuals Classified Medium Risk: {medium_risk_count}")
        print(f"Individuals Classified High Risk: {high_risk_count}")
        print(f"Total Actual Events: {actual_events}")
        print(f"Total Predicted Events: {predicted_events}")

        # Group data by effective month and risk category
        summary = (df_reclassified_recent
                   .groupby(['effective_month_start', 'predicted_risk'])
                   .agg(total_members=('fh_id', 'count'),
                        total_predicted_events=('risk_score', 'sum'),  # Sum of probabilities
                        average_risk_score=('risk_score', 'mean'),  # Average risk score
                        total_actual_events=(target, 'sum'))
                   .reset_index())

        # Display the summary
        print("\n--- Final Summary for Recent Data (2024-07-01 to 2024-12-01) ---")
        print(summary.to_string(index=False))

    else:
        print("No data available for the specified date range.")

    
def main():
    """Main function to run the risk grouper model."""
    logger, log_filepath = setup_logger("Master Dataset Analysis")
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_output_dir = f"output/Master_Dataset_Analysis_{run_timestamp}"
    if not os.path.exists(base_output_dir):
        os.makedirs(base_output_dir)

    target = 'any_event_next_90d'
    
    try:
        # Prompt the user for the dataset name
        dataset_name = input("Please enter the dataset name (e.g., TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_MASTER): ")
        query = f"SELECT * FROM {dataset_name}"
        
        logger.info("Fetching data from Snowflake...")
        data_chunks = []
        with SnowflakeConnector() as sf:
            if sf.connection:
                for chunk in sf.streaming(query):
                    data_chunks.append(chunk)
        df = pd.concat(data_chunks, ignore_index=True) if data_chunks else pd.DataFrame()
        df.columns = [col.lower() for col in df.columns]

        # Create new computed variable
        df['any_event_next_90d'] = ((df['ed_event_next_90d'] == 1) | (df['ip_event_next_90d'] == 1)).astype(int)
        
        # Set the target variable
        target = 'any_event_next_90d'
        
        # --- ADD THIS CODE SNIPPET ---
        # Explicitly convert boolean-like columns to numeric type
        boolean_cols_to_convert = [
            'is_enrolled', 'is_reinstated', 'is_disenrolled', 'is_male', 'is_female',
            'is_gender_unknown', 'is_age_15_19', 'is_age_20_24', 'is_age_25_29',
            'is_age_30_34', 'is_age_35_39', 'is_age_40_44', 'is_age_45_49',
            'is_age_50_54', 'is_age_55_59', 'is_age_60_64', 'is_age_65_69',
            'is_age_70_74', 'is_age_75_79', 'is_age_80_84', 'is_age_85_89',
            'is_age_90_94', 'is_age_95_99', 'is_age_100_plus', 'is_abd', 'is_tanf',
            'is_expansion', 'is_dsnp', 'is_other', 'is_market_canton',
            'is_market_youngstown', 'is_market_memphis', 'is_market_dayton',
            'is_market_richmond', 'is_market_cleveland', 'is_market_chattanooga',
            'is_market_detroit', 'is_market_orlando', 'is_market_toledo',
            'is_market_upper_east_tn', 'is_market_akron', 'is_market_columbus',
            'is_market_nashville', 'is_market_jacksonville', 'is_market_cincinnati',
            'is_market_fairfax', 'is_market_support', 'is_market_tacoma',
            'is_market_miami', 'is_market_knoxville', 'is_market_southwest_virginia',
            'is_market_other', 'is_ever_selected', 'is_selected_month',
            'is_ever_engaged', 'is_engaged_month', 'is_tried_contact_in_month',
            'is_succefful_contact_in_month', 'is_intense_attempt_in_month',
            'is_intense_support_in_month', 'became_non_responsive',
            'became_responsive', 'increase_in_missed_calls', 'new_missed_calls',
            'started_texting', 'stopped_texting', 'started_intense_texting',
            'stopped_intense_texting', 'success_rate_increased',
            'success_rate_decreased', 'has_diabetes', 'has_mental_health',
            'has_cardiovascular', 'has_pulmonary', 'has_kidney', 'has_sud',
            'has_other_complex', 'first_diagnosis_diabetes',
            'first_diagnosis_mental_health', 'first_diagnosis_cardiovascular',
            'first_diagnosis_pulmonary', 'first_diagnosis_kidney',
            'first_diagnosis_sud', 'first_diagnosis_other_complex',
            'diabetes_treatment_required', 'mh_treatment_required',
            'cardio_treatment_required', 'pulmonary_treatment_required',
            'kidney_treatment_required', 'sud_treatment_required',
            'other_complex_treatment_required', 'diabetes_ever_treated',
            'diabetes_treated_last_3_months', 'diabetes_treated_this_month',
            'mh_ever_treated', 'mh_treated_last_3_months', 'mh_treated_this_month',
            'cardio_ever_treated', 'cardio_treated_last_3_months',
            'cardio_treated_this_month', 'pulmonary_ever_treated',
            'pulmonary_treated_last_3_months', 'pulmonary_treated_this_month',
            'kidney_ever_treated', 'kidney_treated_last_3_months',
            'kidney_treated_this_month', 'sud_ever_treated',
            'sud_treated_last_3_months', 'sud_treated_this_month',
            'other_complex_ever_treated',
            'other_complex_treated_last_3_months',
            'other_complex_treated_this_month',
            'diabetes_coordinated_care_last_3_months',
            'diabetes_new_provider_this_month', 'mh_coordinated_care_last_3_months',
            'mh_new_provider_this_month',
            'cardio_coordinated_care_last_3_months',
            'cardio_new_provider_this_month',
            'pulmonary_coordinated_care_last_3_months',
            'pulmonary_new_provider_this_month',
            'kidney_coordinated_care_last_3_months',
            'kidney_new_provider_this_month',
            'sud_coordinated_care_last_3_months',
            'sud_new_provider_this_month',
            'other_complex_coordinated_care_last_3_months',
            'other_complex_new_provider_this_month',
            'insulin_ever_prescribed', 'oral_antidiabetic_ever_prescribed',
            'beta_blocker_ever_prescribed', 'opiate_ever_prescribed',
            'antipsych_ever_prescribed',
            'insulin_med_adherent', 'oral_antidiabetic_med_adherent',
            'beta_blocker_med_adherent', 'opiate_med_adherent',
            'antipsych_med_adherent', 'insulin_missed_refill_this_month',
            'oral_antidiabetic_missed_refill_this_month',
            'beta_blocker_missed_refill_this_month',
            'opiate_missed_refill_this_month',
            'antipsych_missed_refill_this_month',
            'insulin_new_drug_this_month', 'oral_antidiabetic_new_drug_this_month',
            'beta_blocker_new_drug_this_month', 'opiate_new_drug_this_month',
            'antipsych_new_drug_this_month', 'had_ed_event_this_month',
            'had_ip_event_this_month', 'had_ed_event_last_3_months',
            'had_ip_event_last_3_months',
            'had_deteriorating_condition_this_month',
            'had_medication_concern_this_month',
            'had_medical_needs_this_month',
            'needed_triage_escalation_this_month',
            'triage_escalation_resolved_this_month',
            'triage_escalation_unresolved_this_month',
            'self_present_this_month', 'self_score_high_this_month',
            'med_adherence_present_this_month',
            'med_adherence_score_high_this_month',
            'health_present_this_month', 'health_score_high_this_month',
            'care_engagement_present_this_month',
            'care_engagement_score_high_this_month',
            'program_trust_present_this_month',
            'program_trust_score_high_this_month',
            'risk_harm_present_this_month',
            'risk_harm_score_high_this_month',
            'social_stability_present_this_month',
            'social_stability_score_high_this_month',
            'ed_event_next_30d', 'ed_event_next_60d', 'ed_event_next_90d',
            'ip_event_next_30d', 'ip_event_next_60d', 'ip_event_next_90d'
        ]
        df[boolean_cols_to_convert] = df[boolean_cols_to_convert].astype('float64')
        
        # Run the full pipeline
        run_pipeline(df, "Master Dataset Analysis", target, base_output_dir, logger)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        logger.info("Pipeline finished.")

if __name__ == "__main__":
    main()