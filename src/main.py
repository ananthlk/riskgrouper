print("Script started")

# To run this script, you need to install the following libraries:
# pip install pandas scikit-learn xgboost shap


import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
import os
import shap



def prepare_and_run_models(df, analysis_name, target='y_any_90d', base_output='model_output.csv'):
    """
    Prepares data and runs Logistic Regression and XGBoost models.

    Args:
        df (pd.DataFrame): The DataFrame to analyze.
        analysis_name (str): A descriptive name for the analysis.
        target (str): The name of the target variable column.
    """
    if df.empty:
        print(f"\nNo data to analyze for '{analysis_name}'. Skipping model training.")
        return

    print(f"\n--- Running Analysis for: {analysis_name} ---")

    # A) Basic data insights on the filtered dataset
    print("\n--- Basic Data Insights ---")
    print(f"DataFrame shape: {df.shape}")
    print("\nDataFrame info:")
    df.info()
    print("\nDataFrame descriptive statistics:")
    print(df.describe(include='all'))
    
    # Print all columns in the DataFrame for debugging
    print("\nAvailable columns in the DataFrame:")
    print(list(df.columns))
    
    # Split data based on 'dataset_split' column
    train_df = df[df['dataset_split'] == 'TRAIN'].copy()
    val_df = df[df['dataset_split'] == 'TEST'].copy()
    
    # Drop rows with NaN in the target column
    train_df = train_df.dropna(subset=[target])
    val_df = val_df.dropna(subset=[target])

    # Identify features (X) and labels (y)
    # The columns below are assumed to be in your prepared dataset from Snowflake.
    # Updated to explicitly drop string-based date columns.
    non_feature_cols = [
        'member_id', 'event_date', 'dob', 'dataset_split', 'period',
        'gender', 'engagement_group', 'normalized_coverage_category',
        'y_ed_30d', 'y_ed_60d', 'y_ed_90d', 'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
        'y_any_30d', 'y_any_60d', 'y_any_90d', 'has_care_notes_post_period'
    ]
    
    # Ensure any non-numeric columns are dropped
    # This is a more robust way to prevent the ValueError we saw earlier
    features = [col for col in df.columns if col not in non_feature_cols]

    # Prepare training and validation data
    X_train = train_df[features].fillna(0).select_dtypes(include=['number'])
    y_train = train_df[target]
    
    X_val = val_df[features].fillna(0).select_dtypes(include=['number'])
    y_val = val_df[target]
    
    print(f"Training features shape: {X_train.shape}")
    print(f"Validation features shape: {X_val.shape}")

    
    # B) Running a Baseline Model (Logistic Regression)
    print("\n--- Running Logistic Regression Baseline ---")
    lr_model = LogisticRegression(solver='liblinear', random_state=42)
    lr_model.fit(X_train, y_train)
    lr_pred = lr_model.predict(X_val)
    
    # Evaluate the model's performance
    lr_accuracy = accuracy_score(y_val, lr_pred)
    print(f"Accuracy: {lr_accuracy:.4f}")
    print("\nClassification Report (Logistic Regression):")
    print(classification_report(y_val, lr_pred, zero_division=0))

    # C) TRAIN THE XGBOOST MODEL
    print("\n--- Training XGBoost model with Hyperparameter Tuning and Early Stopping ---")

    # Calculate weight for the positive class to handle imbalance
    scale_pos_weight = y_train.value_counts()[0] / y_train.value_counts()[1]
    print(f"Calculated scale_pos_weight for imbalance: {scale_pos_weight:.2f}")

    # Define the parameter grid for GridSearchCV
    param_grid = {
        'n_estimators': [50, 100],
        'max_depth': [3, 5],
        'learning_rate': [0.1, 0.01],
    }

    # Initialize the XGBoost classifier
    model = XGBClassifier(
        objective='binary:logistic',
        eval_metric='auc',
        use_label_encoder=False,
        scale_pos_weight=scale_pos_weight,
        random_state=42
    )

    # Set up GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        scoring='roc_auc', # Use AUC as the scoring metric for tuning
        cv=2, # Use 2-fold cross-validation
        verbose=1,
        n_jobs=-1 # Use all available cores
    )

    # Fit GridSearchCV to the training data
    grid_search.fit(X_train, y_train)
    best_model = grid_search.best_estimator_

    print("\nHyperparameter Tuning Complete.")
    print(f"Best parameters found: {grid_search.best_params_}")
    
    # Train the best model with early stopping on the full training set
    print("\nTraining best model on the full training set...")
    best_model.fit(
        X_train, y_train,
        verbose=False
    )

    # D) EVALUATE THE MODEL ON TRAINING AND TEST SETS
    print("\n--- Training Set Performance ---")
    y_train_pred_proba = best_model.predict_proba(X_train)[:, 1]
    y_train_pred = best_model.predict(X_train)
    print(f"Accuracy: {accuracy_score(y_train, y_train_pred):.4f}")
    print(f"AUC Score: {roc_auc_score(y_train, y_train_pred_proba):.4f}")
    print("\nClassification Report:")
    print(classification_report(y_train, y_train_pred, zero_division=0))

    print("\n--- Validation Set Performance ---")
    y_val_pred_proba = best_model.predict_proba(X_val)[:, 1]
    y_val_pred = best_model.predict(X_val)
    print(f"Accuracy: {accuracy_score(y_val, y_val_pred):.4f}")
    print(f"AUC Score: {roc_auc_score(y_val, y_val_pred_proba):.4f}")
    print("\nConfusion Matrix:")
    print(pd.DataFrame(confusion_matrix(y_val, y_val_pred),
                       index=['Actual Negative', 'Actual Positive'],
                       columns=['Predicted Negative', 'Predicted Positive']))
    print("\nClassification Report:")
    print(classification_report(y_val, y_val_pred, zero_division=0))

    # Save model predictions and feature importances to a flat file
    print("\nSaving model output and feature importances...")
    output_path = input("Enter the path to save the output CSV file: ")
    val_df_copy = val_df.copy()
    val_df_copy['predicted_proba'] = best_model.predict_proba(X_val)[:, 1]
    val_df_copy['predicted_label'] = best_model.predict(X_val)
    # Save feature importances as a separate file
    importances_df = pd.DataFrame({
        'feature': X_val.columns,
        'importance': best_model.feature_importances_
    })
    val_df_copy.to_csv(output_path, index=False)
    importances_df.to_csv(output_path.replace('.csv', '_feature_importances.csv'), index=False)
    print(f"Model predictions saved to {output_path}")
    print(f"Feature importances saved to {output_path.replace('.csv', '_feature_importances.csv')}")

    # --- Part E: SHAP Analysis ---
    print("\n--- Running SHAP Analysis for Feature Interpretability ---")
    # Initialize the SHAP Explainer
    explainer = shap.TreeExplainer(best_model)
    # Calculate SHAP values for the validation set
    shap_values = explainer.shap_values(X_val)
    # Create and display the SHAP summary plot
    print("\nSHAP Summary Plot:")
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    shap.summary_plot(shap_values, X_val, show=False)
    shap_path = os.path.splitext(base_output)[0] + f"_{target}_SHAP.png"
    plt.savefig(shap_path)
    plt.close()
    print(f"SHAP summary plot saved to {shap_path}")

    # --- FINAL STEP: Identify top individuals by predicted score ---
    top_n = 20 if len(val_df_copy) > 20 else len(val_df_copy)
    top_individuals = val_df_copy[['member_id', 'predicted_proba']].sort_values('predicted_proba', ascending=False)
    top_out = os.path.splitext(base_output)[0] + f"_{target}_top.csv"
    top_individuals.head(top_n).to_csv(top_out, index=False)
    print(f"Top {top_n} individuals for {target} saved to {top_out}")

    # --- Save summary results to a TXT file for each target ---
    summary_path = os.path.splitext(base_output)[0] + f"_{target}_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"Dependent variable: {target}\n")
        f.write("\n--- Logistic Regression Results (Validation) ---\n")
        f.write(f"Accuracy: {lr_accuracy:.4f}\n")
        f.write("Classification Report:\n")
        f.write(classification_report(y_val, lr_pred, zero_division=0))
        f.write("\n\n--- XGBoost Training Results ---\n")
        f.write(f"Accuracy: {accuracy_score(y_train, y_train_pred):.4f}\n")
        f.write(f"AUC Score: {roc_auc_score(y_train, y_train_pred_proba):.4f}\n")
        f.write("Confusion Matrix:\n")
        f.write(str(pd.DataFrame(confusion_matrix(y_train, y_train_pred),
                                index=['Actual Negative', 'Actual Positive'],
                                columns=['Predicted Negative', 'Predicted Positive'])))
        f.write("\nClassification Report:\n")
        f.write(classification_report(y_train, y_train_pred, zero_division=0))
        f.write("\n\n--- XGBoost Validation Results ---\n")
        f.write(f"Accuracy: {accuracy_score(y_val, y_val_pred):.4f}\n")
        f.write(f"AUC Score: {roc_auc_score(y_val, y_val_pred_proba):.4f}\n")
        f.write("Confusion Matrix:\n")
        f.write(str(pd.DataFrame(confusion_matrix(y_val, y_val_pred),
                                index=['Actual Negative', 'Actual Positive'],
                                columns=['Predicted Negative', 'Predicted Positive'])))
        f.write("\nClassification Report:\n")
        f.write(classification_report(y_val, y_val_pred, zero_division=0))
        f.write("\n\n--- SHAP Feature Importances (XGBoost) ---\n")
        shap_importances = pd.DataFrame({
            'feature': X_val.columns,
            'importance': best_model.feature_importances_
        })
        f.write(shap_importances.to_string(index=False))
        f.write("\n\n--- Top Individuals by Predicted Score ---\n")
        f.write(top_individuals.head(top_n).to_string(index=False))
    print(f"Summary results for {target} saved to {summary_path}")


def main():
    """
    Main function to orchestrate the three types of analyses.
    """
    print("Local ML Model Runner (XGBoost)")
    print("=" * 40)

    # --- Step 1: Prompt the user for a choice ---
    choice = None
    while choice not in ['1', '2', '3', '4', '5']:
        print("\nWhat type of analysis would you like to run?")
        print("1. Claims Only Predictive Analysis")
        print("2. Claims + Notes Predictive Analysis (All Data)")
        print("3. Combined Claims + Notes Analysis")
        print("4. Exit")
        print("5. Notes Only Analysis (HAS_CARE_NOTES_POST_PERIOD = 1)")
        choice = input("Enter your choice (1, 2, 3, 4, or 5): ")

    # --- Step 2: Read the data from a local file based on user's choice ---
    df = pd.DataFrame()
    file_path = ""
    
    if choice == '1':
        file_path = "/Users/ananth/Downloads/claims_only.csv"
        try:
            print(f"\nFetching data from '{file_path}'...")
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            print(f"Successfully loaded {len(df)} records.")
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found. Please check the file path and name.")
            return
        base_output = input("Enter the base output file name (e.g., /path/to/output.csv): ")
        target_vars = [
            'y_ed_30d', 'y_ed_60d', 'y_ed_90d',
            'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
            'y_any_30d', 'y_any_60d', 'y_any_90d'
        ]
        print(f"\nAll outputs will be saved with the base file name: {base_output}")
        for target in target_vars:
            print(f"\nRunning analysis for target: {target}")
            prepare_and_run_models(df, f"Claims Only Analysis - {target}", target=target, base_output=base_output)
    elif choice == '2':
        file_path = "/Users/ananth/Downloads/claims_and_notes.csv"
        try:
            print(f"\nFetching data from '{file_path}'...")
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            print(f"Successfully loaded {len(df)} records.")
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found. Please check the file path and name.")
            return
        base_output = input("Enter the base output file name (e.g., /path/to/output.csv): ")
        target_vars = [
            'y_ed_30d', 'y_ed_60d', 'y_ed_90d',
            'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
            'y_any_30d', 'y_any_60d', 'y_any_90d'
        ]
        print(f"\nAll outputs will be saved with the base file name: {base_output}")
        for target in target_vars:
            print(f"\nRunning analysis for target: {target}")
            prepare_and_run_models(df, f"Claims + Notes Analysis - {target}", target=target, base_output=base_output)
    elif choice == '3':
        file_path1 = "/Users/ananth/Downloads/claims_only.csv"
        file_path2 = "/Users/ananth/Downloads/claims_and_notes.csv"
        try:
            print(f"\nFetching data from '{file_path1}'...")
            df1 = pd.read_csv(file_path1)
            df1.columns = [col.lower() for col in df1.columns]
            print(f"Successfully loaded {len(df1)} records from claims only.")
            print(f"\nFetching data from '{file_path2}'...")
            df2 = pd.read_csv(file_path2)
            df2.columns = [col.lower() for col in df2.columns]
            print(f"Successfully loaded {len(df2)} records from claims and notes.")
            # Combine the two datasets
            df_combined = pd.concat([df1, df2], ignore_index=True)
            print(f"Combined dataset has {len(df_combined)} records.")
        except FileNotFoundError:
            print(f"Error: One of the files was not found. Please check the file paths and names.")
            return
        base_output = input("Enter the base output file name (e.g., /path/to/output.csv): ")
        target_vars = [
            'y_ed_30d', 'y_ed_60d', 'y_ed_90d',
            'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
            'y_any_30d', 'y_any_60d', 'y_any_90d'
        ]
        print(f"\nAll outputs will be saved with the base file name: {base_output}")
        for target in target_vars:
            print(f"\nRunning analysis for target: {target}")
            prepare_and_run_models(df_combined, f"Combined Claims + Notes Analysis - {target}", target=target, base_output=base_output)
    elif choice == '4':
        print("\nExiting script.")
        return
    elif choice == '5':
        file_path = "/Users/ananth/Downloads/claims_and_notes.csv"
        try:
            print(f"\nFetching data from '{file_path}'...")
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            print(f"Successfully loaded {len(df)} records.")
            # Case-insensitive filter for HAS_CARE_NOTES_POST_PERIOD
            col_candidates = [col for col in df.columns if col.lower() == 'has_care_notes_post_period']
            if col_candidates:
                notes_col = col_candidates[0]
                df = df[df[notes_col] == 1]
                print(f"Filtered to {len(df)} records with HAS_CARE_NOTES_POST_PERIOD = 1.")
            else:
                print("Column 'HAS_CARE_NOTES_POST_PERIOD' not found in file. Skipping filter.")
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found. Please check the file path and name.")
            return
        base_output = input("Enter the base output file name (e.g., /path/to/output.csv): ")
        target_vars = [
            'y_ed_30d', 'y_ed_60d', 'y_ed_90d',
            'y_ip_30d', 'y_ip_60d', 'y_ip_90d',
            'y_any_30d', 'y_any_60d', 'y_any_90d'
        ]
        print(f"\nAll outputs will be saved with the base file name: {base_output}")
        for target in target_vars:
            print(f"\nRunning analysis for target: {target}")
            prepare_and_run_models(df, f"Notes Only Analysis - {target}", target=target, base_output=base_output)

if __name__ == "__main__":
    main()
