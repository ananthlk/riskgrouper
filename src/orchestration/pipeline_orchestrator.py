"""
Orchestration module for two-model ML pipeline (Logistic Regression & XGBoost).
Coordinates data prep, model training, prediction, metrics, SHAP explainability, and output writing.
Strict reproducibility, CLI/config integration, and modular design.
"""

import logging
import shap
from src.config import DEFAULT_CONFIG
from src.data_prep.splitter import split_data
from src.data_prep.features import select_features
from src.models.metrics import compute_metrics
from src.models.shap_tools import get_shap_explainer, extract_local_factors, extract_global_factors
from src.reporting.writers import write_metrics, write_confusion_matrix
from src.reporting.individuals import write_consolidated_individuals
from src.snowflake_connector import fetch_snowflake_data

class PipelineOrchestrator:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("PipelineOrchestrator")
        self.logger.setLevel(logging.INFO)

        # Clear log file based on .env variable
        import os
        from dotenv import load_dotenv
        load_dotenv()
        clear_log = os.getenv("CLEAR_LOG_ON_START", "true").lower() == "true"
        file_mode = "w" if clear_log else "a"

        # File handler
        file_handler = logging.FileHandler("logs/pipeline.log", mode=file_mode)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        file_handler.setFormatter(file_formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s %(name)s: %(message)s')
        console_handler.setFormatter(console_formatter)

        # Always add handlers to ensure console/file output
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    _has_run = False

    def run(self):
        if PipelineOrchestrator._has_run:
            print("PipelineOrchestrator.run() has already been executed in this process. Aborting duplicate run.")
            self.logger.warning("Duplicate pipeline run detected. Aborting.")
            return
        PipelineOrchestrator._has_run = True
        """
        Main orchestration entry point.
        Steps:
        1. Data prep (split, feature selection)
        2. Model training (Logistic, XGBoost)
        3. Prediction
        4. Metrics calculation
        5. SHAP explainability
        6. Output writing
        """

        print("PipelineOrchestrator.run() invoked!")
        self.logger.info("Logger activated: pipeline start.")
        self.logger.info("Starting pipeline orchestration.")

        # Ask user if they want to permanently store outputs
        store_outputs = input("Do you want to permanently store the outputs? (yes/no): ").strip().lower()
        run_name = None
        if store_outputs == "yes":
            run_name = input("Enter a run name for permanent storage: ").strip()

        # 0. Establish Snowflake connection (kept open for entire pipeline)
        self.logger.info("Connecting to Snowflake.")
        from src.snowflake_connector import SnowflakeConnector
        sf_connector = SnowflakeConnector()
        if not sf_connector.connect():
            self.logger.error("Failed to connect to Snowflake.")
            raise RuntimeError("Snowflake connection failed.")

        try:
            # 1. Fetch Data from Snowflake
            table_name = input("Enter Snowflake table name to fetch data: ").strip()
            query = f"SELECT * FROM {table_name}"
            raw_data = sf_connector.query_to_dataframe(query)
            if raw_data is None:
                raise RuntimeError(f"No data returned for table: {table_name}")

            # 2. Data Preparation
            self.logger.info("Splitting data and selecting features.")
            data_splits = split_data(raw_data, self.config)
            features, skipped = select_features(raw_data, self.config['EXCLUDE_FEATURES'], self.logger)
            # Prepare training data
            train_df = data_splits['train']
            X_train = train_df[features].copy()
            X_train = X_train.fillna(X_train.mean())
            y_train = train_df[self.config['TARGET']]
            # Log class distribution
            class_counts = y_train.value_counts().to_dict()
            self.logger.info(f"Training class distribution: {class_counts}")
            # Use original train_df for both models
            data_splits['train_logistic'] = train_df
            data_splits['train_xgb'] = train_df

            # 3. Model Training
            self.logger.info("Training Logistic Regression model.")
            logistic_model = None
            xgb_model = None
            try:
                from src.models.logistic import train_logistic
                logistic_model = train_logistic(data_splits['train_logistic'], features, self.config)
            except ImportError:
                self.logger.warning("train_logistic not implemented yet.")

            self.logger.info("Training XGBoost model.")
            try:
                from src.models.xgboost import train_xgboost
                xgb_model = train_xgboost(data_splits['train_xgb'], features, self.config)
            except ImportError:
                self.logger.warning("train_xgboost not implemented yet.")

            # 4. Prediction
            self.logger.info("Generating predictions.")
            logistic_preds = None
            xgb_preds = None
            try:
                X_test = data_splits['test'][features]
                X_test_logistic = X_test.copy().fillna(0)
                X_test_xgb = X_test.copy()  # XGBoost can handle NaNs
                if logistic_model:
                    logistic_preds = logistic_model.predict_proba(X_test_logistic)[:, 1]
                if xgb_model:
                    xgb_preds = xgb_model.predict_proba(X_test_xgb)[:, 1]
            except Exception as e:
                self.logger.warning(f"Prediction step failed: {e}")

            # 4. Metrics Calculation
            self.logger.info("Calculating metrics for all splits.")
            import pandas as pd
            threshold = 0.5
            output_dir = 'output/metrics'
            splits = ['train', 'test']
            if 'validate' in data_splits:
                splits.append('validate')
            metrics_logistic = {}
            metrics_xgb = {}
            for split_name in splits:
                split_df = data_splits[split_name]
                y_true = split_df[self.config['TARGET']].values
                X_split = split_df[features]
                # Logistic Regression
                if logistic_model is not None:
                    X_split_logistic = X_split.copy().fillna(0)
                    y_prob_logistic = logistic_model.predict_proba(X_split_logistic)[:, 1]
                    y_pred_logistic = (y_prob_logistic >= threshold).astype(int)
                    metrics_logistic[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_logistic,
                        y_prob=y_prob_logistic,
                        threshold=threshold,
                        model_name='logistic',
                        split_name=split_name,
                        output_dir=output_dir
                    )
                    if split_name == 'test':
                        # Print and save coefficients only for test split
                        try:
                            coef = logistic_model.named_steps['logreg'].coef_[0]
                            intercept = logistic_model.named_steps['logreg'].intercept_[0]
                            coef_pairs = sorted(zip(features, coef), key=lambda x: abs(x[1]), reverse=True)
                            self.logger.info("Logistic Regression coefficients (sorted by absolute value):")
                            for fname, val in coef_pairs:
                                self.logger.info(f"  {fname}: {val:.6f}")
                            self.logger.info(f"Intercept: {intercept:.6f}")
                            print("Logistic Regression coefficients:")
                            for fname, val in coef_pairs:
                                print(f"  {fname}: {val:.6f}")
                            print(f"Intercept: {intercept:.6f}")
                            coef_df = pd.DataFrame({
                                'feature': [fname for fname, _ in coef_pairs] + ['intercept'],
                                'coefficient': [val for _, val in coef_pairs] + [intercept]
                            })
                            coef_df.to_csv('output/logistic_coefficients.csv', index=False)
                        except Exception as e:
                            self.logger.warning(f"Could not print logistic coefficients: {e}")
                # XGBoost
                if xgb_model is not None:
                    X_split_xgb = X_split.copy()
                    y_prob_xgb = xgb_model.predict_proba(X_split_xgb)[:, 1]
                    y_pred_xgb = (y_prob_xgb >= threshold).astype(int)
                    metrics_xgb[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_xgb,
                        y_prob=y_prob_xgb,
                        threshold=threshold,
                        model_name='xgboost',
                        split_name=split_name,
                        output_dir=output_dir
                    )
            # Write metrics for all splits using updated writer
            write_metrics(metrics_logistic, metrics_xgb, self.config)

            # --- Precision-Recall Curve Automation ---
            import numpy as np
            import matplotlib.pyplot as plt
            from sklearn.metrics import precision_recall_curve, average_precision_score
            pr_output_dir = 'output/metrics'
            import os
            os.makedirs(pr_output_dir, exist_ok=True)
            for model_name, preds_dict, metrics_dict in [
                ('logistic', {'probs': logistic_preds, 'model': logistic_model}, metrics_logistic),
                ('xgboost', {'probs': xgb_preds, 'model': xgb_model}, metrics_xgb)
            ]:
                if preds_dict['probs'] is not None:
                    y_true = data_splits['test'][self.config['TARGET']].values
                    y_scores = preds_dict['probs']
                    precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
                    avg_prec = average_precision_score(y_true, y_scores)
                    # Plot PR curve
                    plt.figure()
                    plt.plot(recall, precision, label=f'PR curve (AP={avg_prec:.2f})')
                    plt.xlabel('Recall')
                    plt.ylabel('Precision')
                    plt.title(f'Precision-Recall Curve: {model_name}')
                    plt.legend()
                    pr_curve_path = os.path.join(pr_output_dir, f'pr_curve_{model_name}.png')
                    plt.savefig(pr_curve_path)
                    plt.close()
                    # Save precision/recall/thresholds table
                    pr_table = np.vstack([thresholds, precision[:-1], recall[:-1]]).T
                    pr_table_path = os.path.join(pr_output_dir, f'pr_table_{model_name}.csv')
                    import pandas as pd
                    pr_df = pd.DataFrame(pr_table, columns=['threshold', 'precision', 'recall'])
                    pr_df.to_csv(pr_table_path, index=False)
                    # Log summary at key thresholds
                    self.logger.info(f"Precision-Recall summary for {model_name}:")
                    for thresh in [0.2, 0.5, 0.8]:
                        idx = np.searchsorted(thresholds, thresh)
                        if idx < len(precision):
                            self.logger.info(f"  Threshold={thresh:.2f}: Precision={precision[idx]:.2f}, Recall={recall[idx]:.2f}")
                    self.logger.info(f"PR curve and table saved for {model_name}.")
                    # --- Compute and report optimal threshold (max F1-score) ---
                    f1_scores = 2 * (precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-10)
                    optimal_idx = np.argmax(f1_scores)
                    optimal_threshold = thresholds[optimal_idx]
                    optimal_f1 = f1_scores[optimal_idx]
                    self.logger.info(f"Optimal threshold for {model_name}: {optimal_threshold:.4f} (F1-score: {optimal_f1:.4f})")
                    print(f"Optimal threshold for {model_name}: {optimal_threshold:.4f} (F1-score: {optimal_f1:.4f})")
                    self.logger.info(f"Threshold range: min={thresholds.min():.4f}, max={thresholds.max():.4f}, count={len(thresholds)}")
                    print(f"Threshold range for {model_name}: min={thresholds.min():.4f}, max={thresholds.max():.4f}, count={len(thresholds)}")

            # --- SHAP Summary Plot for XGBoost ---
            if xgb_model is not None and 'test' in data_splits:
                try:
                    import shap
                    X_test_xgb = data_splits['test'][features].copy()
                    explainer_xgb = shap.Explainer(xgb_model, X_test_xgb)
                    shap_values = explainer_xgb(X_test_xgb)
                    plt.figure()
                    shap.summary_plot(shap_values, X_test_xgb, show=False)
                    shap_plot_path = os.path.join('output', 'shap_xgb_summary.png')
                    plt.savefig(shap_plot_path, bbox_inches='tight')
                    plt.close()
                    self.logger.info(f"SHAP summary plot saved to {shap_plot_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to generate SHAP summary plot for XGBoost: {e}")

            # SHAP for logistic regression removed as requested
            # ...existing code...

            # 7. Output Writing
            self.logger.info("Writing outputs.")
            try:
                write_confusion_matrix(metrics_logistic, metrics_xgb, self.config)
                # --- New consolidated individual file logic ---
                from src.reporting.individuals import write_full_consolidated_individuals
                # Prepare dicts for all splits
                splits_dict = {k: v for k, v in data_splits.items() if k in ['train', 'test', 'val', 'validate']}
                # Prepare prediction dicts
                logistic_preds_dict = {}
                xgb_preds_dict = {}
                shap_xgb_dict = {}
                logistic_coeffs = {}
                # For each split, map fh_id to predictions
                for split_name, split_df in splits_dict.items():
                    if split_df is not None:
                        fh_ids = split_df['FH_ID'] if 'FH_ID' in split_df.columns else split_df['fh_id']
                        # Logistic preds
                        if logistic_model is not None:
                            X_split_logistic = split_df[features].copy().fillna(0)
                            y_prob_logistic = logistic_model.predict_proba(X_split_logistic)[:, 1]
                            logistic_preds_dict[split_name] = dict(zip(fh_ids, y_prob_logistic))
                        # XGBoost preds
                        if xgb_model is not None:
                            X_split_xgb = split_df[features].copy()
                            y_prob_xgb = xgb_model.predict_proba(X_split_xgb)[:, 1]
                            xgb_preds_dict[split_name] = dict(zip(fh_ids, y_prob_xgb))
                        # SHAP XGB (extract local SHAP values for each individual)
                        shap_xgb_dict[split_name] = {}
                        if xgb_model is not None:
                            try:
                                from src.models.shap_tools import get_shap_explainer, extract_local_factors
                                X_split_xgb = split_df[features].copy()
                                explainer_xgb = get_shap_explainer(xgb_model, X_split_xgb, 'xgboost', 'output/shap_xgb.pkl', self.logger, self.config)
                                shap_df = extract_local_factors(explainer_xgb, X_split_xgb, self.config, set())
                                for i, shap_row in shap_df.iterrows():
                                    fh_id_val = fh_ids.iloc[i] if hasattr(fh_ids, 'iloc') else fh_ids[i]
                                    shap_xgb_dict[split_name][fh_id_val] = {
                                        'base_value': shap_row.get('base_value', 0.0),
                                        'contributions': [c for _, c in shap_row.get('increasing_factors', [])] + [c for _, c in shap_row.get('decreasing_factors', [])]
                                    }
                                # Debug: print SHAP contributions for first 3 individuals in this split
                                self.logger.info(f"Sample SHAP contributions for split {split_name}:")
                                count = 0
                                for fh_id_val, shap_info in shap_xgb_dict[split_name].items():
                                    self.logger.info(f"fh_id: {fh_id_val}, base_value: {shap_info['base_value']}, contributions: {shap_info['contributions']}")
                                    count += 1
                                    if count >= 3:
                                        break
                            except Exception as e:
                                self.logger.warning(f"Failed to extract XGBoost SHAP values for split {split_name}: {e}")
                        # Logistic coefficients (same for all individuals)
                        if logistic_model is not None:
                            try:
                                coef = logistic_model.named_steps['logreg'].coef_[0]
                                intercept = logistic_model.named_steps['logreg'].intercept_[0]
                                coeffs = {f: float(c) for f, c in zip(features, coef)}
                                coeffs['intercept'] = float(intercept)
                                for fh_id in fh_ids:
                                    logistic_coeffs[fh_id] = coeffs
                            except Exception:
                                pass
                write_full_consolidated_individuals(
                    splits=splits_dict,
                    logistic_preds_dict=logistic_preds_dict,
                    xgb_preds_dict=xgb_preds_dict,
                    shap_xgb_dict=shap_xgb_dict,
                    logistic_coeffs=logistic_coeffs,
                    cfg=self.config
                )
                # --- End new logic ---
                # Old per-split individual file generation commented out
                # write_consolidated_individuals(
                #     data_splits['test'],
                #     logistic_preds,
                #     xgb_preds,
                #     shap_logistic,
                #     shap_xgb,
                #     self.config
                # )
            except Exception as e:
                self.logger.warning(f"Output writing failed: {e}")

            self.logger.info("Pipeline orchestration complete.")
            # If user requested permanent storage, copy outputs to run folder (after all outputs are written)
            if run_name:
                import os
                import shutil
                run_folder = os.path.join("output", run_name)
                os.makedirs(run_folder, exist_ok=True)
                files_to_copy = [
                    "output/metrics/metrics.json",
                    "output/logistic_coefficients.csv",
                    "output/shap_xgb.pkl",
                    "output/individual_full_consolidated.csv",
                    "output/shap_xgb_summary.png",
                    "output/metrics/pr_curve_logistic.png",
                    "output/metrics/pr_curve_xgboost.png",
                    "output/metrics/pr_table_logistic.csv",
                    "output/metrics/pr_table_xgboost.csv"
                ]
                for f in files_to_copy:
                    if os.path.exists(f):
                        shutil.copy(f, run_folder)
                print(f"Permanent outputs saved to: {run_folder}")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
        finally:
            # Close Snowflake connection after all steps are complete
            sf_connector.close()
        """
        Main orchestration entry point.
        Steps:
        1. Data prep (split, feature selection)
        2. Model training (Logistic, XGBoost)
        3. Prediction
        4. Metrics calculation
        5. SHAP explainability
        6. Output writing
        """

        print("PipelineOrchestrator.run() invoked!")
        self.logger.info("Logger activated: pipeline start.")
        self.logger.info("Starting pipeline orchestration.")

        # Ask user if they want to permanently store outputs
        store_outputs = input("Do you want to permanently store the outputs? (yes/no): ").strip().lower()
        run_name = None
        if store_outputs == "yes":
            run_name = input("Enter a run name for permanent storage: ").strip()

        # 0. Establish Snowflake connection (kept open for entire pipeline)
        self.logger.info("Connecting to Snowflake.")
        from src.snowflake_connector import SnowflakeConnector
        sf_connector = SnowflakeConnector()
        if not sf_connector.connect():
            self.logger.error("Failed to connect to Snowflake.")
            raise RuntimeError("Snowflake connection failed.")

        try:
            # 1. Fetch Data from Snowflake
            table_name = input("Enter Snowflake table name to fetch data: ").strip()
            query = f"SELECT * FROM {table_name}"
            raw_data = sf_connector.query_to_dataframe(query)
            if raw_data is None:
                raise RuntimeError(f"No data returned for table: {table_name}")

            # 2. Data Preparation
            self.logger.info("Splitting data and selecting features.")
            data_splits = split_data(raw_data, self.config)
            features, skipped = select_features(raw_data, self.config['EXCLUDE_FEATURES'], self.logger)
            # Prepare training data
            train_df = data_splits['train']
            X_train = train_df[features].copy()
            X_train = X_train.fillna(X_train.mean())
            y_train = train_df[self.config['TARGET']]
            # Log class distribution
            class_counts = y_train.value_counts().to_dict()
            self.logger.info(f"Training class distribution: {class_counts}")
            # Use original train_df for both models
            data_splits['train_logistic'] = train_df
            data_splits['train_xgb'] = train_df

            # 3. Model Training
            self.logger.info("Training Logistic Regression model.")
            logistic_model = None
            xgb_model = None
            try:
                from src.models.logistic import train_logistic
                logistic_model = train_logistic(data_splits['train_logistic'], features, self.config)
            except ImportError:
                self.logger.warning("train_logistic not implemented yet.")

            self.logger.info("Training XGBoost model.")
            try:
                from src.models.xgboost import train_xgboost
                xgb_model = train_xgboost(data_splits['train_xgb'], features, self.config)
            except ImportError:
                self.logger.warning("train_xgboost not implemented yet.")

            # 4. Prediction
            self.logger.info("Generating predictions.")
            logistic_preds = None
            xgb_preds = None
            try:
                X_test = data_splits['test'][features]
                X_test_logistic = X_test.copy().fillna(0)
                X_test_xgb = X_test.copy()  # XGBoost can handle NaNs
                if logistic_model:
                    logistic_preds = logistic_model.predict_proba(X_test_logistic)[:, 1]
                if xgb_model:
                    xgb_preds = xgb_model.predict_proba(X_test_xgb)[:, 1]
            except Exception as e:
                self.logger.warning(f"Prediction step failed: {e}")

            # 4. Metrics Calculation
            self.logger.info("Calculating metrics for all splits.")
            import pandas as pd
            threshold = 0.5
            output_dir = 'output/metrics'
            splits = ['train', 'test']
            if 'validate' in data_splits:
                splits.append('validate')
            metrics_logistic = {}
            metrics_xgb = {}
            for split_name in splits:
                split_df = data_splits[split_name]
                y_true = split_df[self.config['TARGET']].values
                X_split = split_df[features]
                # Logistic Regression
                if logistic_model is not None:
                    X_split_logistic = X_split.copy().fillna(0)
                    y_prob_logistic = logistic_model.predict_proba(X_split_logistic)[:, 1]
                    y_pred_logistic = (y_prob_logistic >= threshold).astype(int)
                    metrics_logistic[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_logistic,
                        y_prob=y_prob_logistic,
                        threshold=threshold,
                        model_name='logistic',
                        split_name=split_name,
                        output_dir=output_dir
                    )
                    if split_name == 'test':
                        # Print and save coefficients only for test split
                        try:
                            coef = logistic_model.named_steps['logreg'].coef_[0]
                            intercept = logistic_model.named_steps['logreg'].intercept_[0]
                            coef_pairs = sorted(zip(features, coef), key=lambda x: abs(x[1]), reverse=True)
                            self.logger.info("Logistic Regression coefficients (sorted by absolute value):")
                            for fname, val in coef_pairs:
                                self.logger.info(f"  {fname}: {val:.6f}")
                            self.logger.info(f"Intercept: {intercept:.6f}")
                            print("Logistic Regression coefficients:")
                            for fname, val in coef_pairs:
                                print(f"  {fname}: {val:.6f}")
                            print(f"Intercept: {intercept:.6f}")
                            coef_df = pd.DataFrame({
                                'feature': [fname for fname, _ in coef_pairs] + ['intercept'],
                                'coefficient': [val for _, val in coef_pairs] + [intercept]
                            })
                            coef_df.to_csv('output/logistic_coefficients.csv', index=False)
                        except Exception as e:
                            self.logger.warning(f"Could not print logistic coefficients: {e}")
                # XGBoost
                if xgb_model is not None:
                    X_split_xgb = X_split.copy()
                    y_prob_xgb = xgb_model.predict_proba(X_split_xgb)[:, 1]
                    y_pred_xgb = (y_prob_xgb >= threshold).astype(int)
                    metrics_xgb[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_xgb,
                        y_prob=y_prob_xgb,
                        threshold=threshold,
                        model_name='xgboost',
                        split_name=split_name,
                        output_dir=output_dir
                    )
            # Write metrics for all splits using updated writer
            write_metrics(metrics_logistic, metrics_xgb, self.config)

            # --- Precision-Recall Curve Automation ---
            import numpy as np
            import matplotlib.pyplot as plt
            from sklearn.metrics import precision_recall_curve, average_precision_score
            pr_output_dir = 'output/metrics'
            os.makedirs(pr_output_dir, exist_ok=True)
            for model_name, preds_dict, metrics_dict in [
                ('logistic', {'probs': logistic_preds, 'model': logistic_model}, metrics_logistic),
                ('xgboost', {'probs': xgb_preds, 'model': xgb_model}, metrics_xgb)
            ]:
                if preds_dict['probs'] is not None:
                    y_true = data_splits['test'][self.config['TARGET']].values
                    y_scores = preds_dict['probs']
                    precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
                    avg_prec = average_precision_score(y_true, y_scores)
                    # Plot PR curve
                    plt.figure()
                    plt.plot(recall, precision, label=f'PR curve (AP={avg_prec:.2f})')
                    plt.xlabel('Recall')
                    plt.ylabel('Precision')
                    plt.title(f'Precision-Recall Curve: {model_name}')
                    plt.legend()
                    pr_curve_path = os.path.join(pr_output_dir, f'pr_curve_{model_name}.png')
                    plt.savefig(pr_curve_path)
                    plt.close()
                    # Save precision/recall/thresholds table
                    pr_table = np.vstack([thresholds, precision[:-1], recall[:-1]]).T
                    pr_table_path = os.path.join(pr_output_dir, f'pr_table_{model_name}.csv')
                    import pandas as pd
                    pr_df = pd.DataFrame(pr_table, columns=['threshold', 'precision', 'recall'])
                    pr_df.to_csv(pr_table_path, index=False)
                    # Log summary at key thresholds
                    self.logger.info(f"Precision-Recall summary for {model_name}:")
                    for thresh in [0.2, 0.5, 0.8]:
                        idx = np.searchsorted(thresholds, thresh)
                        if idx < len(precision):
                            self.logger.info(f"  Threshold={thresh:.2f}: Precision={precision[idx]:.2f}, Recall={recall[idx]:.2f}")
                    self.logger.info(f"PR curve and table saved for {model_name}.")

            # --- SHAP Summary Plot for XGBoost ---
            if xgb_model is not None and 'test' in data_splits:
                try:
                    import shap
                    X_test_xgb = data_splits['test'][features].copy()
                    explainer_xgb = shap.Explainer(xgb_model, X_test_xgb)
                    shap_values = explainer_xgb(X_test_xgb)
                    plt.figure()
                    shap.summary_plot(shap_values, X_test_xgb, show=False)
                    shap_plot_path = os.path.join('output', 'shap_xgb_summary.png')
                    plt.savefig(shap_plot_path, bbox_inches='tight')
                    plt.close()
                    self.logger.info(f"SHAP summary plot saved to {shap_plot_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to generate SHAP summary plot for XGBoost: {e}")

            # SHAP for logistic regression removed as requested
            # ...existing code...

            # 7. Output Writing
            self.logger.info("Writing outputs.")
            try:
                write_confusion_matrix(metrics_logistic, metrics_xgb, self.config)
                # --- New consolidated individual file logic ---
                from src.reporting.individuals import write_full_consolidated_individuals
                # Prepare dicts for all splits
                splits_dict = {k: v for k, v in data_splits.items() if k in ['train', 'test', 'val', 'validate']}
                # Prepare prediction dicts
                logistic_preds_dict = {}
                xgb_preds_dict = {}
                shap_xgb_dict = {}
                logistic_coeffs = {}
                # For each split, map fh_id to predictions
                for split_name, split_df in splits_dict.items():
                    if split_df is not None:
                        fh_ids = split_df['FH_ID'] if 'FH_ID' in split_df.columns else split_df['fh_id']
                        # Logistic preds
                        if logistic_model is not None:
                            X_split_logistic = split_df[features].copy().fillna(0)
                            y_prob_logistic = logistic_model.predict_proba(X_split_logistic)[:, 1]
                            logistic_preds_dict[split_name] = dict(zip(fh_ids, y_prob_logistic))
                        # XGBoost preds
                        if xgb_model is not None:
                            X_split_xgb = split_df[features].copy()
                            y_prob_xgb = xgb_model.predict_proba(X_split_xgb)[:, 1]
                            xgb_preds_dict[split_name] = dict(zip(fh_ids, y_prob_xgb))
                        # SHAP XGB (extract local SHAP values for each individual)
                        shap_xgb_dict[split_name] = {}
                        if xgb_model is not None:
                            try:
                                from src.models.shap_tools import get_shap_explainer, extract_local_factors
                                X_split_xgb = split_df[features].copy()
                                explainer_xgb = get_shap_explainer(xgb_model, X_split_xgb, 'xgboost', 'output/shap_xgb.pkl', self.logger, self.config)
                                shap_df = extract_local_factors(explainer_xgb, X_split_xgb, self.config, set())
                                for i, shap_row in shap_df.iterrows():
                                    fh_id_val = fh_ids.iloc[i] if hasattr(fh_ids, 'iloc') else fh_ids[i]
                                    shap_xgb_dict[split_name][fh_id_val] = {
                                        'base_value': shap_row.get('base_value', 0.0),
                                        'contributions': [c for _, c in shap_row.get('increasing_factors', [])] + [c for _, c in shap_row.get('decreasing_factors', [])]
                                    }
                                # Debug: print SHAP contributions for first 3 individuals in this split
                                self.logger.info(f"Sample SHAP contributions for split {split_name}:")
                                count = 0
                                for fh_id_val, shap_info in shap_xgb_dict[split_name].items():
                                    self.logger.info(f"fh_id: {fh_id_val}, base_value: {shap_info['base_value']}, contributions: {shap_info['contributions']}")
                                    count += 1
                                    if count >= 3:
                                        break
                            except Exception as e:
                                self.logger.warning(f"Failed to extract XGBoost SHAP values for split {split_name}: {e}")
                        # Logistic coefficients (same for all individuals)
                        if logistic_model is not None:
                            try:
                                coef = logistic_model.named_steps['logreg'].coef_[0]
                                intercept = logistic_model.named_steps['logreg'].intercept_[0]
                                coeffs = {f: float(c) for f, c in zip(features, coef)}
                                coeffs['intercept'] = float(intercept)
                                for fh_id in fh_ids:
                                    logistic_coeffs[fh_id] = coeffs
                            except Exception:
                                pass
                write_full_consolidated_individuals(
                    splits=splits_dict,
                    logistic_preds_dict=logistic_preds_dict,
                    xgb_preds_dict=xgb_preds_dict,
                    shap_xgb_dict=shap_xgb_dict,
                    logistic_coeffs=logistic_coeffs,
                    cfg=self.config
                )
                # --- End new logic ---
                # Old per-split individual file generation commented out
                # write_consolidated_individuals(
                #     data_splits['test'],
                #     logistic_preds,
                #     xgb_preds,
                #     shap_logistic,
                #     shap_xgb,
                #     self.config
                # )
            except Exception as e:
                self.logger.warning(f"Output writing failed: {e}")

            self.logger.info("Pipeline orchestration complete.")
            # If user requested permanent storage, copy outputs to run folder (after all outputs are written)
            if run_name:
                import os
                import shutil
                run_folder = os.path.join("output", run_name)
                os.makedirs(run_folder, exist_ok=True)
                files_to_copy = [
                    "output/metrics/metrics.json",
                    "output/logistic_coefficients.csv",
                    "output/shap_xgb.pkl",
                    "output/individual_full_consolidated.csv"
                ]
                for f in files_to_copy:
                    if os.path.exists(f):
                        shutil.copy(f, run_folder)
                print(f"Permanent outputs saved to: {run_folder}")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
        finally:
            # Close Snowflake connection after all steps are complete
            sf_connector.close()

            # 4. Metrics Calculation
            self.logger.info("Calculating metrics for all splits.")
            import pandas as pd
            threshold = 0.5
            output_dir = 'output/metrics'
            splits = ['train', 'test']
            if 'validate' in data_splits:
                splits.append('validate')
            metrics_logistic = {}
            metrics_xgb = {}
            for split_name in splits:
                split_df = data_splits[split_name]
                y_true = split_df[self.config['TARGET']].values
                X_split = split_df[features]
                # Logistic Regression
                if logistic_model is not None:
                    X_split_logistic = X_split.copy().fillna(0)
                    y_prob_logistic = logistic_model.predict_proba(X_split_logistic)[:, 1]
                    y_pred_logistic = (y_prob_logistic >= threshold).astype(int)
                    metrics_logistic[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_logistic,
                        y_prob=y_prob_logistic,
                        threshold=threshold,
                        model_name='logistic',
                        split_name=split_name,
                        output_dir=output_dir
                    )
                    if split_name == 'test':
                        # Print and save coefficients only for test split
                        try:
                            coef = logistic_model.named_steps['logreg'].coef_[0]
                            intercept = logistic_model.named_steps['logreg'].intercept_[0]
                            coef_pairs = sorted(zip(features, coef), key=lambda x: abs(x[1]), reverse=True)
                            self.logger.info("Logistic Regression coefficients (sorted by absolute value):")
                            for fname, val in coef_pairs:
                                self.logger.info(f"  {fname}: {val:.6f}")
                            self.logger.info(f"Intercept: {intercept:.6f}")
                            print("Logistic Regression coefficients:")
                            for fname, val in coef_pairs:
                                print(f"  {fname}: {val:.6f}")
                            print(f"Intercept: {intercept:.6f}")
                            coef_df = pd.DataFrame({
                                'feature': [fname for fname, _ in coef_pairs] + ['intercept'],
                                'coefficient': [val for _, val in coef_pairs] + [intercept]
                            })
                            coef_df.to_csv('output/logistic_coefficients.csv', index=False)
                        except Exception as e:
                            self.logger.warning(f"Could not print logistic coefficients: {e}")
                # XGBoost
                if xgb_model is not None:
                    X_split_xgb = X_split.copy()
                    y_prob_xgb = xgb_model.predict_proba(X_split_xgb)[:, 1]
                    y_pred_xgb = (y_prob_xgb >= threshold).astype(int)
                    metrics_xgb[split_name] = compute_metrics(
                        y_true=y_true,
                        y_pred=y_pred_xgb,
                        y_prob=y_prob_xgb,
                        threshold=threshold,
                        model_name='xgboost',
                        split_name=split_name,
                        output_dir=output_dir
                    )
            # Write metrics for all splits using updated writer
            write_metrics(metrics_logistic, metrics_xgb, self.config)

        # SHAP for logistic regression removed as requested
            # ...existing code...

            # 7. Output Writing
            self.logger.info("Writing outputs.")
            try:
                write_confusion_matrix(metrics_logistic, metrics_xgb, self.config)
                write_consolidated_individuals(
                    data_splits['test'],
                    logistic_preds,
                    xgb_preds,
                    shap_logistic,
                    shap_xgb,
                    self.config
                )
            except Exception as e:
                self.logger.warning(f"Output writing failed: {e}")


        # 5. SHAP Explainability
        self.logger.info("Generating SHAP explanations.")
        shap_logistic = None
        shap_xgb = None
        try:
            if logistic_model and logistic_preds is not None:
                explainer_logistic = get_shap_explainer(logistic_model, data_splits['test'][features], 'logistic', 'output/shap_logistic.pkl', self.logger, self.config)
                shap_logistic = extract_local_factors(explainer_logistic, data_splits['test'][features], self.config, set())
                # Save SHAP local factors to CSV
                shap_logistic.to_csv('output/shap_logistic.csv', index=False)
            if xgb_model and xgb_preds is not None:
                explainer_xgb = get_shap_explainer(xgb_model, data_splits['test'][features], 'xgboost', 'output/shap_xgb.pkl', self.logger, self.config)
                shap_xgb = extract_local_factors(explainer_xgb, data_splits['test'][features], self.config, set())
                # Save SHAP local factors to CSV
                shap_xgb.to_csv('output/shap_xgb.csv', index=False)
        except Exception as e:
            self.logger.warning(f"SHAP explainability failed: {e}")

        # 6. Output Writing
        self.logger.info("Writing outputs.")
        try:
            write_confusion_matrix(metrics_logistic, metrics_xgb, self.config)
            write_consolidated_individuals(
                data_splits['test'],
                logistic_preds,
                xgb_preds,
                shap_logistic,
                shap_xgb,
                self.config
            )
        except Exception as e:
            self.logger.warning(f"Output writing failed: {e}")

        # 3. Prediction
        self.logger.info("Generating predictions.")
        # logistic_preds = predict_logistic(logistic_model, data_splits['test'], features)
        # xgb_preds = predict_xgboost(xgb_model, data_splits['test'], features)

        # 4. Metrics Calculation
        self.logger.info("Calculating metrics.")
        # metrics_logistic = compute_metrics(data_splits['test'], logistic_preds)
        # metrics_xgb = compute_metrics(data_splits['test'], xgb_preds)

        # 5. SHAP Explainability
        self.logger.info("Generating SHAP explanations.")
    # explainer_logistic = get_shap_explainer(logistic_model, data_splits['test'][features], 'logistic', 'output/shap_logistic.pkl', self.logger, self.config)
    # shap_logistic = extract_local_factors(explainer_logistic, data_splits['test'][features], self.config, set())
    # explainer_xgb = get_shap_explainer(xgb_model, data_splits['test'][features], 'xgboost', 'output/shap_xgb.pkl', self.logger, self.config)
    # shap_xgb = extract_local_factors(explainer_xgb, data_splits['test'][features], self.config, set())

        # 6. Output Writing
        self.logger.info("Writing outputs.")
        # write_metrics(metrics_logistic, metrics_xgb, self.config)
        # write_confusion_matrix(metrics_logistic, metrics_xgb, self.config)
        # write_consolidated_individuals(data_splits['test'], logistic_preds, xgb_preds, shap_logistic, shap_xgb, self.config)

        self.logger.info("Pipeline orchestration complete.")
