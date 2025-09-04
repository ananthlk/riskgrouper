"""
Risk Threshold Derivation and Consensus Classification Module
- Bins risk scores by month
- Plots predicted/actual events
- Computes F1 scores for thresholds
- Derives high/low risk thresholds
- Classifies individuals by consensus between Logistic Regression and XGBoost
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import f1_score, precision_recall_curve

class RiskConsensusAnalyzer:
    def classify_and_export(self, bins=10):
        """
        Classify each individual as high/low/medium risk using a single threshold of 0.4 for both models and export required columns for last 3 months.
        Uses 'fh_id' for both FH and ID columns.
        """
        threshold = 0.4
        # Filter for last 3 months
        months = pd.to_datetime(self.df[self.month_col]).sort_values().unique()
        last_3_months = months[-3:]
        df_last3 = self.df[pd.to_datetime(self.df[self.month_col]).isin(last_3_months)].copy()
        # Classify
        lr = df_last3[self.lr_score_col]
        xgb = df_last3[self.xgb_score_col]
        consensus = []
        for i in range(len(df_last3)):
            if lr.iloc[i] < threshold and xgb.iloc[i] < threshold:
                consensus.append('LOW')
            elif lr.iloc[i] >= threshold and xgb.iloc[i] >= threshold:
                consensus.append('HIGH')
            else:
                consensus.append('MEDIUM')
        df_last3['RISK_CLASSIFICATION'] = consensus
        # Select and sort columns
        out_cols = ['fh_id', self.month_col, 'RISK_CLASSIFICATION', self.lr_score_col, self.xgb_score_col, self.event_col]
        df_out = df_last3[out_cols].sort_values(self.month_col, ascending=False)
        return df_out
    def print_bins_table(self, bins=10):
        """
        Prints the bins table for both models for the last 3 months.
        """
        tables = self.get_bins_table(bins=bins)
        for model, table in tables.items():
            print(f"\nRisk bins table for last 3 months: {model}")
            print(table)
    def __init__(self, df, lr_score_col, xgb_score_col, event_col, month_col):
        """
        df: DataFrame with individual predictions
        lr_score_col: column name for logistic regression risk score
        xgb_score_col: column name for xgboost risk score
        event_col: column name for actual event (binary)
        month_col: column name for month
        """
        self.df = df.copy()
        self.lr_score_col = lr_score_col
        self.xgb_score_col = xgb_score_col
        self.event_col = event_col
        self.month_col = month_col
        self.thresholds = {}

    def get_bins_table(self, bins=10):
        """Return cumulative bin table for risk scores, aggregated across months."""
        tables = {}
        for model in [self.lr_score_col, self.xgb_score_col]:
            self.df['bin'] = pd.cut(self.df[model], bins=bins)
            agg = self.df.groupby('bin').agg(
                predicted_events=(model, lambda x: np.sum(x > 0.5)),
                actual_events=(self.event_col, 'sum'),
                count=(model, 'count'),
                mean_score=(model, 'mean')
            ).reset_index()
            tables[model] = agg
            self.df.drop('bin', axis=1, inplace=True)
        return tables

    def get_bins_table(self, bins=10):
        """Return bin table for risk scores, aggregated for the last 3 months only, with consistent bin edges."""
        tables = {}
        # Find last 3 months
        months = pd.to_datetime(self.df[self.month_col]).sort_values().unique()
        last_3_months = months[-3:]
        df_last3 = self.df[pd.to_datetime(self.df[self.month_col]).isin(last_3_months)].copy()
        # Compute common bin edges from min/max across both models
        min_score = min(df_last3[self.lr_score_col].min(), df_last3[self.xgb_score_col].min())
        max_score = max(df_last3[self.lr_score_col].max(), df_last3[self.xgb_score_col].max())
        bin_edges = np.linspace(min_score, max_score, bins + 1)
        for model in [self.lr_score_col, self.xgb_score_col]:
            df_last3['bin'] = pd.cut(df_last3[model], bins=bin_edges, include_lowest=True)
            agg = df_last3.groupby('bin').agg(
                predicted_events=(model, 'sum'),
                actual_events=(self.event_col, 'sum'),
                count=(model, 'count'),
                mean_score=(model, 'mean')
            ).reset_index()
            # Add calibration percent
            agg['calibration_percent'] = np.where(agg['predicted_events'] > 0, agg['actual_events'] / agg['predicted_events'], np.nan)
            tables[model] = agg
            df_last3.drop('bin', axis=1, inplace=True)
        return tables
        high_thresh = thresholds[best_idx]
        low_thresh = thresholds[np.argmin(np.abs(recalls - 0.1))]  # Example: low recall ~0.1
        self.thresholds[model_col] = {'high': high_thresh, 'low': low_thresh}
        return self.thresholds[model_col]

    def classify_consensus(self):
        """Classify individuals by consensus between models."""
        lr = self.df[self.lr_score_col]
        xgb = self.df[self.xgb_score_col]
        lr_high = lr >= self.thresholds[self.lr_score_col]['high']
        lr_low = lr <= self.thresholds[self.lr_score_col]['low']
        xgb_high = xgb >= self.thresholds[self.xgb_score_col]['high']
        xgb_low = xgb <= self.thresholds[self.xgb_score_col]['low']
        consensus = []
        for i in range(len(self.df)):
            if lr_high.iloc[i] and xgb_high.iloc[i]:
                consensus.append('high_risk')
            elif lr_low.iloc[i] and xgb_low.iloc[i]:
                consensus.append('low_risk')
            else:
                consensus.append('medium_risk')
        self.df['consensus_risk'] = consensus
        return self.df

    def run_full_analysis(self, bins=10):
        self.print_bins_table(bins)
        # Set default thresholds for consensus classification
        self.thresholds[self.lr_score_col] = {'low': 0.3, 'high': 0.65}
        self.thresholds[self.xgb_score_col] = {'low': 0.3, 'high': 0.65}
        return self.classify_consensus()

# Example usage:
# df = pd.read_csv('output/individual_predictions.csv')
# analyzer = RiskConsensusAnalyzer(df, 'logistic_score', 'xgb_score', 'actual_event', 'month')
# result_df = analyzer.run_full_analysis()
# result_df.to_csv('output/individual_predictions_with_consensus.csv', index=False)
