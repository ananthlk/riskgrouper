"""
LLM Analysis Results Interpreter

This script automates the consolidation of results from multiple machine learning
model runs to prepare them for interpretation by a Large Language Model (LLM).

The script performs the following steps:
1.  **Finds the Latest Output Directory**: It automatically identifies the most
    recent analysis output folder inside the `output/` directory. This ensures
    that it always processes the results from the latest run of `run_analysis.sh`.
2.  **Gathers Key Artifacts**: It walks through the identified directory and finds
    all the critical output files:
    - `*_summary.txt`: Contains the detailed performance metrics (AUC, accuracy,
      classification report, etc.) for each model.
    - `*_feature_importances.csv`: Lists the most important predictive features
      for each model.
3.  **Consolidates Information**: It reads the content of all these files and
    compiles them into a single, well-structured Markdown document. The document
    is organized by model, making it easy to read and parse.
4.  **Generates a Master Prompt**: It prepends a carefully crafted prompt to the
    consolidated text. This prompt instructs an LLM to act as an expert data
    scientist, guiding it to provide a high-level executive summary, compare
    model performance, and identify key business insights.
5.  **Saves the Final Report**: The final consolidated text and prompt are saved
    to a single file named `final_analysis_report.md` in the project's root
    directory.

To use this script, simply run it from the command line after a full analysis
run has completed:
    $ python scripts/analysis/interpret_results.py

You can then copy the contents of `final_analysis_report.md` and paste it into
the LLM of your choice for a comprehensive, automated interpretation of your
modeling results.
"""
import os
import glob
import pandas as pd

def find_latest_output_directory(base_dir='output'):
    """
    Finds the most recently created subdirectory in the base_dir.

    Args:
        base_dir (str): The directory to search within.

    Returns:
        str or None: The path to the latest directory, or None if no directories are found.
    """
    try:
        list_of_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
        if not list_of_dirs:
            return None
        latest_dir = max(list_of_dirs, key=os.path.getmtime)
        return latest_dir
    except FileNotFoundError:
        return None

def consolidate_results(directory):
    """
    Consolidates summary and feature importance files from a given directory.

    Args:
        directory (str): The path to the analysis output directory.

    Returns:
        str: A formatted string containing the consolidated results.
    """
    if not directory:
        return "No output directory found. Please run an analysis first."

    print(f"Consolidating results from: {directory}")
    
    summary_files = glob.glob(os.path.join(directory, '*_summary.txt'))
    feature_files = glob.glob(os.path.join(directory, '*_feature_importances.csv'))

    if not summary_files:
        return f"No summary files (*_summary.txt) found in {directory}."

    consolidated_text = ""
    
    # Sort files to ensure a consistent order
    summary_files.sort()
    feature_files.sort()

    for summary_file in summary_files:
        model_name = os.path.basename(summary_file).replace('_summary.txt', '')
        consolidated_text += f"## Model Run: {model_name}\n\n"
        
        # Add summary text
        consolidated_text += "### Performance Summary\n"
        with open(summary_file, 'r') as f:
            consolidated_text += f.read()
        consolidated_text += "\n\n"

        # Find and add corresponding feature importances
        matching_feature_file = os.path.join(directory, f"{model_name}_feature_importances.csv")
        if os.path.exists(matching_feature_file):
            consolidated_text += "### Top 15 Feature Importances\n"
            try:
                features_df = pd.read_csv(matching_feature_file)
                # Format as a markdown table
                consolidated_text += features_df.head(15).to_markdown(index=False)
                consolidated_text += "\n\n"
            except Exception as e:
                consolidated_text += f"Could not read or format feature importances: {e}\n\n"
        
        consolidated_text += "---\n\n"

    return consolidated_text

def generate_llm_prompt(consolidated_results):
    """
    Generates a master prompt for an LLM, including the consolidated results.

    Args:
        consolidated_results (str): The string of consolidated results.

    Returns:
        str: The full prompt ready to be sent to an LLM.
    """
    prompt = """
You are an expert data scientist tasked with interpreting the results of a series of machine learning models. Your audience is a mix of technical and business stakeholders.

Below is a consolidated summary of multiple model runs, each designed to predict a different healthcare-related outcome (e.g., `y_smi_60d` for Serious Mental Illness, `y_chf_60d` for Congestive Heart Failure).

Your task is to provide a high-level executive summary of the findings. Please structure your response as follows:

**1. Executive Summary:**
   - Start with a brief, high-level overview of the project's goal and the overall performance of the models.
   - Mention which models were the most and least successful in terms of predictive power (AUC score is the most important metric here).

**2. Key Insights & Trends:**
   - **Model Performance Comparison:** Compare the performance across the different target variables. Are there certain conditions (e.g., SMI, CHF) that are easier or harder to predict?
   - **Feature Importance Analysis:** Identify the top 5-10 features that appear most consistently across all models. What do these common features tell us about the underlying drivers of risk?
   - **Business Implications:** What are the key takeaways for the business? For example, if `months_since_batched` is a top feature, it might suggest that the member's tenure in the program is a critical factor. If specific HCC codes are important, it could inform clinical outreach strategies.

**3. Recommendations:**
   - Based on the results, suggest 1-2 concrete next steps. This could be focusing on a specific high-performing model, exploring certain features further, or suggesting a specific business action.

Please be concise and focus on actionable insights.

---
**CONSOLIDATED MODEL RESULTS:**
---

"""
    return prompt + consolidated_results

def main():
    """
    Main function to run the consolidation and prompt generation process.
    """
    print("Starting LLM analysis results interpretation...")
    
    output_dir = find_latest_output_directory()
    
    if not output_dir:
        print("Error: No output directories found in the 'output' folder.")
        print("Please run the main analysis script first to generate results.")
        return

    consolidated_text = consolidate_results(output_dir)
    
    if "No summary files" in consolidated_text or "No output directory found" in consolidated_text:
        print(f"Error: {consolidated_text}")
        return

    final_prompt = generate_llm_prompt(consolidated_text)
    
    output_filename = 'final_analysis_report.md'
    with open(output_filename, 'w') as f:
        f.write(final_prompt)
        
    print(f"\nSuccessfully created the final analysis report: {output_filename}")
    print("You can now copy the contents of this file and paste it into your preferred LLM for interpretation.")

if __name__ == "__main__":
    main()
