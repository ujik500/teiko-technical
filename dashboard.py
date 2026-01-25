import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from teiko_technical import load_csv_to_sqlite

# Page configuration
st.set_page_config(page_title="Cell Count Analysis Dashboard", layout="wide")
st.title("Cell Count Analysis Dashboard")

# Initialize database
db_name = "teiko_technical.db"
table_name = "sample_data"
overview_table_name = "overview"

# Load data into SQLite (if not already loaded)
if not st.session_state.get('data_loaded'):
    load_csv_to_sqlite("cell-count.csv", db_name, table_name)
    st.session_state.data_loaded = True

# ============================================================================
# SECTION 1: DATA FILTERING (Like filter() function)
# ============================================================================

st.sidebar.header("🔍 Data Filters")

conn = sqlite3.connect(db_name)

# Get unique values for filter dropdowns
projects = sorted(pd.read_sql_query(f"SELECT DISTINCT project FROM {table_name}", conn)['project'].tolist())
conditions = sorted(pd.read_sql_query(f"SELECT DISTINCT condition FROM {table_name}", conn)['condition'].tolist())
treatments = sorted(pd.read_sql_query(f"SELECT DISTINCT treatment FROM {table_name}", conn)['treatment'].tolist())
sex_options = sorted(pd.read_sql_query(f"SELECT DISTINCT sex FROM {table_name}", conn)['sex'].tolist())
responses = sorted([x for x in pd.read_sql_query(f"SELECT DISTINCT response FROM {table_name}", conn)['response'].tolist() if x])

# Create filter widgets
selected_projects = st.sidebar.multiselect("Project", projects, default=projects, key="project_filter")
selected_conditions = st.sidebar.multiselect("Condition", conditions, default=conditions, key="condition_filter")
selected_treatments = st.sidebar.multiselect("Treatment", treatments, default=treatments, key="treatment_filter")
selected_sex = st.sidebar.multiselect("Sex", sex_options, default=sex_options, key="sex_filter")
selected_responses = st.sidebar.multiselect("Response", responses, default=responses, key="response_filter")

# Build dynamic filter query
query = f"SELECT * FROM {table_name} WHERE 1=1"
params = []

if selected_projects:
    placeholders = ','.join(['?' for _ in selected_projects])
    query += f" AND project IN ({placeholders})"
    params.extend(selected_projects)

if selected_conditions:
    placeholders = ','.join(['?' for _ in selected_conditions])
    query += f" AND condition IN ({placeholders})"
    params.extend(selected_conditions)

if selected_treatments:
    placeholders = ','.join(['?' for _ in selected_treatments])
    query += f" AND treatment IN ({placeholders})"
    params.extend(selected_treatments)

if selected_sex:
    placeholders = ','.join(['?' for _ in selected_sex])
    query += f" AND sex IN ({placeholders})"
    params.extend(selected_sex)

if selected_responses:
    placeholders = ','.join(['?' for _ in selected_responses])
    query += f" AND response IN ({placeholders})"
    params.extend(selected_responses)

# Execute filtered query
filtered_data = pd.read_sql_query(query, conn, params=params)

conn.close()

# ============================================================================
# SECTION 2: OVERVIEW METRICS
# ============================================================================

st.header("📊 Data Overview")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Samples", len(filtered_data))
col2.metric("Unique Subjects", filtered_data['subject'].nunique())
col3.metric("Unique Conditions", filtered_data['condition'].nunique())
col4.metric("Unique Projects", filtered_data['project'].nunique())

# ============================================================================
# SECTION 3: FILTERED DATA TABLE
# ============================================================================

st.header("📋 Filtered Data")
st.dataframe(filtered_data, use_container_width=True, height=300)

# ============================================================================
# SECTION 4: SUMMARY VISUALIZATIONS
# ============================================================================

st.header("📈 Summary Statistics")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Samples by Condition")
    condition_counts = filtered_data['condition'].value_counts()
    st.bar_chart(condition_counts)

with col2:
    st.subheader("Samples by Sex")
    sex_counts = filtered_data['sex'].value_counts()
    sex_labels = {'M': 'Male', 'F': 'Female'}
    sex_counts.index = sex_counts.index.map(sex_labels)
    st.bar_chart(sex_counts)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Samples by Treatment")
    treatment_counts = filtered_data['treatment'].value_counts()
    st.bar_chart(treatment_counts)

with col2:
    st.subheader("Samples by Response")
    response_counts = filtered_data['response'].value_counts()
    response_labels = {'yes': 'Responders', 'no': 'Non-responders'}
    response_counts.index = response_counts.index.map(response_labels)
    st.bar_chart(response_counts)

# ============================================================================
# SECTION 5: CELL FREQUENCY ANALYSIS & BOXPLOTS
# ============================================================================

st.header("🔬 Cell Frequency Analysis & Statistical Testing")

# Let user select parameters for analysis
col1, col2, col3 = st.columns(3)

with col1:
    analysis_condition = st.selectbox("Select Condition for Analysis", conditions)

with col2:
    analysis_treatment = st.selectbox("Select Treatment for Analysis", treatments)

with col3:
    sample_type = st.selectbox("Select Sample Type", ["PBMC", "All"])

# Query data for analysis
conn = sqlite3.connect(db_name)

analysis_query = f"""
SELECT ov.population, ov.percentage, sd.response, sd.sample
FROM {overview_table_name} ov
JOIN {table_name} sd ON ov.sample = sd.sample
WHERE sd.condition = ? 
  AND sd.treatment = ?
  AND sd.sample_type = ?
"""

sample_type_filter = "PBMC" if sample_type == "PBMC" else "%"
analysis_data = pd.read_sql_query(
    analysis_query,
    conn,
    params=[analysis_condition, analysis_treatment, sample_type_filter if sample_type != "All" else "%"]
)

if len(analysis_data) > 0:
    # Organize data by population and response
    cell_types = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
    data_by_population = {ct: {'yes': [], 'no': []} for ct in cell_types}
    
    for _, row in analysis_data.iterrows():
        population = row['population']
        percentage = row['percentage']
        response = row['response']
        if population in data_by_population and response in ['yes', 'no']:
            data_by_population[population][response].append(percentage)
    
    # Count samples
    unique_samples_yes = len(analysis_data[analysis_data['response'] == 'yes']['sample'].unique())
    unique_samples_no = len(analysis_data[analysis_data['response'] == 'no']['sample'].unique())
    
    # Display sample counts
    metric_col1, metric_col2 = st.columns(2)
    metric_col1.metric("Responders (Yes)", unique_samples_yes)
    metric_col2.metric("Non-responders (No)", unique_samples_no)
    
    # Create boxplots
    st.subheader("Cell Population Distribution by Response Status")
    
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()
    
    for idx, cell_type in enumerate(cell_types):
        yes_data = data_by_population[cell_type]['yes']
        no_data = data_by_population[cell_type]['no']
        
        # Create boxplot
        axes[idx].boxplot([yes_data, no_data], labels=['Yes', 'No'])
        axes[idx].set_title(f'{cell_type.replace("_", " ").title()}', fontsize=12, fontweight='bold')
        axes[idx].set_xlabel('Treatment Response')
        axes[idx].set_ylabel('Relative Frequency (%)')
        axes[idx].grid(axis='y', alpha=0.3)
    
    # Remove extra subplot
    fig.delaxes(axes[5])
    
    plt.tight_layout()
    st.pyplot(fig)
    
    # ========================================================================
    # SECTION 6: STATISTICAL TESTING (Welch's t-test with FDR Correction)
    # ========================================================================
    
    st.subheader("📉 Statistical Analysis (Welch's t-test with Benjamini-Hochberg FDR)")
    
    print_output = "Cell Type Relative Frequency Statistics (Welch's t-test):\n"
    p_values = []
    
    for cell_type in cell_types:
        yes_data = data_by_population[cell_type]['yes']
        no_data = data_by_population[cell_type]['no']
        
        yes_mean = np.mean(yes_data) if yes_data else 0
        no_mean = np.mean(no_data) if no_data else 0
        
        # Perform Welch's t-test
        if len(yes_data) > 0 and len(no_data) > 0:
            statistic, p_value = stats.ttest_ind(yes_data, no_data, equal_var=False)
            p_values.append((cell_type, p_value, yes_mean, no_mean))
    
    # Benjamini-Hochberg FDR correction
    if p_values:
        sorted_p_values = sorted(p_values, key=lambda x: x[1])
        
        # Create results dataframe
        results_df = []
        for rank, (cell_type, p_value, yes_mean, no_mean) in enumerate(sorted_p_values):
            threshold = (rank + 1) / len(cell_types) * 0.05
            is_significant = p_value < threshold
            
            results_df.append({
                'Cell Type': cell_type.replace('_', ' ').title(),
                'Responders Mean %': f"{yes_mean:.2f}",
                'Non-responders Mean %': f"{no_mean:.2f}",
                'p-value': f"{p_value:.4f}",
                'BH Threshold': f"{threshold:.4f}",
                'Significant': "✓ Yes" if is_significant else "No"
            })
        
        results_table = pd.DataFrame(results_df)
        st.dataframe(results_table, use_container_width=True)
        
        # Summary interpretation
        st.subheader("📝 Interpretation")
        significant_cells = [r['Cell Type'] for r in results_df if r['Significant'] == "✓ Yes"]
        
        if significant_cells:
            st.success(f"**Significant findings:** {', '.join(significant_cells)} show statistically significant differences between responders and non-responders (p < 0.05 after FDR correction).")
        else:
            st.info("No cell types show statistically significant differences between responders and non-responders after FDR correction.")
    
else:
    st.warning(f"No data available for condition='{analysis_condition}' and treatment='{analysis_treatment}'")

conn.close()

# ============================================================================
# SECTION 7: EXPORT DATA
# ============================================================================

st.header("💾 Export Options")

col1, col2 = st.columns(2)

with col1:
    csv = filtered_data.to_csv(index=False)
    st.download_button(
        label="Download Filtered Data as CSV",
        data=csv,
        file_name="filtered_sample_data.csv",
        mime="text/csv"
    )

with col2:
    st.info("Use the filters on the left sidebar to customize your analysis")
