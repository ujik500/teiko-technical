from itertools import count
import sqlite3
import csv
import matplotlib.pyplot as plt
from scipy import stats

#Dataset Example Rows
'''
project,subject,condition,age,sex,treatment,response,sample,sample_type,time_from_treatment_start,b_cell,cd8_t_cell,cd4_t_cell,nk_cell,monocyte
prj1,sbj000,melanoma,57,M,miraclib,no,sample00000,PBMC,0,10908,24440,20491,13864,23511
prj1,sbj000,melanoma,57,M,miraclib,no,sample00001,PBMC,7,6777,19407,33459,18170,23011
'''

'''
Load CSV data from input file into a table in an SQlite database
'''
def load_csv_to_sqlite(input_filename: str, db_name: str, table_name: str) -> None:
    file = open(input_filename, "r")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # overwrite data if it is already present
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # define database schema
    cursor.execute(f"""
        CREATE TABLE {table_name} (
        project TEXT,
        subject TEXT,
        condition TEXT,
        age INTEGER,
        sex TEXT, 
        treatment TEXT,
        response TEXT,
        sample TEXT,
        sample_type TEXT,
        time_from_treatment_start INTEGER,
        b_cell INTEGER,
        cd8_t_cell INTEGER,
        cd4_t_cell INTEGER,
        nk_cell INTEGER,
        monocyte INTEGER
        )"""
    )

    columns = ['project','subject','condition','age','sex','treatment',
               'response','sample', 'sample_type','time_from_treatment_start',
               'b_cell','cd8_t_cell','cd4_t_cell','nk_cell',
               'monocyte']
    
    col_names = ",".join(columns)
    placeholders = ",".join(["?"] * len(columns))

    reader = csv.reader(file)
    next(reader) # skip header

    cursor.executemany(
        f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        """
        , reader
    )

    conn.commit()
    conn.close()

    file.close()
    
'''
Given an SQlite db and table name, produce a new summary table of the samples 
in that dataset
'''
def overview(db_name: str, table_name: str, overview_table_name: str) -> None:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {overview_table_name}")
    cursor.execute(f"""
        CREATE TABLE {overview_table_name} AS
        WITH intermediate AS (
            SELECT 
                sample,
                b_cell + cd8_t_cell + cd4_t_cell + nk_cell + monocyte AS total_count,
                b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte
            FROM {table_name}
        )
        SELECT 
            sample, 
            total_count, 
            'b_cell' AS population, 
            b_cell AS count,
            ROUND(100.0 * b_cell / total_count, 4) AS percentage
        FROM intermediate
        UNION ALL
        SELECT sample, total_count, 'cd8_t_cell', cd8_t_cell, ROUND(100.0 * cd8_t_cell / total_count, 4) FROM intermediate
        UNION ALL
        SELECT sample, total_count, 'cd4_t_cell', cd4_t_cell, ROUND(100.0 * cd4_t_cell / total_count, 4) FROM intermediate
        UNION ALL
        SELECT sample, total_count, 'nk_cell', nk_cell, ROUND(100.0 * nk_cell / total_count, 4) FROM intermediate
        UNION ALL
        SELECT sample, total_count, 'monocyte', monocyte, ROUND(100.0 * monocyte / total_count, 4) FROM intermediate
    """)
    
    print(f"\n=== Overview Table: Cell Population Frequency Analysis ===")
    print(f"Columns: sample | total_count | population | count | percentage")
    print(f"Sample size: First 10 rows\n")
    
    for row in cursor.execute(f"SELECT * FROM {overview_table_name} LIMIT 10"):
        print(row)
    
    conn.commit()
    conn.close()

'''
Provides analysis on cell types that have significant frequency changes between
responders and non-responders, given a specific treatment and condition.
'''
def analyze_frequencies(db_name: str, sample_table_name: str, overview_table_name: str, treatment: str, condition: str) -> None:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Query relative frequencies (percentages) with response status
    query = f"""
    SELECT ov.population, ov.percentage, sd.response
    FROM {overview_table_name} ov
    JOIN {sample_table_name} sd ON ov.sample = sd.sample
    WHERE sd.sample_type = 'PBMC' 
      AND sd.condition = '{condition}' 
      AND sd.treatment = '{treatment}'
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\n=== Cell Relative Frequency Analysis ===")
    print(f"Condition: {condition}, Treatment: {treatment}")
    print(f"Sample Type: PBMC")
    print(f"Total observations in analysis: {len(results)}")
    
    # Organize data by population and response
    cell_types = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
    data_by_population = {ct: {'yes': [], 'no': []} for ct in cell_types}
    
    for population, percentage, response in results:
        if population in data_by_population:
            data_by_population[population][response].append(percentage)
    
    # Count samples
    yes_count = sum(len(data_by_population[ct]['yes']) for ct in cell_types) // len(cell_types)
    no_count = sum(len(data_by_population[ct]['no']) for ct in cell_types) // len(cell_types)
    
    print(f"Responders (Response=Yes): {yes_count}")
    print(f"Non-responders (Response=No): {no_count}\n")
    
    # Create 5 subplots
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.flatten()
    
    for idx, cell_type in enumerate(cell_types):
        yes_data = data_by_population[cell_type]['yes']
        no_data = data_by_population[cell_type]['no']
        
        # Create boxplot
        axes[idx].boxplot([yes_data, no_data], labels=['Yes', 'No'])
        axes[idx].set_title(f'{cell_type.replace("_", " ").title()}')
        axes[idx].set_xlabel('Treatment Response')
        axes[idx].set_ylabel('Relative Frequency (%)')
        axes[idx].grid(axis='y', alpha=0.3)
    
    # Remove extra subplot
    fig.delaxes(axes[5])
    
    plt.tight_layout()
    plt.savefig('cell_response_boxplots.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Print detailed statistics
    print("Cell Type Relative Frequency Statistics:")
    p_values = []
    
    for cell_type in cell_types:
        yes_data = data_by_population[cell_type]['yes']
        no_data = data_by_population[cell_type]['no']
        
        yes_mean = sum(yes_data) / len(yes_data) if yes_data else 0
        no_mean = sum(no_data) / len(no_data) if no_data else 0
        
        print(f"\n{cell_type}:")
        print(f"  Responders (Yes)     - Mean: {yes_mean:.2f}%")
        print(f"  Non-responders (No)  - Mean: {no_mean:.2f}%")
        
        # Perform Mann-Whitney U test on relative frequencies
        statistic, p_value = stats.mannwhitneyu(yes_data, no_data, alternative='two-sided')
        p_values.append((cell_type, p_value))
    
    # Benjamini-Hochberg FDR correction
    print("\n" + "="*70)
    print("Statistical Significance Testing (Mann-Whitney U with BH FDR)")
    print("="*70)
    
    sorted_indices = sorted(range(len(p_values)), key=lambda i: p_values[i][1])
    
    print(f"{'Cell Type':<18} {'p-value':<12} {'BH Threshold':<15} {'Significant':<12}")
    print("-"*70)
    
    for rank, idx in enumerate(sorted_indices):
        cell_type, p_value = p_values[idx]
        threshold = (rank + 1) / len(cell_types) * 0.05
        is_significant = p_value < threshold
        marker = "Yes ***" if is_significant else "No"
        
        print(f"{cell_type:<18} {p_value:<12.4f} {threshold:<15.4f} {marker:<12}")
    
    conn.close()


input_filename = "cell-count.csv"
db_name = "teiko_technical.db"
table_name = "sample_data"
overview_table_name = "overview"
load_csv_to_sqlite(input_filename, db_name, table_name)
overview(db_name, table_name, overview_table_name)
analyze_frequencies(db_name, table_name, overview_table_name, "miraclib", "melanoma")
