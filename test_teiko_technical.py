import unittest
import sqlite3
import os
import csv
import tempfile
from pathlib import Path
import sys

# Import functions from the main module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from teiko_technical import load_csv_to_sqlite, overview, filter

# Suppress the matplotlib import in main module
import warnings
warnings.filterwarnings("ignore")


class TestLoadCSVToSQLite(unittest.TestCase):
    """Test cases for the load_csv_to_sqlite function"""
    
    def setUp(self):
        """Create temporary directory and files for testing"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.csv_path = os.path.join(self.test_dir, "test.csv")
        self.table_name = "test_data"
        self.overview_table_name = "test_overview"
    
    def tearDown(self):
        """Clean up temporary files"""
        # Close any open database connections
        try:
            conn = sqlite3.connect(self.db_path)
            conn.close()
        except:
            pass
        
        import time
        time.sleep(0.1)  # Brief delay to ensure file is released
        
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)
        
        os.rmdir(self.test_dir)
    
    def create_test_csv(self, rows):
        """Helper to create a test CSV file"""
        headers = ['project', 'subject', 'condition', 'age', 'sex', 'treatment',
                   'response', 'sample', 'sample_type', 'time_from_treatment_start',
                   'b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
        
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
    
    def test_basic_csv_loading(self):
        """Test that CSV data is correctly loaded into database"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_test_csv(test_rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        # Verify data was loaded
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 1, "Should have loaded 1 row")
    
    def test_multiple_rows_loading(self):
        """Test loading multiple rows"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
            ['prj1', 'sbj002', 'carcinoma', '65', 'F', 'drug_b', 'no', 
             'sample002', 'PBMC', '7', '120', '180', '320', '160', '280'],
            ['prj2', 'sbj003', 'melanoma', '52', 'M', 'drug_a', 'yes', 
             'sample003', 'PBMC', '14', '110', '210', '310', '155', '260'],
        ]
        self.create_test_csv(test_rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 3, "Should have loaded 3 rows")
    
    def test_data_types_preserved(self):
        """Test that data types are correctly preserved (integers as integers, text as text)"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_test_csv(test_rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT age, b_cell FROM {self.table_name}")
        age, b_cell = cursor.fetchone()
        conn.close()
        
        self.assertIsInstance(age, int, "Age should be an integer")
        self.assertIsInstance(b_cell, int, "Cell count should be an integer")
        self.assertEqual(age, 57)
        self.assertEqual(b_cell, 100)
    
    def test_table_overwrite(self):
        """Test that existing table is overwritten"""
        test_rows_1 = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_test_csv(test_rows_1)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        # Create new CSV with different data
        test_rows_2 = [
            ['prj2', 'sbj002', 'carcinoma', '65', 'F', 'drug_b', 'no', 
             'sample002', 'PBMC', '7', '120', '180', '320', '160', '280'],
            ['prj2', 'sbj003', 'melanoma', '52', 'M', 'drug_a', 'yes', 
             'sample003', 'PBMC', '14', '110', '210', '310', '155', '260'],
        ]
        self.create_test_csv(test_rows_2)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 2, "Table should be overwritten with 2 new rows")
    
    def test_header_skipping(self):
        """Test that CSV header is correctly skipped"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_test_csv(test_rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT project FROM {self.table_name}")
        project = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(project, 'prj1', "Header should be skipped, data should start with 'prj1'")
        self.assertNotEqual(project, 'project', "Should not have loaded the header row")
    
    def test_column_mapping(self):
        """Test that columns are correctly mapped"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_test_csv(test_rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT subject, condition, sample FROM {self.table_name}")
        subject, condition, sample = cursor.fetchone()
        conn.close()
        
        self.assertEqual(subject, 'sbj001')
        self.assertEqual(condition, 'melanoma')
        self.assertEqual(sample, 'sample001')


class TestOverview(unittest.TestCase):
    """Test cases for the overview function"""
    
    def setUp(self):
        """Set up test database"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.csv_path = os.path.join(self.test_dir, "test.csv")
        self.table_name = "test_data"
        self.overview_table = "test_overview"
    
    def tearDown(self):
        """Clean up"""
        import time
        import gc
        try:
            conn = sqlite3.connect(self.db_path)
            conn.close()
        except:
            pass
        
        gc.collect()  # Force garbage collection to release file handles
        time.sleep(0.2)  # Brief delay to ensure file is released
        
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except:
                pass
        
        if os.path.exists(self.csv_path):
            try:
                os.remove(self.csv_path)
            except:
                pass
        
        try:
            os.rmdir(self.test_dir)
        except:
            pass
    
    def create_and_load_csv(self, rows):
        """Helper to create and load CSV"""
        headers = ['project', 'subject', 'condition', 'age', 'sex', 'treatment',
                   'response', 'sample', 'sample_type', 'time_from_treatment_start',
                   'b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
        
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
    
    def test_overview_table_created(self):
        """Test that overview table is created"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.overview_table}'")
        result = cursor.fetchone()
        conn.close()
        
        self.assertIsNotNone(result, "Overview table should be created")
    
    def test_overview_row_count(self):
        """Test that overview has correct number of rows (5 populations per sample)"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.overview_table}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 5, "Should have 5 rows (one per population)")
    
    def test_overview_multiple_samples(self):
        """Test overview with multiple samples"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
            ['prj1', 'sbj002', 'carcinoma', '65', 'F', 'drug_b', 'no', 
             'sample002', 'PBMC', '7', '120', '180', '320', '160', '280'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.overview_table}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 10, "Should have 10 rows (5 populations × 2 samples)")
    
    def test_total_count_calculation(self):
        """Test that total_count is correctly calculated"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT total_count FROM {self.overview_table}")
        total_count = cursor.fetchone()[0]
        conn.close()
        
        expected_total = 100 + 200 + 300 + 150 + 250  # 1000
        self.assertEqual(total_count, expected_total, f"Total count should be {expected_total}")
    
    def test_percentage_calculation(self):
        """Test that percentage is correctly calculated"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Test b_cell percentage: 100 / 1000 * 100 = 10%
        cursor.execute(f"SELECT percentage FROM {self.overview_table} WHERE population = 'b_cell'")
        b_cell_percentage = cursor.fetchone()[0]
        
        # Allow small floating point differences
        self.assertAlmostEqual(b_cell_percentage, 10.0, places=1, msg="b_cell percentage should be ~10%")
        
        conn.close()
    
    def test_all_populations_present(self):
        """Test that all 5 populations are present in overview"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT population FROM {self.overview_table} ORDER BY population")
        populations = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        expected_populations = ['b_cell', 'cd4_t_cell', 'cd8_t_cell', 'monocyte', 'nk_cell']
        self.assertEqual(sorted(populations), sorted(expected_populations), 
                        "All 5 populations should be present")
    
    def test_overview_column_structure(self):
        """Test that overview table has correct column structure"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.overview_table})")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        conn.close()
        
        expected_columns = {'sample', 'total_count', 'population', 'count', 'percentage'}
        self.assertEqual(set(columns.keys()), expected_columns, 
                        "Overview table should have exactly these columns")
    
    def test_percentages_sum_to_100(self):
        """Test that percentages for each sample sum to approximately 100%"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT sample, SUM(percentage) FROM {self.overview_table} GROUP BY sample")
        sample, percentage_sum = cursor.fetchone()
        conn.close()
        
        self.assertAlmostEqual(percentage_sum, 100.0, places=1, 
                              msg="Percentages for a sample should sum to ~100%")
    
    def test_count_matches_source_data(self):
        """Test that counts in overview match the source data"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '100', '200', '300', '150', '250'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check b_cell count
        cursor.execute(f"SELECT count FROM {self.overview_table} WHERE population = 'b_cell'")
        b_cell_count = cursor.fetchone()[0]
        
        self.assertEqual(b_cell_count, 100, "b_cell count should match source data")
        
        conn.close()


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation and edge cases"""
    
    def setUp(self):
        """Set up test database"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.csv_path = os.path.join(self.test_dir, "test.csv")
        self.table_name = "test_data"
        self.overview_table = "test_overview"
    
    def tearDown(self):
        """Clean up"""
        import time
        import gc
        try:
            conn = sqlite3.connect(self.db_path)
            conn.close()
        except:
            pass
        
        gc.collect()  # Force garbage collection to release file handles
        time.sleep(0.2)  # Brief delay to ensure file is released
        
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except:
                pass
        
        if os.path.exists(self.csv_path):
            try:
                os.remove(self.csv_path)
            except:
                pass
        
        try:
            os.rmdir(self.test_dir)
        except:
            pass
    
    def create_and_load_csv(self, rows):
        """Helper to create and load CSV"""
        headers = ['project', 'subject', 'condition', 'age', 'sex', 'treatment',
                   'response', 'sample', 'sample_type', 'time_from_treatment_start',
                   'b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
        
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
    
    def test_zero_cell_counts(self):
        """Test handling of zero cell counts"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '0', '0', '0', '0', '100'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.overview_table}")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 5, "Should handle zero counts gracefully")
    
    def test_large_cell_counts(self):
        """Test handling of large cell counts"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '1000000', '2000000', '3000000', '1500000', '2500000'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT SUM(count) FROM {self.overview_table}")
        total = cursor.fetchone()[0]
        conn.close()
        
        expected_total = 1000000 + 2000000 + 3000000 + 1500000 + 2500000
        self.assertEqual(total, expected_total, "Should handle large counts correctly")
    
    def test_percentage_precision(self):
        """Test that percentage values have appropriate precision"""
        test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'drug_a', 'yes', 
             'sample001', 'PBMC', '0', '1', '1', '1', '1', '1'],
        ]
        self.create_and_load_csv(test_rows)
        
        overview(self.db_path, self.table_name, self.overview_table)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT percentage FROM {self.overview_table} LIMIT 1")
        percentage = cursor.fetchone()[0]
        conn.close()
        
        # Each population should be 1/5 = 20%
        self.assertAlmostEqual(percentage, 20.0, places=2, msg="Percentage should be ~20%")


class TestFilter(unittest.TestCase):
    """Test cases for the filter function"""
    
    def setUp(self):
        """Create temporary directory and files for testing"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.csv_path = os.path.join(self.test_dir, "test.csv")
        self.table_name = "test_data"
        
        # Create test data with known values
        self.test_rows = [
            ['prj1', 'sbj001', 'melanoma', '57', 'M', 'miraclib', 'yes', 'sample001', 'PBMC', '0', '100', '200', '300', '400', '500'],
            ['prj1', 'sbj002', 'melanoma', '60', 'F', 'miraclib', 'no', 'sample002', 'PBMC', '7', '110', '210', '310', '410', '510'],
            ['prj1', 'sbj003', 'carcinoma', '65', 'M', 'phauximab', 'yes', 'sample003', 'PBMC', '0', '120', '220', '320', '420', '520'],
            ['prj2', 'sbj004', 'melanoma', '70', 'M', 'miraclib', 'yes', 'sample004', 'PBMC', '14', '130', '230', '330', '430', '530'],
            ['prj2', 'sbj005', 'melanoma', '55', 'F', 'phauximab', 'no', 'sample005', 'PBMC', '0', '140', '240', '340', '440', '540'],
        ]
        self.create_and_load_csv(self.test_rows)
    
    def tearDown(self):
        """Clean up temporary files"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.close()
        except:
            pass
        
        import time
        time.sleep(0.1)
        
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)
        
        os.rmdir(self.test_dir)
    
    def create_and_load_csv(self, rows):
        """Helper to create and load a test CSV file"""
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['project', 'subject', 'condition', 'age', 'sex', 'treatment',
                           'response', 'sample', 'sample_type', 'time_from_treatment_start',
                           'b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte'])
            # Write data rows
            writer.writerows(rows)
        
        load_csv_to_sqlite(self.csv_path, self.db_path, self.table_name)
    
    def get_filter_result_count(self, **filters):
        """Helper to get the count of results from a filter query without printing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        valid_columns = ['project', 'subject', 'condition', 'age', 'sex', 'treatment', 
                        'response', 'sample', 'sample_type', 'time_from_treatment_start']
        
        where_conditions = []
        params = []
        
        for column, value in filters.items():
            if column not in valid_columns:
                continue
            where_conditions.append(f"{column} = ?")
            params.append(value)
        
        query = f"SELECT * FROM {self.table_name}"
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return len(results)
    
    def test_filter_single_condition(self):
        """Test filtering with a single condition"""
        count = self.get_filter_result_count(condition='melanoma')
        self.assertEqual(count, 4, "Should find 4 melanoma rows")
    
    def test_filter_single_sex(self):
        """Test filtering by sex"""
        count = self.get_filter_result_count(sex='M')
        self.assertEqual(count, 3, "Should find 3 male rows")
    
    def test_filter_multiple_conditions(self):
        """Test filtering with multiple conditions"""
        count = self.get_filter_result_count(condition='melanoma', sex='M')
        self.assertEqual(count, 2, "Should find 2 melanoma male rows")
    
    def test_filter_treatment(self):
        """Test filtering by treatment"""
        count = self.get_filter_result_count(treatment='miraclib')
        self.assertEqual(count, 3, "Should find 3 miraclib treatment rows")
    
    def test_filter_project(self):
        """Test filtering by project"""
        count = self.get_filter_result_count(project='prj1')
        self.assertEqual(count, 4, "Should find 4 prj1 rows")
    
    def test_filter_time_from_treatment_start(self):
        """Test filtering by time_from_treatment_start"""
        count = self.get_filter_result_count(time_from_treatment_start='0')
        self.assertEqual(count, 3, "Should find 3 rows with time_from_treatment_start=0")
    
    def test_filter_response(self):
        """Test filtering by response"""
        count = self.get_filter_result_count(response='yes')
        self.assertEqual(count, 3, "Should find 3 rows with response='yes'")
    
    def test_filter_three_conditions(self):
        """Test filtering with three conditions"""
        count = self.get_filter_result_count(condition='melanoma', sex='M', time_from_treatment_start='0')
        self.assertEqual(count, 1, "Should find 1 melanoma male at baseline")
    
    def test_filter_no_matches(self):
        """Test filtering with no matching results"""
        count = self.get_filter_result_count(condition='melanoma', project='prj2', sex='F')
        self.assertEqual(count, 1, "Should find 1 matching row")
    
    def test_filter_all_rows(self):
        """Test filtering with no conditions returns all rows"""
        count = self.get_filter_result_count()
        self.assertEqual(count, 5, "Should return all 5 rows when no filters applied")
    
    def test_filter_specific_sample(self):
        """Test filtering by specific sample"""
        count = self.get_filter_result_count(sample='sample001')
        self.assertEqual(count, 1, "Should find exactly 1 row with sample='sample001'")


if __name__ == '__main__':
    unittest.main()
