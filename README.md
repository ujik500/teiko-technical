# Run Instructions
## Part I: Running the Main Code
To run the main code and see all outputs required in parts 1-4, simply run the
file **teiko_technical.py**. Results for each part will be printed to the terminal,
and the boxplot chart will be displayed on screen. The boxplot image will also be saved as **cell_response_boxplots.png**. SQLite data will be stored in 
a file called **teiko_technical.db**.

## Part II: Running Unit Tests
### Option #1: Run all unit tests together
python -m unittest test_teiko_technical.py -v
### Option #2: Run just one test class
python -m unittest test_teiko_technical.TestOverview -v

# Database Schema
## Overall Strategy
The code uses a local SQLite database approach for storing and querying data for 
the original sample data and the overview table. Why this approach?
* SQLite allows for datasets with larger memory requirements, since it is able to utilize disk resources and only load smaller segments into RAM at a time, whereas vanilla Python data structures or pandas dataframes must completely fit in main memory.
* SQLite's database queries can be converted to server-based options like mySQL much easier than pandas dataframes, which have quite different logic and syntax. 
* SQLite is an appropriate local option for a task like this, since my technical code doesn't require many concurrent viewers/editors, a need that a server-based approach like mySQL would be a much better fit for in practice.

## Table Schema
My database is separated into two tables:
1. *sample_data*:
Holds all data fields from the original CSV file. This allows flexibility for almost any kind of data analysis that could be performed. 
2. *overview*:
Holds only the summary table that unpivots the five cell type count columns, and displays 

If the team knew certain queries and comparisions were more common than others, this schema would be altered (drop certain columns, precalculate desired values) to speed future analysis. However, in this case, it is unclear what the priorities would be, so the generic *filter()* function can work as an all-around tool to grab desired data from the *sample_data* table and go from there.

# Code Structure
* The code is divided into five main segments: the first four being functions necessary to complete the analysis for parts 1-4, and the fifth being the entire main pipeline execution. Outputs are visually separated by section for ease of viewing.

* Each part is broken into one or more functions, that are tailored for the specific task, but with some flexibility to allow for reusability. For example, the table names and database file name are not hardcoded, and can be set to anything the programmer needs. In addition, *plot_cell_frequencies()* can analyze any condition-treatment pair, not just melanoma and miraclib. 

* If a part requires more than one task to be completed, e.g. creating a plot AND doing a statistcal test, then those functions are separated for compartmentalization and reusability purposes. 

* In part 3, I provided two different functions: one that analyzes cell relative frequencies based on Welch's t-test, and another that uses the Mann-Whitney U test, so there is also flexibility depending on which statistical test is preferred. My code uses Welch's t-test, but this can be switched by uncommenting line 415 instead of 410-412.

# Dashboard Link
[Streamlit Dashboard](https://teiko-technical.streamlit.app/)