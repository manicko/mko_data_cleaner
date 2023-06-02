# mko_data_cleaner
A Python tool using the dictionary to categorize (or label) each 
row of data in the dataset basing on the contained text.

## How it works?
Script loops through rows of data and searches if column with the specified **column index** contains 
**search value** (any occurrence, not a complete match) and for that row puts the required **label** 
in the separate column.

## Test data
The script is already set to use sample data from 'data_cleaner\data\raw_data\example_data.csv' 
with the sample dictionary from 'data_cleaner\data\dict\example_dict.csv'. 
Thus, you can download the repository and run main.py, to check how everything works.
The .csv file with results will appear in 'data_cleaner\data\clean_data'.

## Usage
In general process is very simple:
1. Set up the dictionary: specify **search value**, **column index** 
   (the number of a column to search starting from 0) to search for and the output **label**.
2. Change settings in the **main.py** and **default_settings.py**
3. Run the main.py