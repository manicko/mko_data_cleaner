# mko_data_cleaner
A Python tool using the dictionary to categorize (or label) each row of data in the dataset basing on the contained text.

## Test data
The script is already set to use sample data and sample dictionary. Thus you can download the repository and run main.py, to check how everething works.
The .csv file with resultes will appear in data\clean_data.

## Usage
In general process is very simple: script searches if column with specified **index** containes **search value** and for that row puts the desired output **lable** in the separate column.
Put raw data to ..\data\raw_data, update the dictionary in ..\the data\dict, check settings in the main.py. 