"""
Q5.py - utilities to supply data to the templates.

This file contains a pair of functions for retrieving and manipulating data
that will be supplied to the template for generating the table.
"""
import csv


def username():
    return 'lye60'


def data_wrangling(filter_class: str = None):
    """
    Args:
        - filter_class (str): Optional parameter that specifies the animal class
            to filter the data for.
    """
    with open('data/q5.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        table = list()
        # Feel free to add any additional variables
        dropdown_options = set()  # Set to store unique animal classes
        
        # Read in the header
        for header in reader:
            break
        
        # Read in each row
        for row in reader:
            row_data = [row[0], row[1], int(row[2])]
            table.append(row_data)
            dropdown_options.add(row_data[1])  # Add the class (second column) to the set
        
        # Programmatically get unique classes and sort alphabetically for dropdown - [2 point] Q5.4.a
        dropdown_options = sorted(dropdown_options)
        
        # Filter, sort, and limit the table - [3 points] Q5.4.b
        # Filter the data by the class column (second column)
        if filter_class:
            # Filter the data by the class column (second column)
            table = [row for row in table if row[1] == filter_class]
        # Order table by the count column (last column) - don't need to worry about tiebreaks
        table = sorted(table, key=lambda x: x[2], reverse=True)
        # Take only the first 10 rows
        table = table[:10]
    
    return header, table, dropdown_options
