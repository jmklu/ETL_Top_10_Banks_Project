# Largest Banks Data ETL Pipeline

## Overview
This is a Coursera course (IBM Data Engineering) project which is a comprehensive ETL (Extract, Transform, Load) pipeline designed to scrape, process, and analyze data related to the largest banks worldwide. The data is extracted from a Wikipedia page listing the banks and their market capitalizations in USD. The pipeline then transforms this data, converting market capitalizations into GBP, EUR, and INR using exchange rates, and finally loads the processed data into both a CSV file and a SQLite database.


## Features
- **Web Scraping**: Utilizes Python libraries such as `requests` and `BeautifulSoup` to extract relevant information from a specified Wikipedia page.
- **Data Transformation**: Converts market capitalizations from USD to GBP, EUR, and INR using exchange rates from a CSV file.
- **Data Storage**: Saves the processed data in a CSV file (`Largest_banks_data.csv`) for easy accessibility and a SQLite database (`Banks.db`) for more structured data storage.
- **Logging**: Keeps a log file (`code_log.txt`) to record progress and important events during the execution of the ETL process.


## Dependencies
- **pandas**: For data manipulation and storage.
- **numpy**: For numerical operations.
- **requests**: For making HTTP requests.
- **beautifulsoup4**: For web scraping.
- **sqlite3**: For database operations.


## File Structure
- **banks_project.py**: Main script for the ETL process.
- **exchange_rate.csv**: CSV file containing exchange rates for currency conversion.
- **Largest_banks_data.csv**: Final processed data saved as a CSV file.
- **Banks.db**: SQLite database file for structured data storage.
- **code_log.txt**: Log file recording the execution progress.


## Contribution
Feel free to contribute by submitting bug reports, feature requests, or pull requests. Your feedback and collaboration are highly appreciated!

