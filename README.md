# Option Data Pipeline

## Overview
This repository demonstrates a data pipeline for scraping, processing, and analyzing options chain data from the CBOE website. The project showcases my skills in **data engineering, data processing, and automation** using R and partitioned Parquet files.

The pipeline is designed to:
- **Scrape** options chain data for multiple tickers.
- **Process** raw data, compute implied volatility, and extract risk metrics.
- **Store** processed data efficiently in Hive-partitioned Parquet format.
- **Automate** daily data updates using Task Scheduler on Windows.

## **Getting Started**
To run this project, ensure you have the following installed:
- **R 4.4.2+** with necessary packages (`data.table`, `httr`, `jsonlite`, `arrow`, `tidyverse`, etc.)
- **Task Scheduler (Windows)** for automation
- **Git** for version control

### **Installation**
1. Clone this repository:
   ```bash
   git clone https://github.com/gar-c/OptionData.git
   cd OptionData
