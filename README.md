# Enhanced BKDataHarvest

## Overview
Welcome to the Enhanced BKDataHarvest, an advanced fork of LLM Explorer's BKDataHarvest tool. This project extends the original functionality by scraping location and menu data from approximately 6,670 Burger King locations across the United States. This scraping script powers a website where users can interact and access the menu data based on the United States of America.

## Features
- store_scraper: Scrapes the US and gets a database of stores.
- menu_scraper: Scrapes all the stores in the database for the menus.
- item_info_scraper: Scrapes all the menus in the database for info.
- validate_database: Cleans up the database, excluding unneeded data for saving storage.
- plot_states: Creates a bar graph of the number of stores per state.
- plot_locations: Creates a plot map of the stores based on their latitude and longitude coordinates.
- update_menu_prices: Updates item prices for menu data.
- generate_json: Exports a JSON file for a data-displaying website.
- plot_average_prices: Creates four bar graphs to display the state average price for four menu items.

## Installation
Clone this repository using:
```bash
git clone https://github.com/LaudersP/Enhanced-BKDataHarvest.git
```
Navigate into the project directory:
```bash
cd Enhanced-BKDataHarvest
```
Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage
To start collecting data, comment out the undesired functions in the `main.py` file:
Then run the script using:
```bash
python main.py
```

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgments
- Original BKDataHarvest project by LLM Explorer
