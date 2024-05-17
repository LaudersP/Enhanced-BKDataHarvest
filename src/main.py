from BKDataScrape import BKDataScraping

if __name__ == "__main__":
    bk_data_gathering = BKDataScraping("database.db", True)

    # Scrapes the stores in the USA
    bk_data_gathering.store_scraper()

    # Scraptes the menus at every located store
    bk_data_gathering.menu_scraper()

    # Scrapes the menu item info from menus
    bk_data_gathering.item_info_scraper()

    # Removes unneeded data
    bk_data_gathering.validate_database()

    # Plot the state data
    bk_data_gathering.plot_states()

    # Plot a graph of locations
    bk_data_gathering.plot_locations()

    # Update menu prices
    bk_data_gathering.update_menu_prices()

    # Calculate average prices
    data = bk_data_gathering.calculate_average_prices()

    # Plot the item averages
    bk_data_gathering.plot_average_prices(data)