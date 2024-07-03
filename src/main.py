from BKDataScrape import BKDataScraping

if __name__ == "__main__":
    bk_data_gathering = BKDataScraping("database.db", True)

    # Scrapes the stores in the USA
    bk_data_gathering.store_scraper()

    # Scrapes the menus at every located store
    bk_data_gathering.menu_scraper()

    # Scrapes the menu item info from menus
    bk_data_gathering.item_info_scraper()

    # Removes unneeded data
    bk_data_gathering.validate_database()

    # Plot the state data
    bk_data_gathering.plot_states()

    # Plot a graph of locations
    bk_data_gathering.plot_locations()

    # Calculate average prices
    bk_data_gathering.generate_json("data.json")

    # Plot the item averages
    item_names = ["Whopper", "16 Pc. Chicken Nuggets", "Big Fish", "Large Coca-Cola"]
    bk_data_gathering.plot_average_prices("average_prices.json", item_names)