from BKDataScrape import BKDataScraping

if __name__ == "__main__":
    bk_data_gathering = BKDataScraping("database.db", True)

    #bk_data_gathering.store_scraper()
    #bk_data_gathering.menu_scraper()
    bk_data_gathering.item_info_scraper()