from BKDataScrap import BKDataScraping

if __name__ == "__main__":
    bk_data_gathering = BKDataScraping("database.db")

    bk_data_gathering.store_scraper()