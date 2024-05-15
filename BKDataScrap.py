import sqlite3
from BKClient import BKClient
from tqdm import tqdm

class BKDataScraping:
    def __init__(self, database_path, show_process_bar=False):
        self.database_path = database_path
        self.show_progress = show_process_bar
        self.client = BKClient()

    def __process_state(self, cursor, states):
        if self.show_progress:
            state_data = tqdm(states.items(), total=len(states))
        else:
            state_data = states.items()

        for _, bounds in state_data:
            lat_start, lon_start = bounds[0]
            lat_end, lon_end = bounds[1]

            stores = self.client.search_lat_lon(lat_start, lat_end, lon_start, lon_end)

            for store_id, store_info in stores.items():
                _id = store_info['_id']
                city = store_info['physicalAddress']['city'].title().replace(',', '')
                state_name = store_info['physicalAddress']['stateProvince']
                postal_code = store_info['physicalAddress']['postalCode'].split('-')[0]
                latitude = store_info['latitude']
                longitude = store_info['longitude']

                if state_name in states:
                    try:
                        cursor.execute('INSERT INTO stores VALUES (?, ?, ?, ?, ?, ?, ?)',
                                    (_id, store_id, city, state_name, postal_code, latitude, longitude))
                    except sqlite3.IntegrityError:
                        print(f"Duplicate entry for {_id}, skipping...")
                else:   
                    f = open('manual_review_required.txt', 'a')
                    f.write(f"ID: {_id}\n")
                    f.write(f"Store: {store_id}\n")
                    f.write(f"State: {state_name}\n")
                    f.write(f"Postal Code: {postal_code}\n")
                    f.write(f"Lat: {latitude}\n")
                    f.write(f"Long: {longitude}\n")
                    f.write("------------------------------\n")
                    f.close()

    def __process_menu_scrape(self, store_ids, menu):
        if self.show_progress:
            stores = tqdm(store_ids, total=len(store_ids))
        else:
            stores = store_ids

        for store_id in stores:
            try:
                scraped_menu = self.client.get_menu(store_id)
                if scraped_menu:
                    menu[store_id] = scraped_menu
            except Exception as e:
                print(f"ERROR: {e}")

    def __process_menus(self, menus, cursor):
        if self.show_progress:
            items = tqdm(menus.items(), total=len(menus))
        else:
            items = menus.items()

        for store_id, menu in items:
            for item in menu:
                item_id = item.get('id')
                price = item.get('price')

                if price is None:
                    continue

                price_min = price.get('min')
                price_max = price.get('max')
                price_default = price.get('default')

                if price_min * price_max * price_default == 0:
                    continue

                try:
                    cursor.execute("INSERT OR REPLACE INTO menus VALUES (?, ?, ?, ?, ?)",
                               (store_id, item_id, price_min, price_max, price_default))
                except sqlite3.Error as e:
                    print(f"An error occurred: {e}")


    def store_scraper(self):
        states = {
            'Alabama': ((30.137521, -88.473227), (35.008028, -84.888246)),
            'Alaska': ((51.209712, -179.148909), (71.538800, -129.979510)),
            'Arizona': ((31.332177, -114.818269), (37.004260, -109.045223)),
            'Arkansas': ((33.004106, -94.617919), (36.499602, -89.644395)),
            'California': ((32.534156, -124.409591), (42.009518, -114.131211)),
            'Colorado': ((36.993076, -109.045223), (41.003444, -102.041524)),
            'Connecticut': ((40.950943, -73.727775), (42.050587, -71.786994)),
            'Delaware': ((38.451013, -75.789023), (39.839007, -75.048939)),
            'Florida': ((24.396308, -87.634938), (31.000968, -80.031362)),
            'Georgia': ((30.357851, -85.605165), (35.000659, -80.839729)),
            'Hawaii': ((18.910361, -178.334698), (28.402123, -154.806773)),
            'Idaho': ((41.988057, -117.243027), (49.001146, -111.043564)),
            'Illinois': ((36.970298, -91.512982), (42.508481, -87.019935)),
            'Indiana': ((37.771743, -88.097792), (41.760592, -84.784579)),
            'Iowa': ((40.375501, -96.639485), (43.501196, -90.140061)),
            'Kansas': ((36.993076, -102.051744), (40.003166, -94.588413)),
            'Kentucky': ((36.497129, -89.571509), (39.147458, -81.964970)),
            'Louisiana': ((28.928609, -94.043147), (33.019457, -88.817017)),
            'Maine': ((42.977764, -71.083924), (47.459686, -66.949895)),
            'Maryland': ((37.911717, -79.487651), (39.722302, -75.048939)),
            'Massachusetts': ((41.186328, -73.508142), (42.886589, -69.928393)),
            'Michigan': ((41.696118, -90.418135), (48.306063, -82.413474)),
            'Minnesota': ((43.499356, -97.239209), (49.384358, -89.491739)),
            'Mississippi': ((30.173943, -91.655009), (34.996052, -88.097888)),
            'Missouri': ((35.995683, -95.774704), (40.613640, -89.098843)),
            'Montana': ((44.358221, -116.049153), (49.001146, -104.039138)),
            'Nebraska': ((39.999998, -104.052700), (43.001708, -95.308050)),
            'Nevada': ((35.001857, -120.005746), (42.002207, -114.039648)),
            'New Hampshire': ((42.696778, -72.556532), (45.305476, -70.704705)),
            'New Jersey': ((38.788657, -75.563587), (41.357423, -73.893979)),
            'New Mexico': ((31.332301, -109.050173), (37.000232, -103.001964)),
            'New York': ((40.496103, -79.762591), (45.015850, -71.856214)),
            'North Carolina': ((33.842316, -84.321869), (36.588117, -75.460621)),
            'North Dakota': ((45.935054, -104.048897), (49.000492, -96.554507)),
            'Ohio': ((38.403202, -84.820308), (41.977523, -80.518693)),
            'Oklahoma': ((33.615833, -103.002565), (37.002206, -94.431185)),
            'Oregon': ((41.991794, -124.566244), (46.292035, -116.463262)),
            'Pennsylvania': ((39.719798, -80.519891), (42.269690, -74.689516)),
            'Rhode Island': ((41.146339, -71.862772), (42.018798, -71.120570)),
            'South Carolina': ((32.034600, -83.354790), (35.215402, -78.541839)),
            'South Dakota': ((42.479635, -104.057698), (45.94545, -96.436622)),
            'Tennessee': ((34.982921, -90.310298), (36.678118, -81.646900)),
            'Texas': ((25.837377, -106.645646), (36.500704, -93.508292)),
            'Utah': ((36.998982, -114.052962), (42.001567, -109.041058)),
            'Vermont': ((42.726933, -73.431723), (45.016659, -71.510225)),
            'Virginia': ((36.540738, -83.675299), (39.466012, -75.242266)),
            'Washington': ((45.543541, -124.848974), (49.002494, -116.916071)),
            'West Virginia': ((37.201483, -82.644739), (40.638801, -77.719519)),
            'Wisconsin': ((42.491983, -92.888114), (47.302488, -86.249548)),
            'Wyoming': ((40.994746, -111.056888), (45.005904, -104.052700))
        }

        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stores (
                       _id TEXT PRIMARY KEY,
                       store_id INTEGAR,
                       city TEXT,
                       state TEXT,
                       postal_code TEXT,
                       latitude REAL,
                       longitude REAL
            )
        ''')

        self.__process_state(cursor, states)

        conn.commit()
        conn.close()
        print("Store scraping completed!")

    def menu_scraper(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS menus (
                store_id INTEGER,
                item_id TEXT,
                price_min INTEGER,
                price_max INTEGER,
                price_default INTEGER,
                PRIMARY KEY (store_id, item_id)
            )
        ''')

        cursor.execute('''
            SELECT store_id
            FROM stores
        ''')

        store_ids = {row[0] for row in cursor.fetchall()}
        menus = {}
        self.__process_menu_scrape(store_ids, menus)
        self.__process_menus(menus, cursor)

        conn.commit()
        cursor.close()
        conn.close()
        print("Menu scraping completed!")

    def item_info_scraper(self, threads=1):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                item_id TEXT PRIMARY KEY,
                item_name TEXT
            )
        ''')

        cursor.execute('''
            SELECT item_id
            FROM menus
                       ''')

        item_ids_short = {row[0] for row in cursor.fetchall()}
        for id in tqdm(item_ids_short, total=len(item_ids_short)):
            itemInfo = self.client.get_item_info(id)
            item_id = itemInfo[0]
            if item_id == id:
                item_name = itemInfo[1]
                
                try:
                    cursor.execute('''
                        INSERT INTO items (item_id, item_name)
                        VALUES (?, ?)
                    ''', (item_id, item_name))
                except sqlite3.IntegrityError as e:
                    print(f"Error: {e}")

        conn.commit()
        cursor.close() 
        conn.close()       
        print("Item info scraping completed!")