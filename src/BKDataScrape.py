import json
import matplotlib.pyplot as plt
import sqlite3
from BKClient import BKClient
from matplotlib.colors import to_hex
from matplotlib import gridspec as gridspec
from tqdm import tqdm
from itertools import chain

class BKDataScraping:
    def __init__(self, database_path, show_process_bar=False):
        self.database_path = database_path
        self.show_progress = show_process_bar
        self.client = BKClient()

    def __process_items(self, items, process_func, desc):
        if self.show_progress:
            items = tqdm(items, total=len(items), desc=desc)
        
        for item in items:
            process_func(item)

    def __process_state(self, cursor, states):
        def process_state(state):
            lat_start, lon_start = state[1][0]
            lat_end, lon_end = state[1][1]
            stores = self.client.search_lat_lon(lat_start, lat_end, lon_start, lon_end)

            insert_query = '''
                INSERT INTO stores (_id, store_id, city, state, postal_code, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            '''

            for store_id, store_info in stores.items():
                    _id = store_info['_id']
                    city = store_info['physicalAddress']['city'].title().replace(',', '')
                    state_name = store_info['physicalAddress']['stateProvince']
                    postal_code = store_info['physicalAddress']['postalCode'].split('-')[0]
                    latitude = store_info['latitude']
                    longitude = store_info['longitude']

                    if state_name in states:
                        try:
                            cursor.execute(insert_query, (_id, store_id, city, state_name, postal_code, latitude, longitude))
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

        self.__process_items(states.items(), process_state, "Store Scraping")

    def __process_menu_scrape(self, store_ids, menu):
        def process_store(store_id):
            try:
                scraped_menu = self.client.get_menu(store_id)
                if scraped_menu:
                    menu[store_id] = scraped_menu
            except Exception as e:
                print(f"ERROR: {e}")

        self.__process_items(store_ids, process_store, "Menu Scraping")

    def __process_menus(self, menus, cursor):
        insert_query = '''
            INSERT OR REPLACE INTO menus (store_id, item_id, price_min, price_max, price_default)
            VALUES (?, ?, ?, ?, ?)
        '''

        def process_menu(menu):
            store_id, menu = menu
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
                    cursor.execute(insert_query, (store_id, item_id, price_min, price_max, price_default))
                except sqlite3.Error as e:
                    print(f"An error occurred: {e}")

        self.__process_items(menus.items(), process_menu, "Item Scraping")
    
    def __get_item_id(self, item_names, cur):
        select_query = '''
            SELECT item_id
            FROM items
            WHERE item_name = ?
        '''

        def process_item(item):
            cur.execute(select_query, (item,))
            ids = [row[0] for row in cur.fetchall()]
            return ids

        return [process_item(item) for item in item_names]

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

        select_query = '''
            SELECT store_id
            FROM stores
        '''

        cursor.execute(select_query)

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

        select_query = '''
            SELECT item_id
            FROM menus
        '''

        insert_query = '''
            INSERT INTO items (item_id, item_name)
            VALUES (?, ?)
        '''

        cursor.execute(select_query)

        item_ids_short = {row[0] for row in cursor.fetchall()}
        for id in tqdm(item_ids_short, total=len(item_ids_short), desc="Inserting Item Data"):
            itemInfo = self.client.get_item_info(id)
            item_id = itemInfo[0]
            if item_id == id:
                item_name = itemInfo[1]
                
                try:
                    cursor.execute(insert_query, (item_id, item_name))
                except sqlite3.IntegrityError as e:
                    print(f"Error: {e}")

        conn.commit()
        cursor.close() 
        conn.close()       
        print("Item info scraping completed!")

    def validate_database(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        
        total_deleted_rows = 0
        pbar = tqdm(total=4, desc="Validating database")

        delete_stores_query = '''
            DELETE FROM stores
            WHERE store_id NOT IN (SELECT store_id
                                    FROM menus)
        '''
        
        try:
            cursor.execute(delete_stores_query)
            total_deleted_rows += cursor.rowcount
            conn.commit()
            pbar.update(1)
        except sqlite3.OperationalError as e:
            print(f"Error: {e}")

        select_consistent_items_query = '''
            SELECT i.item_name
            FROM items i
            JOIN menus m ON i.item_id = m.item_id
            JOIN stores s ON m.store_id = s.store_id
            WHERE i.item_name NOT LIKE '%Meal%'
            GROUP BY i.item_name
            HAVING COUNT(DISTINCT s.state) = (SELECT COUNT(DISTINCT state) FROM stores);
        '''

        try:
            cursor.execute(select_consistent_items_query)
            consistent_items = [row[0] for row in cursor.fetchall()]
            pbar.update(1)

            if consistent_items:
                consistent_items_id = self.__get_item_id(consistent_items, cursor)
                consistent_items_id = chain.from_iterable(consistent_items_id)
                consistent_items_id = list(set(filter(lambda x: x.startswith('item_'), consistent_items_id)))
                
                try:
                    in_params = tuple(consistent_items_id)
                    cursor.execute("DELETE FROM items WHERE item_id NOT IN (%s)" % (" , ".join(["?"] * len(in_params))), in_params)
                    total_deleted_rows += cursor.rowcount
                    conn.commit()
                except sqlite3.Error as e:
                    print(f"\nError deleting unused items: {e}")
            else:
                print("\nNo unused items to delete!")

            pbar.update(1)
        except sqlite3.OperationalError as e:
            print(f"ERROR: {e}")        

        select_item_id_query = '''
            SELECT item_id
            FROM items
        '''

        try:
            cursor.execute(select_item_id_query)
            rows = cursor.fetchall()
            item_ids = [row[0] for row in rows]
            
            if item_ids:
                try:
                    in_params = tuple(item_ids)
                    cursor.execute("DELETE FROM menus WHERE item_id NOT IN (%s)" % (" , ".join(["?"] * len(in_params))), in_params)
                    total_deleted_rows += cursor.rowcount
                    conn.commit()
                except sqlite3.Error as e:
                    print(f"Error deleting menus: {e}")
            else:
                print("No menus to delete.")
                
            pbar.update(1)
        except sqlite3.OperationalError as e:
            print(f"Error: {e}")

        cursor.close()
        conn.close() 
        pbar.close()

        print(f"\nTotal database entries removed: {total_deleted_rows}")
        print("Database validation completed!")

    def update_menu_prices(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        select_query = '''
            SELECT store_id, item_id
            FROM menus
        '''

        cursor.execute(select_query)

        store_item_pairs = [(row[0], row[1]) for row in cursor.fetchall()]
        updated_menus = {}

        def process_store_item_pair(store_item_pair):
            store_id, item_id = store_item_pair
            try:
                scraped_menu = self.client.get_menu(store_id)
                if scraped_menu:
                    updated_menus[store_id] = scraped_menu
            except Exception as e:
                print(f"ERROR: {e}")

        self.__process_items(store_item_pairs, process_store_item_pair, "Updating Menu Prices")

        update_query = '''
            UPDATE menus
            SET price_min = ?, price_max = ?, price_default = ?
            WHERE store_id = ? AND item_id = ?
        '''

        def process_updated_menu(menu):
            store_id, menu = menu
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
                    cursor.execute(update_query, (price_min, price_max, price_default, store_id, item_id))
                except sqlite3.Error as e:
                    print(f"An error occurred: {e}")

        self.__process_items(updated_menus.items(), process_updated_menu, "Updating Menu Prices")

        conn.commit()
        cursor.close()
        conn.close()
        print("Menu prices updated!")

    def calculate_average_prices(self, json_filename):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        create_table_query = '''
            CREATE TABLE IF NOT EXISTS average_prices (
                item_name TEXT,
                state TEXT,
                average_price REAL,
                PRIMARY KEY (item_name, state)
            );
        '''
        cursor.execute(create_table_query)

        insert_query = '''
            INSERT OR REPLACE INTO average_prices (item_name, state, average_price)
            SELECT i.item_name, s.state, AVG(m.price_default) as average_price
            FROM stores s
            JOIN menus m ON s.store_id = m.store_id
            JOIN items i ON m.item_id = i.item_id
            GROUP BY i.item_name, s.state;
        '''
        cursor.execute(insert_query)
        conn.commit()

        select_query = '''
            SELECT item_name, state, average_price
            FROM average_prices;
        '''
        cursor.execute(select_query)
        rows = cursor.fetchall()

        state_data = {}
        for row in rows:
            item_name, state, average_price = row
            if state not in state_data:
                state_data[state] = {"state": state}
            state_data[state][item_name] = average_price

        data = list(state_data.values())

        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        cursor.close()
        conn.close()
    
    def plot_states(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        select_query = '''
            SELECT state, COUNT(*) as count
            FROM stores
            GROUP BY state
        '''

        cursor.execute(select_query)
        results = cursor.fetchall()
        conn.close()

        states = [row[0] for row in results]
        counts = [row[1] for row in results]

        plt.figure(figsize=(10, 6))
        plt.bar(states, counts, color='skyblue')
        plt.xlabel('State')
        plt.ylabel('Count')
        plt.title('Number of Burger Kings in Each State')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()

    def plot_locations(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        select_query = '''
            SELECT latitude, longitude, state
            FROM stores
        '''
        cursor.execute(select_query)
        data = cursor.fetchall()

        lat_contiguous = []
        long_contiguous = []
        lat_hawaii = []
        long_hawaii = []
        lat_alaska = []
        long_alaska = []
        for row in data:
            if row[2] == 'Hawaii':
                lat_hawaii.append(row[0])
                long_hawaii.append(row[1])
            elif row[2] == 'Alaska':
                lat_alaska.append(row[0])
                long_alaska.append(row[1])
            else:
                lat_contiguous.append(row[0])
                long_contiguous.append(row[1])

        conn.close()

        fig = plt.figure(figsize=(15, 7))
        gs = gridspec.GridSpec(2, 3, width_ratios=[1, 2, 2], height_ratios=[1, 1])

        ax0 = fig.add_subplot(gs[0, 0])
        ax0.scatter(long_alaska, lat_alaska, s=10)
        ax0.set_xlabel('Longitude')
        ax0.set_ylabel('Latitude')
        ax0.set_title('Alaska')
        ax0.grid(True)

        ax1 = fig.add_subplot(gs[1, 0])
        ax1.scatter(long_hawaii, lat_hawaii, s=10)
        ax1.set_xlabel('Longitude')
        ax1.set_ylabel('Latitude')
        ax1.set_title('Hawaii')
        ax1.grid(True)

        ax2 = fig.add_subplot(gs[:, 1:])
        ax2.scatter(long_contiguous, lat_contiguous, s=10)
        ax2.set_xlabel('Longitude')
        ax2.set_ylabel('Latitude')
        ax2.set_title('Contiguous US')
        ax2.grid(True)

        plt.tight_layout()
        plt.show()
    
    def plot_average_prices(self, json_file_path, item_names):
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        states = [entry["state"] for entry in data]
        average_prices = []
        for item_name in item_names:
            item_prices = [entry.get(item_name, None) for entry in data]
            average_prices.append(item_prices)

        # Plotting
        figsize = (18, 9)
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=figsize)
        fig.subplots_adjust(wspace=0.3, hspace=0.4)

        highlight_color_max = 'red'
        highlight_color_min = 'green'

        for i, ax in enumerate(axes.flat):
            avg_prices = average_prices[i]  
            # Fill missing prices with None to maintain correct indexing
            avg_prices = [price if price is not None else None for price in avg_prices]
            
            # Find max and min indices, excluding None values
            valid_prices = [p for p in avg_prices if p is not None]
            max_index = avg_prices.index(max(valid_prices)) if valid_prices else None
            min_index = avg_prices.index(min(valid_prices)) if valid_prices else None

            ax.bar(states, avg_prices, color=[
                highlight_color_max if j == max_index else 
                highlight_color_min if j == min_index else 
                to_hex('C0') for j, _ in enumerate(avg_prices)
            ])
            ax.set_title(f'Average Price of {item_names[i]}')
            ax.set_xlabel('State')
            ax.set_ylabel('Price in Cents')
            ax.set_xticklabels(states, rotation=90)

        fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.14), ncol=4)  
        plt.tight_layout()
        plt.show()