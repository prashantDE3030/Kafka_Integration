from datetime import datetime
import random
from faker import Faker
import pandas as pd
import uuid
fake = Faker()

class GenerateSyntheticData:
    """ This class will be used to generate the 
    synthetic data"""

    # def __init__(self,number_of_record):
    #     self.records_num = number_of_record
    

    def generate_product_data(records_num):
        """ This function is used to generate the synthetic data
        products"""

        categories = {
                "Electronics": {
                    "price_range": (500, 200000),
                    "products": [
                        "Smart TV", "Bluetooth Headphones", "Wireless Mouse", "Gaming Keyboard",
                        "Smartphone", "Tablet", "Laptop", "Smart Watch", "USB Cable",
                        "Portable Speaker", "Webcam", "Power Bank", "Wireless Charger",
                        "Smart Home Hub", "Fitness Tracker", "Drone", "VR Headset",
                        "Digital Camera", "Monitor", "Router"
                    ]
                },
                "Clothing": {
                    "price_range": (1500, 30000),
                    "products": [
                        "T-Shirt", "Jeans", "Dress", "Jacket", "Sneakers", "Hoodie",
                        "Shorts", "Sweater", "Blouse", "Pants", "Skirt", "Coat",
                        "Boots", "Sandals", "Hat", "Scarf", "Gloves", "Socks"
                    ]
                },
                "Books": {
                    "price_range": (500, 4000),
                    "products": [
                        "Novel", "Cookbook", "Biography", "Science Fiction", "Mystery",
                        "Self-Help Book", "History Book", "Art Book", "Travel Guide",
                        "DataEngineering Book", "Textbook", "Poetry Collection", "Manual"
                    ]
                },
                "Home & Garden": {
                    "price_range": (100, 1500),
                    "products": [
                        "Garden Hose", "Plant Pot", "Lawn Mower", "Tool Set", "Fertilizer",
                        "Outdoor Chair", "BBQ Grill", "Watering Can", "Garden Gloves",
                        "Patio Umbrella", "Flower Seeds", "Pruning Shears", "Compost Bin"
                    ]
                },
                "Sports": {
                    "price_range": (200, 8000),
                    "products": [
                        "Running Shoes", "Basketball", "Tennis Racket", "Yoga Mat",
                        "Dumbbells", "Bicycle", "Swimming Goggles", "Football",
                        "Golf Clubs", "Hiking Boots", "Sports Water Bottle", "Exercise Band"
                    ]
                },
                "Beauty": {
                    "price_range": (80, 1500),
                    "products": [
                        "Face Cream", "Lipstick", "Shampoo", "Perfume", "Foundation",
                        "Nail Polish", "Eye Shadow", "Moisturizer", "Hair Oil",
                        "Face Mask", "Sunscreen", "Lip Balm", "Body Lotion"
                    ]
                },
                "Toys": {
                    "price_range": (5, 200),
                    "products": [
                        "Action Figure", "Puzzle", "Board Game", "Stuffed Animal",
                        "Building Blocks", "Remote Control Car", "Doll", "Art Set",
                        "Educational Toy", "Musical Instrument", "Outdoor Game"
                    ]
                },
                "Automotive": {
                    "price_range": (25, 1500),
                    "products": [
                        "Car Battery", "Motor Oil", "Brake Pads", "Air Filter",
                        "Spark Plugs", "Car Cover", "Floor Mats", "Car Charger",
                        "Tire Pressure Gauge", "Jump Starter", "Car Vacuum"
                    ]
                }
            }

        products = []

        for i in range(records_num):
            category = random.choice(list(categories.keys()))
            #print(category)
            category_info = categories[category]
            price_min, price_max = category_info["price_range"]
            #print(price_min,price_max)

            product ={
                "product_id": "PROD_"+str(uuid.uuid4()),
                "product_name": random.choice(category_info["products"]),
                "price": round(random.uniform(price_min,price_max),2),
                "category": category,
                "last_updated_time": datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            }
            #time.sleep(0.5)

            products.append(product)

        product_df = pd.DataFrame(products)
        #print(product_df)
        return product_df

# p=GenerateSyntheticData(100)
# p.generate_product_data()