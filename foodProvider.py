import random
import pandas as pd
from faker.providers import BaseProvider
from faker.providers.phone_number import Provider

class FoodProviders(BaseProvider):
    def food_item(self):
        food_item_names = [
            'Chicken Mugalai Biryani',
            'Guntur Gongura Biryani',
            'Hyderabadi Chicken Biryani',
            'Ulavacharu Biryani',
            'Dosa Chicken',
            'Idly Mutton',
            'Nellore Fish Curry',
            'Neyi Karam Dosa',
            'Chicken Curry',
            'Chicken Fry',
            'Prawns Biryani',
            'Fish Biryani',
            'Mutton Biryani'
        ]
        return food_item_names[random.randint(0, len(food_item_names)-1)]

    def Indian_Names(self):
        with open("C:/ApacheBeam/Streaming-Data-Pipeline/config/Indian_Names.txt", "r") as f:
            content = f.read()
        Names_list = content.split(',')
        return Names_list[random.randint(0, len(Names_list)-1)]

    def food_price(self):
        price = [220, 380]
        return random.randint(price[0],price[1])

    def AddOn_price(self):
        price = [20, 70]
        return random.randint(price[0],price[1])

    def AddOns(self):
        AddOns_list = [
            'tomato',
            'Egg',
            'curry',
            'chutney',
            'sambar',
            'curd',
            'carrot',
            'olives',
            'garlic',
            'tuna',
            'onion',
            'pineapple',
            'strawberry',
            'banana'
        ]
        return  AddOns_list [random.randint(0, len(AddOns_list)-1)]

    def Restaurant_Name(self):
        Restaurant_Name_List = [
            'KPR',
            'Murali Krishna',
            'Janatha Dabha',
            'Salma',
            'Vengamaba Dabha',
            'Andhara Spice',
            'SSA',
            'Nirvana',
            'Venkata Ramana',
            'Reshma',
            'Celebrations',
            'BBQ'
        ]
        return Restaurant_Name_List[random.randint(0, len(Restaurant_Name_List)-1)]

   