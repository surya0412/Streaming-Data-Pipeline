import json

a = {'id': 0, 'shop': 'Salma', 'name': ' faijal', 'phoneNumber': '(772)656-8181', 'address': '0353 Combs Ford Apt. 235\nConnieborough, NH 73298', 'food_item': [{'FoodName': 'Fish Biryani', 'AddOns': ['tuna', 'tomato', 'curry']}, {'FoodName': 'Fish Biryani', 'AddOns': ['curry', 'tuna']}, {'FoodName': 'Neyi Karam Dosa', 'AddOns': ['curry']}, {'FoodName': 'Dosa Chicken', 'AddOns': ['chutney']}], 'amount': '1503 INR', 'publish_timestamp': '2021-09-18 15:37:55.582335'}

a = json.dumps(a, indent=4)

print(a)