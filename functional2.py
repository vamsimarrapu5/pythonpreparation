from collections import namedtuple
from functools import reduce

# Prices are in USD
menu_item = namedtuple("menu_item", ["name", "dish_type", "price"])

jsp = menu_item("Jumbo Shrimp Platter", "Appetizer", 29.95)
lc = menu_item("Lobster Cake", "Appetizer", 30.95)
scb = menu_item("Sizzling Canadian Bacon", "Appetizer", 9.95)
ccc = menu_item("Codecademy Crab Cake", "Appetizer", 32.95)
cs = menu_item("Caeser Salad", "Salad", 14.95)
mgs = menu_item("Mixed Green Salad", "Salad", 10.95)
cp = menu_item("Codecademy Potatoes", "Side", 34.95)
mp = menu_item("Mashed Potatoes", "Side", 14.95)
a = menu_item("Asparagus", "Side", 15.95)
rs = menu_item("Ribeye Steak", "Entree", 75.95)
phs = menu_item("Porter House Steak", "Entree", 131.95)
grs = menu_item("Grilled Salmon", "Entree", 36.95)

menu = (jsp, lc, scb, ccc, cs, mgs, cp, mp, a, rs, phs, grs)
entree = 0
least_expensive = 0

entree = reduce(lambda x, y: x if x.price > y.price else y, filter(lambda x: x.dish_type == "Entree", menu))

print(entree)

least_expensive= reduce(lambda x, y: x if x.price < y.price else y, filter(lambda x: x.dish_type == "Side" or x.dish_type == "Salad", menu))

print(least_expensive)

from functools import reduce

fruits = {"Grape":(4, 6, 2), "Lemon":(7, 3, 1), "Orange":(5, 8, 1), "Apple":(2, 8, 10), "Watermelon":(0, 9, 6)}

total_fruits = 0

total_fruits = reduce(lambda x, y: x + y, map(lambda q: fruits[q][0] + fruits[q][1] + fruits[q][2], fruits))

print(total_fruits)



from functools import reduce

costs = {"shirt": (4, 13.00), "shoes":(2, 80.00), "pants":(3, 100.00), "socks":(5, 5.00), "ties":(3, 14.00), "watch":(1, 145.00)}

nums = (24, 6, 7, 16, 8, 2, 3, 11, 21, 20, 22, 23, 19, 12, 1, 4, 17, 9, 25, 15)

total = reduce(lambda x, y: x+y, filter(lambda r: r > 150.00, map(lambda q: costs[q][0] * costs[q][1], costs)))

print(total)

product = -1

product = reduce(lambda x, y: x * y, map(lambda z: z + 5, filter(lambda q: q < 10, nums)))

print(product)


import csv
from collections import namedtuple
from functools import reduce

tree = namedtuple("tree", ["index", "width", "height", "volume"]) 

with open('trees.csv', newline = '') as csvfile:
  reader = csv.reader(csvfile, delimiter=',', quotechar='|')
  next(reader)
  mapper = map(lambda x: tree(int(x[0].strip()), float(x[1]), int(x[2]), float(x[3])), reader)
  
  t = filter(lambda x: x.height > 75, mapper)
  trees = tuple(t)
  widest = reduce(lambda x, y: x if x.width > y.width else y, trees)
  print(widest)
  
 
import json
from collections import namedtuple
from functools import reduce

city = namedtuple("city", ["name", "country", "coordinates", "continent"])

with open('cities.json') as json_file:
  data = json.load(json_file) 

cities = map(lambda x: city(x["name"], x["country"], x["coordinates"], x["continent"]), data["city"])

asia = tuple(filter(lambda q: q.continent == "Asia", cities))

print(asia)

west = None

west = reduce(lambda x, y: x if x.coordinates[1] < y.coordinates[1] else y, asia)

print(west)


import csv
from functools import reduce

def count(predicate, itr):
  # task 1
  count_filter = filter(predicate, itr)
  # task 2
  count_reduce = reduce(lambda x, y: x + 1, count_filter, 0)
  return count_reduce

def average(itr):
  iterable = iter(itr)
  # task 8
  return avg_helper(0, iterable, 0)

# task 3
def avg_helper(curr_count, itr, curr_sum): 
  # task 4
  next_num = next(itr, "null")
  # task 5
  if next_num == "null": 
    return curr_sum/curr_count
  curr_count += 1 
  # task 4
  curr_sum += next_num
  # task 6
  return avg_helper(curr_count, itr, curr_sum)


with open('1kSalesRec.csv', newline = '') as csvfile:
  reader = csv.reader(csvfile, delimiter=',', quotechar='|')
  fields = next(reader)
  # task 9
  count_belgium = count(lambda x: x[1] == "Belgium", reader)
  print(count_belgium)
  csvfile.seek(0)
  # task 10
  avg_portugal = average(map(lambda x: float(x[13]),filter(lambda x: x[1] == "Portugal", reader)))
  print(avg_portugal)