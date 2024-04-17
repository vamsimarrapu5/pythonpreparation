# List comprehension to filter and modify elements
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even_squares = [x ** 2 for x in numbers if x % 2 == 0]

# Using zip to iterate over multiple lists simultaneously
names = ['Alice', 'Bob', 'Charlie']
ages = [30, 25, 35]
for name, age in zip(names, ages):
    print(f'{name} is {age} years old')

# Sorting a list of tuples based on a specific element
points = [(1, 2), (3, 1), (5, 5), (2, 3)]
points_sorted_by_y = sorted(points, key=lambda x: x[1])

# Using enumerate to get both index and value in a loop
fruits = ['apple', 'banana', 'cherry']
for index, fruit in enumerate(fruits):
    print(f'Index: {index}, Fruit: {fruit}')

# Dictionary comprehension to create a dictionary from two lists
keys = ['a', 'b', 'c']
values = [1, 2, 3]
my_dict = {k: v for k, v in zip(keys, values)}

# Merging dictionaries using unpacking (**)
dict1 = {'a': 1, 'b': 2}
dict2 = {'c': 3, 'd': 4}
merged_dict = {**dict1, **dict2}

# Using collections.defaultdict for default values
from collections import defaultdict
my_defaultdict = defaultdict(int)  # int() returns 0 by default
my_defaultdict['a'] += 1  # Increment the value for key 'a'

# Using dict.setdefault to set default values
my_dict = {}
my_dict.setdefault('a', []).append(1)  # Append 1 to the list value of key 'a'

# Set comprehension to perform operations on sets
set1 = {1, 2, 3, 4, 5}
set2 = {3, 4, 5, 6, 7}
union_set = {x for x in set1.union(set2) if x % 2 == 0}

# Using set intersection to find common elements
common_elements = set1.intersection(set2)

# Using set difference to find elements unique to one set
unique_to_set1 = set1.difference(set2)

# Checking for subset or superset relationship
is_subset = {1, 2}.issubset(set1)
is_superset = set1.issuperset({1, 2})



