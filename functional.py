nums = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

# filter_values is not a higher-order function
def filter_values(predicate, lst):

  # Mutable list required because this example is imperative, not declarative
  ret = []
  for i in lst:
    if predicate(i):
      ret.append(i)
  return ret

filtered_numbers = filter_values(lambda x: x % 2 == 0, nums) 

print(filtered_numbers) 

nums = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

filtered_numbers = filter(lambda x: x % 2 == 0, nums) 

print(tuple(filtered_numbers))


nums = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

def mapper(function, lst):
  ret = []
  for i in lst:
    ret.append(function(i))
  return ret

mapped_numbers  = mapper(lambda x: x*x, nums)

print(tuple(mapped_numbers))


numbers = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

mapped_numbers = map(lambda x: x*x, numbers) 

print(tuple(mapped_numbers))

from functools import reduce
nums = (16, 2, 19, 22, 10, 23, 16, 2, 27, 29, 19, 26, 12, 20, 16, 29, 6, 2, 12, 20)

filtered_numbers = filter(lambda x: x % 2 == 0, nums)

# Print the result
print(tuple(filtered_numbers))

mapped_numbers = map(lambda x: x * 3, nums)

# Print the result
print(tuple(mapped_numbers))
product = reduce(lambda x, y: x + y, nums)
print(product)


from collections import namedtuple


country = namedtuple("country", ["name", "capital", "continent"])

France = country(name="France", capital="Paris", continent="Europe")
Japan = country(name="Japan", capital="Tokyo", continent="Asia")
Senegal = country(name="Senegal", capital="Dakar", continent="Africa")

# Accessing fields for France
print("Country Name:", France.name)
print("Capital:", France.capital)
print("Continent:", France.continent)


print("Country Name:", Japan.name)
print("Capital:", Japan.capital)
print("Continent:", Japan.continent)


print("Country Name:", Senegal.name)
print("Capital:", Senegal.capital)
print("Continent:", Senegal.continent)

countries = (France, Japan, Senegal)


print("First Country Name:", countries[0].name)
print("First Country Capital:", countries[0].capital)
print("First Country Continent:", countries[0].continent)


from functools import reduce
nums = (16, 2, 19, 22, 10, 23, 16, 2, 27, 29, 19, 26, 12, 20, 16, 29, 6, 2, 12, 20)

filtered_numbers = filter(lambda x: x % 2 == 0, nums)

# Print the result
print(tuple(filtered_numbers))

mapped_numbers = map(lambda x: x * 3, nums)

# Print the result
print(tuple(mapped_numbers))
product = reduce(lambda x, y: x + y, nums)
print(product)

from collections import namedtuple

# Create a class called student
student = namedtuple("student", ["name", "grade", "course_number"]) 

peter = student("Peter", 'B', 101)
amanda = student("Amanda", 'C', 101 )
sarah = student("Sarah", 'A', 102)
lisa = student("Lisa", 'D', 101)
alex = student("Alex", 'A', 102)
maria = student("Maria", 'B', 101)
andrew = student("Andrew", 'C', 102)

math_class = (peter, amanda, sarah, lisa, alex, maria, andrew)

math_201 = map(lambda s: student(s.name, 'X', 201), filter(lambda q: q.grade <= 'B', math_class))

print(tuple(math_201))

nums = (2, 12, 5, 8, 9, 3, 16, 7, 13, 19, 21, 1, 15, 4, 22, 20, 11)

# Checkpoint 1 code goes here.
greater_than_10_doubled = map(lambda x: x*2, filter(lambda y: y > 10, nums))

print(tuple(greater_than_10_doubled))

# Checkpoint 2 code goes here.
functional_way = map(lambda x: x * 3, filter(lambda y: y % 3 == 0, nums))

print(tuple(functional_way))


