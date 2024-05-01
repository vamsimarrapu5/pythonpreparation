def display_as_percentage(val):
  return '{:.1f}%'.format(val * 100)

# Write code here
def calculate_simple_return(start_price, end_price, dividend=0):
  return (end_price - start_price + dividend) / start_price

simple_return = calculate_simple_return(200, 250, 20)

print('The simple rate of return is', display_as_percentage(simple_return))

# Import library here
from math import log

def display_as_percentage(val):
  return '{:.1f}%'.format(val * 100)

# Write code here
def calculate_log_return(start_price, end_price):
  return log(end_price / start_price)

log_return = calculate_log_return(200, 250)

print('The log rate of return is', display_as_percentage(log_return))

def display_as_percentage(val):
  return '{:.1f}%'.format(val * 100)

daily_return_a = 0.001
monthly_return_b = 0.022

# Write code here
print('The daily rate of return for Investment A is', display_as_percentage(daily_return_a))

print('The monthly rate of return for Investment B is', display_as_percentage(monthly_return_b))

def annualize_return(log_return, t):
  return log_return * t

annual_return_a = annualize_return(daily_return_a, 252)
print('The annual rate of return for Investment A is', display_as_percentage(annual_return_a))

annual_return_b = annualize_return(monthly_return_b, 12)
print('The annual rate of return for Investment B is', display_as_percentage(annual_return_b))

import numpy as np

returns_disney = [0.22, 0.12, 0.01, 0.05, 0.04]
returns_cbs = [-0.13, -0.15, 0.31, -0.06, -0.29]

variance_disney = np.var(returns_disney)
variance_cbs = np.var(returns_cbs)

# Write code here
dataset = [10, 8, 9, 10, 12]

def calculate_variance(dataset):
  mean = sum(dataset) / len(dataset)

  numerator = 0
  for data in dataset:
    numerator += (data - mean) ** 2

  variance = numerator / len(dataset)
  
  return variance

variance_disney = calculate_variance(returns_disney)
variance_cbs = calculate_variance(returns_cbs)

print('The variance of Disney stock returns is', variance_disney)
print('The variance of CBS stock returns is', variance_cbs)
