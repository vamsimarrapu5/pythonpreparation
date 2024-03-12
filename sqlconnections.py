import sqlite3

con = sqlite3.connect("titanic.db")
curs = con.cursor()

one = curs.execute("SELECT * FROM titanic").fetchone()
print(one)

ten = curs.execute("SELECT * FROM titanic").fetchmany(10)
print(ten)

all_rows = curs.execute("SELECT * FROM titanic").fetchall()
print(all_rows)

all_survived = curs.execute('''SELECT * FROM titanic WHERE Survived = 1;''').fetchall()
print(all_survived)


import sqlite3

# Connecting to the database
con = sqlite3.connect("hotel_booking.db")

# Creating a cursor object
cur = con.cursor()

# Retrieving lead time for cancelled bookings
cur.execute('''SELECT lead_time FROM bra_customers WHERE is_cancelled = 1;''')
lead_time_can = cur.fetchall()

# Calculating the sum of lead times
lead_time_sum = 0
for lead_time in lead_time_can:
    lead_time_sum += lead_time[0]

# Calculating the average lead time
if len(lead_time_can) > 0:
    average_lead_time = lead_time_sum / len(lead_time_can)
    print("Average lead time for cancelled bookings:", average_lead_time)
else:
    print("No lead times found for cancelled bookings.")


import sqlite3

con = sqlite3.connect("titanic.db")
curs = con.cursor()

# Pull the Age records from the titanic table using .fetchall() method and save as age
age = curs.execute("SELECT Age FROM titanic;").fetchall()

# Create a for loop that calculates the number of children
sum = 0
for num in age: 
  if num[0] < 18:
    sum = sum + 1
            
print(sum)

