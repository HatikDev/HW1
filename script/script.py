from random import randint

id_min = 1
id_max = 10

value_min = 10
value_max = 200

timestamp_min = 1652649570000
timestamp_max = 1652650570000

filename = "output"
count = 500

def generate_record():
	return str(randint(id_min, id_max + 1)) + "," + str(randint(timestamp_min, timestamp_max + 1)) + "," + str(randint(value_min, value_max + 1))

file = open(filename, "w")
for i in range(count):
	file.write(generate_record() + "\n")
file.close()
