import pendulum

# define a string with the timestamp in UTC
timestamp_str = "2023-03-22 10:30:00"

# create a Pendulum datetime object from the string in UTC timezone
timestamp_obj_utc = pendulum.parse(timestamp_str, tz='UTC')

# convert the datetime object to the Israel timezone
timestamp_obj_israel = timestamp_obj_utc.in_timezone('Israel')

# print the timestamp in the Israel timezone
print(timestamp_obj_israel)


import pendulum

# create a Pendulum datetime object with the current time in the Israel timezone
current_time_israel = pendulum.now('Israel')

# format the datetime object as a string
current_time_israel_str = current_time_israel.format('YYYY-MM-DD HH:mm:ss')

# print the current time in Israel
print("Current time in Israel:", current_time_israel_str)
