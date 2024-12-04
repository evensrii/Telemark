import datetime

# Get the current time
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Log script start
with open("D:/Scripts/analyse/Telemark/Python/Automatisering/Task scheduler/logs/log_test.txt", "a") as log_file:
    log_file.write(f"{current_time}: Task Scheduler test script started.\n")

# Simulate work
try:
    with open("D:/Scripts/analyse/Telemark/Python/Automatisering/Task scheduler/logs/log_test.txt", "a") as log_file:
        log_file.write(f"{current_time}: Task Scheduler test executed.\n")
except Exception as e:
    with open("D:/Scripts/analyse/Telemark/Python/Automatisering/Task scheduler/logs/log_test.txt", "a") as log_file:
        log_file.write(f"{current_time}: Error - {str(e)}\n")