## Se https://gaurav-adarshi.medium.com/different-ways-to-schedule-tasks-in-python-45e03d5411ee Method 4
## https://crontab.guru/#*_*_*_*_* for crontab syntax

# Cron krever i utgangspunktet Linux, men kan brukes i Windows med WSL:
# https://www.howtogeek.com/746532/how-to-launch-cron-automatically-in-wsl-on-windows-10-and-11/

# pip install crontab

from crontab import CronTab

# Create a new crontab object
cron = CronTab(user="username")

# Add a new cron job to run the script every day at 6 AM
job = cron.new(command="python crontab_scheduled_script.py")
job.setall("5-6 1-12 * * *")

# Write the job to the user's crontab
cron.write()
