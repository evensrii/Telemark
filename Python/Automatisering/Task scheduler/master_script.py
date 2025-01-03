import subprocess
import os
from datetime import datetime
from Helper_scripts.github_functions import compare_to_github
from Helper_scripts.utility_functions import delete_files_in_temp_folder

# Paths and configurations
base_path = os.getenv("PYTHONPATH")
if base_path is None:
    raise ValueError("PYTHONPATH environment variable is not set")
LOG_DIR = os.path.join(base_path, "Automatisering", "Task scheduler", "logs")

MASTER_LOG_FILE = os.path.join(LOG_DIR, "00_master_run.log")
EMAIL_LOG_FILE = os.path.join(LOG_DIR, "00_email.log")
CONDA_ENV = "analyse"
PYTHON_PATH = os.getenv("PYTHONPATH")
if PYTHON_PATH is None:
    raise ValueError("PYTHONPATH environment variable is not set")

TEMP_FOLDER = os.environ.get("TEMP_FOLDER")
if TEMP_FOLDER is None:
    raise ValueError("TEMP_FOLDER environment variable is not set")

SCRIPTS = [
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandrere_bosatt.py"), "Innvandrere - Bosatt"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandringsgrunn.py"), "Innvandrere - Innvandringsgrunn"),
]

# Create log directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Initialize master log
with open(MASTER_LOG_FILE, "w", encoding="utf-8") as log_file:
    log_file.write(f"[{datetime.now()}] Master script initialized\n")


def run_script(script_path, task_name):
    """Run a single script in the Conda environment and log its result."""
    script_name = os.path.basename(script_path)
    script_log_file = os.path.join(LOG_DIR, f"{task_name.replace(' ', '_')}.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "Completed"
    new_data = "No"

    try:
        with open(script_log_file, "w", encoding="utf-8") as log:
            log.write(f"[{timestamp}] Starting {task_name} ({script_name})\n")
            command = f"conda activate {CONDA_ENV} && python {script_path}"
            subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT, check=True)

        # Assume the script outputs a DataFrame and logs to a specific file
        # Define GitHub parameters and compare the generated data
        file_name = f"{task_name.replace(' ', '_').lower()}.csv"
        github_folder = f"Data/09_Innvandrere og inkludering/{task_name}"
        
        # Check if new data exists using the compare_to_github function
        is_new_data = compare_to_github(df=None,  # Assume script saves this DataFrame to TEMP_FOLDER
                                        file_name=file_name,
                                        github_folder=github_folder,
                                        temp_folder=TEMP_FOLDER)
        new_data = "Ja" if is_new_data else "Nei"

        # Clean up temporary files
        delete_files_in_temp_folder()

    except subprocess.CalledProcessError as e:
        status = "Failed"
        with open(script_log_file, "a", encoding="utf-8") as log:
            log.write(f"[{timestamp}] Script failed with error: {e}\n")

    # Append to master log
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as master_log:
        master_log.write(f"[{timestamp}] {task_name} : {script_name} : {status}, {new_data}\n")


def send_email():
    """Call the email script to format and send the email."""
    with open(EMAIL_LOG_FILE, "w", encoding="utf-8") as email_log:
        command = f"conda activate {CONDA_ENV} && python D:/Scripts/analyse/Telemark/Python/Automatisering/Task scheduler/email_when_run_completed.py"
        subprocess.run(command, shell=True, stdout=email_log, stderr=subprocess.STDOUT)


def main():
    """Main function to execute all scripts and send the email."""
    # Clear old logs except readme.txt and 00_email.log
    for file in os.listdir(LOG_DIR):
        if file not in ["readme.txt", "00_email.log"]:
            os.remove(os.path.join(LOG_DIR, file))

    # Run each script
    for script, task_name in SCRIPTS:
        run_script(script, task_name)

    # Send email
    send_email()

    # Final log entry
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as master_log:
        master_log.write(f"[{datetime.now()}] Master script completed\n")


if __name__ == "__main__":
    main()
