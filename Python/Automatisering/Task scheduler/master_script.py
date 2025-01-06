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
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/botid.py"), "Innvandrere - Botid"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/andel_flyktninger_og_arbeidsinnvandrere.py"), "Innvandrere - Flyktninger og arbeidsinnvandrere"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/deltakere_introduksjonsprogram.py"), "Innvandrere - Deltakere introduksjonsprogrammet"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/enslige_mindreaarige.py"), "Innvandrere - Enslige mindreaarige"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/anmodninger_og_faktisk_bosetting.py"), "Innvandrere - Anmodninger og faktisk bosetting"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/sekundaerflytting.py"), "Innvandrere - Sekundaerflytting")
]

# Create log directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Initialize master log
with open(MASTER_LOG_FILE, "w", encoding="utf-8") as log_file:
    log_file.write(f"[{datetime.now()}] Master script initialized\n")


def run_script(script_path, task_name):
    """Run a single script in the Conda environment and log its result."""
    script_name = os.path.basename(script_path)
    # Use the task name as-is for the log file name
    script_log_file = os.path.join(LOG_DIR, f"{task_name}.log")
    timestamp = datetime.now().strftime("%d.%m.%Y  %H:%M:%S")
    status = "Completed"
    new_data = "No"

    try:
        # Run the individual script with proper quoting
        script_path = os.path.abspath(script_path)  # Ensure absolute path
        command = f'cmd.exe /c "conda activate {CONDA_ENV} && python "{script_path}""'
        
        with open(script_log_file, "w", encoding="utf-8") as log:
            log.write(f"[{timestamp}] Started {task_name} ({script_name})\n")
            subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT, check=True)

        # Read new data status (if applicable)
        task_name_safe = task_name.replace(" ", "_").replace(".", "_")
        new_data_status_file = os.path.join(LOG_DIR, f"new_data_status_{task_name_safe}.log")
        if os.path.exists(new_data_status_file):
            with open(new_data_status_file, "r", encoding="utf-8") as status_file:
                line = status_file.read().strip()
                _, _, new_data = line.split(",")
            os.remove(new_data_status_file)  # Clean up

    except subprocess.CalledProcessError as e:
        status = "Failed"
        with open(script_log_file, "a", encoding="utf-8") as log:
            log.write(f"[{timestamp}] Script failed with error: {e}\n")

    # Append results to master log
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as master_log:
        master_log.write(f"[{timestamp}] {task_name} : {script_name} : {status}, {new_data}\n")
        master_log.flush()  # Ensure the write is complete

def send_email():
    """Call the email script to format and send the email."""
    with open(EMAIL_LOG_FILE, "w", encoding="utf-8") as email_log:
        # Dynamically construct the path to the email script
        python_path = os.getenv("PYTHONPATH")
        if python_path is None:
            raise ValueError("PYTHONPATH environment variable is not set")
        
        email_script_path = os.path.join(
            python_path, "Automatisering", "Task scheduler", "email_when_run_completed.py"
        )
        
        # Quote the script path to handle spaces
        command = f'cmd.exe /c "conda activate {CONDA_ENV} && python \"{email_script_path}\""'
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
