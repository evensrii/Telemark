import subprocess
import os
from datetime import datetime
from Helper_scripts.github_functions import compare_to_github, get_last_commit_time
from Helper_scripts.utility_functions import delete_files_in_temp_folder
import re
import sys

# Paths and configurations
base_path = os.getenv("PYTHONPATH")
if base_path is None:
    raise ValueError("PYTHONPATH environment variable is not set")

LOG_DIR = os.path.join(base_path, "Automatisering", "Task scheduler", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

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
    ## Klima og energi
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/klimagassutslipp.py"), "Klima og energi - Sektorvise utslipp"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/norskeutslipp.py"), "Klima og energi - Utslipp fra landbasert industri"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/okologisk_tilstand_server_versjon.py"), "Klima og energi - Okologisk tilstand vann"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/antall_felt.py"), "Klima og energi - Felte hjortedyr"),

    ## Idrett, friluftsliv og frivillighet (husk, ingen komma i oppgavenavn)
    #(os.path.join(PYTHON_PATH, "Queries/07_Idrett_friluftsliv_og_frivillighet/Friluftsliv/andel_jegere.py"), "Idrett friluftsliv og frivillighet - Jegere"),

    ## Innvandrere og inkludering
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/andel_flyktninger_og_arbeidsinnvandrere.py"), "Innvandrere - Flyktninger og arbeidsinnvandrere"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/botid.py"), "Innvandrere - Botid"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandrere_bosatt.py"), "Innvandrere - Bosatt"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandringsgrunn.py"), "Innvandrere - Innvandringsgrunn"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_innvandrere_i_lavinntekt.py"), "Innvandrere - Lavinntekt"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_innvandrere.py"), "Innvandrere - Sysselsatte"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_etter_botid_og_landbakgrunn.py"), "Innvandrere - Sysselsatte etter botid og bakgrunn"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/deltakere_introduksjonsprogram.py"), "Innvandrere - Deltakere introduksjonsprogrammet"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/etter_introduksjonsprogram.py"), "Innvandrere - Etter introduksjonsprogrammet"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/enslige_mindreaarige.py"), "Innvandrere - Enslige mindreaarige"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/anmodninger_og_faktisk_bosetting.py"), "Innvandrere - Anmodninger og faktisk bosetting"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/sekundaerflytting.py"), "Innvandrere - Sekundaerflytting"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_fullfort_vgo.py"), "Innvandrere - Fullfort VGO"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_hoyeste_utdanning.py"), "Innvandrere - Hoyeste utdanning"),
    
    ## Areal og stedsutvikling
    #(os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/jordbruksareal_per_kommune.py"), "Areal - Jordbruksareal per kommune"),
    #(os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/fulldyrka_vs_ikke-fulldyrka.py"), "Areal - Fulldyrka vs ikke-fulldyrka"),
]

# Initialize master log
with open(MASTER_LOG_FILE, "w", encoding="utf-8") as log_file:
    log_file.write(f"[{datetime.now()}] Master script initialized\n")


def run_script(script_path, task_name):
    """Run a single script in the Conda environment and log its result."""
    script_name = os.path.basename(script_path) #Eks. "innvandrere_botid.py"
    # Use the task name as-is for the log file name
    script_log_file = os.path.join(LOG_DIR, f"{task_name}.log") #Eks. "Innvandrere - Botid.log"
    timestamp = datetime.now().strftime("%d.%m.%Y  %H:%M:%S")
    status = "Completed"
    new_data = "No"

    # Get the script's module name and import it
    sys.path.append(os.path.dirname(script_path))
    module_name = os.path.splitext(script_name)[0]

    try:
        # Run the script and capture its output
        result = subprocess.run(
            [
                "conda",
                "run",
                "-n",
                CONDA_ENV,
                "python",
                script_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        # Write script output to its log file
        with open(script_log_file, "w", encoding="utf-8") as log:
            log.write(result.stdout)
            if result.stderr:
                log.write("\nErrors/Warnings:\n")
                log.write(result.stderr)

        # Check if new data was detected
        if "New data detected" in result.stdout or "New data detected" in result.stderr:
            new_data = "Yes"

        # Get the last commit time for the CSV file
        # Extract github_folder and file_name from the script's output
        github_folder_match = re.search(r"github_folder\s*=\s*['\"]([^'\"]+)['\"]", result.stdout)
        file_name_match = re.search(r"file_name\s*=\s*['\"]([^'\"]+)['\"]", result.stdout)
        
        last_commit = None
        if github_folder_match and file_name_match:
            github_folder = github_folder_match.group(1)
            file_name = file_name_match.group(1)
            file_path = f"{github_folder}/{file_name}"
            last_commit = get_last_commit_time(file_path)

    except subprocess.CalledProcessError as e:
        status = "Failed"
        # Write error output to the script's log file
        with open(script_log_file, "w", encoding="utf-8") as log:
            if e.stdout:
                log.write(e.stdout)
            if e.stderr:
                log.write("\nErrors:\n")
                log.write(e.stderr)

    # Log to master log file
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as log:
        log_entry = f"[{timestamp}] {task_name}: {script_name}: {status}, {new_data}"
        if last_commit:
            log_entry += f", {last_commit}"
        log.write(log_entry + "\n")


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
