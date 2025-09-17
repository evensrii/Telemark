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
    ## Befolkning
    (os.path.join(PYTHON_PATH, "Queries/01_Befolkning/Befolkningsframskrivinger/befolkningsframskrivinger_historiske.py"), "Befolkning - Befolkningsframskrivinger historiske"),
    (os.path.join(PYTHON_PATH, "Queries/01_Befolkning/Flytting/flytting_til_og_fra_telemark.py"), "Flytting - Til og fra Telemark"),
    (os.path.join(PYTHON_PATH, "Queries/01_Befolkning/Befolkningsutvikling/folketall.py"), "Befolkning - Folketall"),
    
    ## Opplæring og kompetanse
    (os.path.join(PYTHON_PATH, "Queries/02_Opplæring_og_kompetanse/Utdanningsnivå/utdanningsnivaa.py"), "Opplaering og kompetanse - Utdanningsnivaa"),
    (os.path.join(PYTHON_PATH, "Queries/02_Opplæring_og_kompetanse/Utdanningsnivå/utdanningsnivaa_telemark_og_landet.py"), "Opplaering og kompetanse - Utdanningsnivaa Telemark og landet"),

    ## Arbeid og næringsliv
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Arbeidsliv/Sysselsetting/arbeidsmarkedstilknytning.py"), "Arbeid og naeringsliv - Arbeidsmarkedstilknytning per kommune"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Arbeidsliv/Sysselsetting/sysselsetting_alder.py"), "Arbeid og naeringsliv - Sysselsatte etter aldersgruppe"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Arbeidsliv/Sysselsetting/sysselsetting_i_kommunene.py"), "Arbeid og naeringsliv - Sysselsatte i kommunene"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Næringsliv/Naringsstruktur_og_arbeidsplasser/offentlig_vs_privat_sysselsetting.py"), "Arbeid og naeringsliv - Offentlig vs. privat naeringsliv"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Næringsliv/Naringsstruktur_og_arbeidsplasser/arbeidsplasser_i_naringer.py"), "Arbeid og naeringsliv - Sysselsatte i naeringer"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Næringsliv/Naringsstruktur_og_arbeidsplasser/arbeidsplasser_fliser.py"), "Arbeid og naeringsliv - Sysselsatte (fliser)"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Næringsliv/Naringsstruktur_og_arbeidsplasser/endring_arbeidsplasser_over_tid.py"), "Arbeid og naeringsliv - Endring arbeidsplasser over tid"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Næringsliv/Virksomheter/virksomheter.py"), "Arbeid og naeringsliv - Virksomheter"),

    ## Arbeid og næringsliv - NAV
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Arbeidsliv/NAV/arbeidsledighet.py"), "NAV - Arbeidsledighet"),
    (os.path.join(PYTHON_PATH, "Queries/03_Arbeid_og_naeringsliv/Arbeidsliv/NAV/nedsatt_arbeidsevne.py"), "NAV - Nedsatt arbeidsevne"),

    ## Klima og energi
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/klimagassutslipp.py"), "Klima og energi - Sektorvise utslipp"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/norskeutslipp.py"), "Klima og energi - Utslipp fra landbasert industri"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/okologisk_tilstand.py"), "Klima og energi - Okologisk tilstand vann"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/antall_felt.py"), "Klima og energi - Felte hjortedyr"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Produksjon/NVE/vannkraft.py"), "Klima og energi - Vannkraft Telemark"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Produksjon/SSB/kraftproduksjon_ssb.py"), "Klima og energi - Kraftproduksjon Telemark (SSB)"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Strompriser/strompriser.py"), "Klima og energi - Strompriser"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Forbruk/Elhub/elhub.py"), "Klima og energi - Stromforbruk (Elhub)"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Produksjon/Elhub/installert_effekt.py"), "Klima og energi - Installert_effekt (Elhub)"),

    ## Idrett, friluftsliv og frivillighet (husk, ingen komma i oppgavenavn)
    (os.path.join(PYTHON_PATH, "Queries/07_Idrett_friluftsliv_og_frivillighet/Friluftsliv/andel_jegere.py"), "Idrett friluftsliv og frivillighet - Jegere"),

    ## Innvandrere og inkludering
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/andel_flyktninger_og_arbeidsinnvandrere.py"), "Innvandrere - Flyktninger og arbeidsinnvandrere"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/botid.py"), "Innvandrere - Botid"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandrere_bosatt.py"), "Innvandrere - Bosatt"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandringsgrunn.py"), "Innvandrere - Innvandringsgrunn"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_innvandrere_i_lavinntekt_fhi.py"), "Innvandrere - Lavinntekt"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_innvandrere.py"), "Innvandrere - Sysselsatte"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_etter_botid_og_landbakgrunn.py"), "Innvandrere - Sysselsatte etter botid og bakgrunn"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/deltakere_introduksjonsprogram.py"), "Innvandrere - Deltakere introduksjonsprogrammet"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/etter_introduksjonsprogram.py"), "Innvandrere - Etter introduksjonsprogrammet"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/enslige_mindreaarige.py"), "Innvandrere - Enslige mindreaarige"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/anmodninger_og_faktisk_bosetting.py"), "Innvandrere - Anmodninger og faktisk bosetting"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/sekundaerflytting.py"), "Innvandrere - Sekundaerflytting"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_fullfort_vgo.py"), "Innvandrere - Fullfort VGO"),
    (os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_hoyeste_utdanning.py"), "Innvandrere - Hoyeste utdanning"),
    
    ## Areal og stedsutvikling
    (os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/jordbruksareal_per_kommune.py"), "Areal - Jordbruksareal per kommune"),
    (os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/fulldyrka_vs_ikke-fulldyrka.py"), "Areal - Fulldyrka vs ikke-fulldyrka"),

    ## Bystrategi Grenland
    (os.path.join(PYTHON_PATH, "Queries/Bystrategi_Grenland/Klima/klimagassutslipp_vei.py"), "Bystrategi Grenland - Utslipp fra vei"),
    (os.path.join(PYTHON_PATH, "Queries/Bystrategi_Grenland/Areal_og_byutvikling/arealprofiler.py"), "Bystrategi Grenland - Arealprofiler"),
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
    last_commit = None  # Initialize last_commit at the start

    try:
        # First read the script content to extract github_folder and file_name
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script_content = f.read()
                
            # Extract github_folder and potential file names
            github_folder_match = re.search(r'github_folder\s*=\s*["\']([^"\']+)["\']', script_content)
            file_name_matches = re.findall(r'file_name\d*\s*=\s*["\']([^"\']+)["\']', script_content)
            
            github_folder = github_folder_match.group(1) if github_folder_match else None

            # Get the last commit time for the CSV files if we found the necessary info
            if github_folder and file_name_matches:
                from Helper_scripts.github_functions import get_last_commit_time
                # Get the most recent commit time among all related files
                commit_times = []
                for file_name in file_name_matches:
                    file_path = f"{github_folder}/{file_name}"
                    commit_time = get_last_commit_time(file_path)
                    if commit_time:
                        commit_times.append(commit_time)
                
                if commit_times:
                    # Use the most recent commit time
                    last_commit = max(commit_times)
        except Exception as e:
            print(f"Warning: Could not extract file information from {script_path}: {e}")
            # Continue execution even if we can't get the commit time
        
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
    # Clear all logs except readme.txt
    for file in os.listdir(LOG_DIR):
        if file != "readme.txt":
            os.remove(os.path.join(LOG_DIR, file))

    # Run each script
    for script, task_name in SCRIPTS:
        run_script(script, task_name)

    # Send email
    send_email()

    # Clean up individual status log files from the Python/Log folder
    status_log_dir = os.path.join(PYTHON_PATH, "Log")
    if os.path.exists(status_log_dir):
        for file in os.listdir(status_log_dir):
            if (file.startswith("new_data_status_") and file.endswith(".log")) or \
               (file.startswith("last_commit_") and file.endswith(".log")):
                try:
                    os.remove(os.path.join(status_log_dir, file))
                except Exception as e:
                    print(f"Warning: Could not remove status file {file}: {e}")

    # Final log entry
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as master_log:
        master_log.write(f"[{datetime.now()}] Master script completed\n")


if __name__ == "__main__":
    main()
