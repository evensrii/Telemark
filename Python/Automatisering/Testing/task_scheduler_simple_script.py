import sys

sys.path.append("E:\Dokumenter\Python-leking\API-test\custom_functions\")

with open("hello_world.txt", "w") as file:
    # file.write("Hello, World!")
    file.write(f"{sys.path}")
    file.close()

""" import sys

# Legger til mappen "custom_functions" i sys.path (en liste med mapper hvor interpreter leter etter moduler)
# sys.path.append(".." + "\custom_functions")
sys.path.append("E:\Dokumenter\Python-leking\API-test\custom_functions\")

from github_functions import upload_to_github

##################### Opplasting til Github #####################

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

github_token = "IKKE FYLL DIREKTE INN HER HVIS JEG VIL PUSHE TIL GITHUB!"
source_file = "E:\Dokumenter\Python-leking\API-test\data\task_scheduler_test.txt"
destination_folder = "test_folder"
github_repo = "evensrii/python_testing"
git_branch = "main"

upload_to_github(github_token, source_file, destination_folder, github_repo, git_branch) """