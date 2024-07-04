##################### Github: Opplasting eller oppdatering #####################

import datetime as dt
from github import Github
import os
from dotenv import load_dotenv

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.


def upload_to_github(
    source_file: str,
    destination_folder: str,
    github_repo: str,
    git_branch: str,
) -> None:
    """
    Uploads a file to a GitHub Pages repository using the PyGithub library.
    Parameters:
        github_token: Github token for authentication
        source_file: The path of the local file to be uploaded
        destination_folder: The path of the folder in the GitHub repository where the file will be uploaded
        github_repo: The name of the GitHub repository
        git_branch: The name of the branch in the GitHub repository
    """

    ## Github token ligger i en egen fil (token.env) i samme mappe som modulen, men skal ikke lastes opp til Github (.gitignore)

    # Get the directory of this script
    module_dir = os.path.dirname(__file__)

    # Load environment variables from .env file in module directory
    dotenv_path = os.path.join(module_dir, ".env")
    load_dotenv(dotenv_path)

    github_token = os.getenv("GITHUB_TOKEN")
    if github_token is None:
        raise ValueError(
            "GitHub token not found. Please set it in the environment variables or .env file."
        )

    # Create a Github instance using token
    g = Github(github_token)
    # Get the repository object
    repo = g.get_repo("evensrii/Telemark")
    # Get the branch object
    branch = repo.get_branch("main")
    # Create the path of the file in the GitHub repository
    path = destination_folder + "/" + source_file.split("/")[-1]
    # Current time for commit message
    ct = dt.datetime.now()
    # Create or update the file in the GitHub repository
    try:
        # Get the existing file details if it exists
        existing_file = repo.get_contents(path, ref=branch.name)
        # Update the file (kun hvis endringer er gjort, da opprettes en ny "sha" (hash))
        repo.update_file(
            path,
            "Updated file at " f"{ct}",
            open(source_file, "rb").read(),
            existing_file.sha,
            branch=branch.name,
        )
        print(f"File '{path}' updated successfully.")
    except Exception as e:
        # If the file does not exist, create it
        repo.create_file(
            path,
            "Created file at " f"{ct}",
            open(source_file, "rb").read(),
            branch=branch.name,
        )
        print(f"File '{path}' created successfully.")


""" SHA (dvs. hvis endringer): Git assigns each commit a unique ID, called a SHA or hash, that identifies:

The specific changes
When the changes were made
Who created the changes """
