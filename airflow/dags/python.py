import requests
import json

def get_all_files_from_github_profile(username, token=None):
    """
    Fetch all files from all public repositories of a GitHub profile.
    
    Args:
        username: GitHub username
        token: Optional GitHub personal access token (increases rate limit)
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    def get_repos(username):
        repos = []
        page = 1
        while True:
            url = f"https://api.github.com/users/{username}/repos?per_page=100&page={page}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            repos.extend(data)
            page += 1
        return repos

    def get_files_in_repo(owner, repo, path=""):
        files = []
        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return files
        items = response.json()
        for item in items:
            if item["type"] == "file":
                files.append({
                    "repo": repo,
                    "path": item["path"],
                    "name": item["name"],
                    "size": item["size"],
                    "url": item["html_url"],
                    "download_url": item.get("download_url")
                })
            elif item["type"] == "dir":
                files.extend(get_files_in_repo(owner, repo, item["path"]))
        return files

    print(f"Fetching repositories for: {username}")
    repos = get_repos(username)
    print(f"Found {len(repos)} repositories\n")

    all_files = {}

    for repo in repos:
        repo_name = repo["name"]
        print(f"Scanning repo: {repo_name} ...")
        files = get_files_in_repo(username, repo_name)
        all_files[repo_name] = files
        print(f"  → {len(files)} files found")

    return all_files


def save_results(all_files, output_file="github_files.json"):
    with open(output_file, "w") as f:
        json.dump(all_files, f, indent=2)
    print(f"\nResults saved to {output_file}")


def print_summary(all_files):
    print("\n===== SUMMARY =====")
    total = 0
    for repo, files in all_files.items():
        print(f"{repo}: {len(files)} files")
        total += len(files)
    print(f"\nTotal files across all repos: {total}")


if __name__ == "__main__":
    USERNAME = "octocat"           # Replace with target GitHub username
    TOKEN = None                   # Optional: "ghp_yourTokenHere" for higher rate limits

    all_files = get_all_files_from_github_profile(USERNAME, token=TOKEN)
    print_summary(all_files)
    save_results(all_files)