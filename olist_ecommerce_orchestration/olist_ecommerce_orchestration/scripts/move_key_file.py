import os
import platform
import shutil
from pathlib import Path
import argparse

def find_project_root():
    """
    Traverse up from the current directory until a .git folder is found.
    Returns the path to the project root or None if not found.
    """
    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            return parent
    return None

def get_windows_downloads_dir_wsl():
    if "microsoft" in platform.uname().release.lower():
        users_dir = "/mnt/c/Users"
        skip_names = {"Default", "Default User", "Public", "All Users", "desktop.ini"}
        try:
            possible_users = [
                d for d in os.listdir(users_dir)
                if os.path.isdir(os.path.join(users_dir, d)) and d not in skip_names
            ]
        except FileNotFoundError:
            raise RuntimeError("Could not locate Windows Users directory from WSL")

        if len(possible_users) == 1:
            return f"/mnt/c/Users/{possible_users[0]}/Downloads"

        wsl_user = os.getenv("USER", "").lower()
        for u in possible_users:
            if u.lower() == wsl_user:
                return f"/mnt/c/Users/{u}/Downloads"

        print("Multiple Windows users found:", possible_users)
        selected = input("Enter your Windows username from the above list: ").strip()
        return f"/mnt/c/Users/{selected}/Downloads"
    else:
        return str(Path.home() / "Downloads")

def move_key_file(key_type, filename, source_dir=None):
    key_dir = Path.home() / ".keys"
    key_dir.mkdir(parents=True, exist_ok=True)
    target_path = key_dir / filename

    if target_path.exists():
        print(f"✅ {key_type.title()} key '{filename}' already exists at {target_path}")
    else:
        if source_dir:
            source_path = Path(source_dir) / filename
        else:
            source_path = Path(get_windows_downloads_dir_wsl()) / filename

        try:
            shutil.copy(source_path, target_path)
            print(f"✅ Copied {key_type} key '{filename}' to {target_path}")
        except FileNotFoundError:
            print(f"❌ Could not find {filename} at {source_path}")
            return

        if platform.system() != "Windows":
            try:
                os.chmod(target_path, 0o600)
                print(f"🔐 Set permissions to 600 for {key_type} key")
            except Exception as e:
                print(f"❌ Could not set permissions: {e}")
        else:
            print("ℹ️ Skipped chmod (not supported on Windows)")

    project_root = find_project_root()
    if not project_root:
        print("⚠️ Could not find project root with .git folder. Skipping .gitignore update.")
        return

    gitignore_path = project_root / ".gitignore"
    line = f".keys/{filename}"
    if gitignore_path.exists():
        lines = gitignore_path.read_text().splitlines()
    else:
        lines = []

    if line not in lines:
        with open(gitignore_path, "a") as f:
            f.write(f"\n# {key_type.title()} API credentials\n{line}\n")
        print(f"📝 Added '.keys/{filename}' to .gitignore at {gitignore_path}")
    else:
        print(f"✅ '.keys/{filename}' already in .gitignore")

    return target_path      # Return the actual moved path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Move a key file to the .keys directory and update .gitignore.")
    parser.add_argument("key_type", help="The type of key (e.g., 'API', 'SSH').")
    parser.add_argument("filename", help="The name of the key file (e.g., 'my_api_key.json', 'id_rsa').")
    parser.add_argument("--source_dir", help="Optional: The source directory of the key file. If not provided, it defaults to the Downloads directory (Windows Downloads for WSL).")

    args = parser.parse_args()

    move_key_file(args.key_type, args.filename, args.source_dir)