#!/bin/bash
# setup_github_repo.sh
# Setup GitHub repository for distributed scraping

set -e

REPO_NAME="savvyo-work-distribution"
REPO_URL=""

echo "🚀 Setting up GitHub repository for distributed scraping..."

# Check if git is installed
if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed. Please install git first."
    exit 1
fi

# Get GitHub repository URL from user
read -p "📝 Enter GitHub repository URL (or press Enter to create local repo): " REPO_URL

if [ -n "$REPO_URL" ]; then
    echo "📥 Cloning repository: $REPO_URL"
    git clone "$REPO_URL" "$REPO_NAME"
    cd "$REPO_NAME"
else
    echo "📁 Creating local repository: $REPO_NAME"
    mkdir -p "$REPO_NAME"
    cd "$REPO_NAME"
    git init
fi

# Create folder structure
echo "📂 Creating folder structure..."
mkdir -p available_competitions
mkdir -p claimed_competitions  
mkdir -p completed_competitions
mkdir -p failed_competitions

# Create .gitkeep files to ensure folders are tracked
touch available_competitions/.gitkeep
touch claimed_competitions/.gitkeep
touch completed_competitions/.gitkeep
touch failed_competitions/.gitkeep

# Create README
cat > README.md << 'EOF'
# Scraping Work Distribution

This repository manages distributed scraping work orders.

## Folder Structure

- `available_competitions/` - Work orders ready to be claimed by workers
- `claimed_competitions/` - Work orders currently being processed
- `completed_competitions/` - Finished work with results
- `failed_competitions/` - Failed work orders for retry

## Usage

### Host Machine (Database + Task Creator)
```bash
python host_work_manager.py --repo-url <github-repo-url>
```

### Worker Machines (Task Processors)
```bash
python worker_main.py --repo-url <github-repo-url>
```

## Work Order Format

```json
{
  "work_id": "comp_GB1_uuid",
  "competition_id": "GB1",
  "competition_url": "https://...",
  "completed_seasons": ["2020", "2021"],
  "created_at": "2025-07-16T10:30:00Z"
}
```
EOF

# Create .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
.venv/
.env

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
logs/
EOF

# Initial commit
echo "💾 Creating initial commit..."
git add .
git commit -m "Initial setup of distributed scraping work repository"

# Push if remote URL provided
if [ -n "$REPO_URL" ]; then
    echo "📤 Pushing to remote repository..."
    git push -u origin main || git push -u origin master
fi

echo "✅ Repository setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Share this repository URL with all machines:"
if [ -n "$REPO_URL" ]; then
    echo "   $REPO_URL"
else
    echo "   $(pwd)"
    echo "   (Upload to GitHub and share the URL)"
fi
echo ""
echo "2. On HOST machine (with database), run:"
echo "   python host_work_manager.py --repo-url <repo-url>"
echo ""
echo "3. On WORKER machines, run:"
echo "   python worker_main.py --repo-url <repo-url>"
echo ""
echo "🎉 Ready for distributed scraping!"