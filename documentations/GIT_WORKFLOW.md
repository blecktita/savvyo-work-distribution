# Team Git Workflow Recipe Book 📚

**A step-by-step guide for conflict-free collaboration**

---

## 🎯 Core Principles

1. **Main branch is SACRED** - Never commit directly to main
2. **Always start from latest main** - Your branch must be up-to-date
3. **Small, frequent merges** - Feature branches should live 2-5 days max
4. **One feature per branch** - Keep changes focused and atomic

---

## 📋 Daily Workflow Recipes

### Recipe 1: Starting a New Feature

```bash
# Step 1: Get latest main
git checkout main
git pull origin main

# Step 2: Create feature branch (use descriptive names)
git checkout -b feature/user-authentication
# OR: git checkout -b bugfix/login-validation
# OR: git checkout -b improvement/database-performance

# Step 3: Verify you're on the right branch
git status
git branch --show-current
```

**✅ Success Check:** You should see `On branch feature/your-feature-name`

---

### Recipe 2: Daily Work Routine

```bash
# Step 1: Start each day by syncing with main
git checkout main
git pull origin main
git checkout your-feature-branch
git rebase main  # This replays your commits on top of latest main

# Step 2: Make your changes and commit frequently
git add .
git commit -m "Add user registration form"

# Step 3: Push your branch (first time)
git push -u origin feature/your-feature-name

# Step 4: Push updates (subsequent times)
git push origin feature/your-feature-name
```

**⚠️ If rebase fails with conflicts:**
```bash
# Fix conflicts in the files shown
# Edit files, remove conflict markers (<<<<<<< ======= >>>>>>>)
git add .
git rebase --continue
```

---

### Recipe 3: Preparing for Merge (Before Creating PR)

```bash
# Step 1: Final sync with main
git checkout main
git pull origin main
git checkout your-feature-branch
git rebase main

# Step 2: Clean up your commits (optional but recommended)
git rebase -i main  # Interactive rebase to squash/edit commits

# Step 3: Force push your cleaned branch
git push origin feature/your-feature-name --force-with-lease

# Step 4: Create Pull Request on GitHub/GitLab
# Go to your repository and click "Compare & Pull Request"
```

**✅ Success Check:** No conflicts should appear when creating the PR

---

### Recipe 4: Reviewing & Merging Pull Requests

**For the PR Creator:**
```bash
# If reviewers request changes:
git checkout your-feature-branch
# Make requested changes
git add .
git commit -m "Address review feedback"
git push origin feature/your-feature-name
```

**For the PR Reviewer:**
```bash
# Test the feature locally (optional)
git fetch origin
git checkout feature/their-feature-name
# Test the feature
git checkout main  # Go back to main when done
```

**For merging (Team Lead/Maintainer):**
- Use GitHub/GitLab "Squash and Merge" button
- OR use command line:
```bash
git checkout main
git pull origin main
git merge --squash feature/feature-name
git commit -m "Add user authentication feature"
git push origin main
```

---

### Recipe 5: After Your PR is Merged

```bash
# Step 1: Update your main
git checkout main
git pull origin main

# Step 2: Delete the merged branch
git branch -d feature/your-feature-name
git push origin --delete feature/your-feature-name

# Step 3: Start next feature
git checkout -b feature/next-awesome-feature
```

---

## 🚨 Emergency Recipes

### Recipe 6: "I Committed to Main by Accident!"

```bash
# Step 1: Don't panic! Create a branch from current state
git checkout -b feature/accidental-commits

# Step 2: Reset main to previous state
git checkout main
git reset --hard HEAD~1  # Removes 1 commit, adjust number as needed

# Step 3: Push the corrected main
git push origin main --force-with-lease

# Step 4: Continue working on your feature branch
git checkout feature/accidental-commits
```

### Recipe 7: "My Branch is Behind and Conflicts Everywhere!"

```bash
# Step 1: Backup your work
git checkout your-branch
git checkout -b backup-my-work

# Step 2: Restart from fresh main
git checkout main
git pull origin main
git checkout -b feature/same-feature-v2

# Step 3: Cherry-pick your good commits
git cherry-pick commit-hash-1
git cherry-pick commit-hash-2
# Get commit hashes from: git log backup-my-work --oneline
```

### Recipe 8: "I Need to Fix a Bug in Production NOW!"

```bash
# Step 1: Create hotfix branch from main
git checkout main
git pull origin main
git checkout -b hotfix/critical-bug-fix

# Step 2: Make minimal fix
# Edit files
git add .
git commit -m "Fix critical login bug"

# Step 3: Fast-track merge
git push origin hotfix/critical-bug-fix
# Create PR and merge immediately

# Step 4: Update your feature branches
git checkout your-feature-branch
git rebase main  # Gets the hotfix
```

---

## 📏 Branch Naming Convention

**Use this format:** `type/short-description`

**Types:**
- `feature/` - New functionality
- `bugfix/` - Bug fixes
- `hotfix/` - Critical production fixes
- `improvement/` - Enhancements to existing features
- `docs/` - Documentation changes
- `refactor/` - Code restructuring

**Examples:**
- `feature/user-profile-settings`
- `bugfix/email-validation-error`
- `improvement/database-query-performance`
- `docs/api-endpoint-documentation`

---

## 💬 Commit Message Standards

**Format:** `Type: Brief description`

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `style:` Formatting, no code change
- `refactor:` Code restructuring
- `test:` Adding tests
- `chore:` Build tasks, etc.

**Examples:**
```bash
git commit -m "feat: Add user registration endpoint"
git commit -m "fix: Resolve email validation bug"
git commit -m "docs: Update API documentation"
git commit -m "refactor: Reorganize database models"
```

---

## 🔍 Daily Checklist

**Before Starting Work:**
- [ ] `git checkout main`
- [ ] `git pull origin main`
- [ ] `git checkout -b feature/my-new-feature`

**During Work:**
- [ ] Commit frequently with clear messages
- [ ] Push branch to origin early: `git push -u origin feature/my-feature`

**Before Creating PR:**
- [ ] `git checkout main && git pull origin main`
- [ ] `git checkout my-branch && git rebase main`
- [ ] All tests pass locally
- [ ] Code follows team standards

**After PR Merged:**
- [ ] `git checkout main && git pull origin main`
- [ ] `git branch -d feature/my-feature`
- [ ] `git push origin --delete feature/my-feature`

---

## 🆘 When Things Go Wrong

**"I have merge conflicts!"**
1. Don't panic
2. Run `git status` to see conflicted files
3. Open files, look for `<<<<<<<` markers
4. Edit files to resolve conflicts
5. `git add .` then `git rebase --continue`

**"I lost my changes!"**
1. `git reflog` shows all recent actions
2. Find your commit: `git reflog | head -20`
3. Restore: `git checkout commit-hash`

**"My branch is a mess!"**
1. Create backup: `git checkout -b backup-branch`
2. Start fresh from main
3. Cherry-pick good commits from backup

---

## 🎓 Pro Tips

1. **Use `git status` constantly** - Know what's happening
2. **Push early and often** - Your work is backed up
3. **Write descriptive commit messages** - Future you will thank you
4. **Keep branches small** - Easier to review and merge
5. **Use the GitHub/GitLab interface** - Visual PRs catch issues
6. **Communicate** - Tell team about big changes

---

## 📞 Getting Help

1. **Check branch status:** `git status`
2. **See recent changes:** `git log --oneline -10`
3. **Compare branches:** `git diff main..your-branch`
4. **Ask the team** - Share your screen, work together
5. **When in doubt, create a backup branch first!**

---

**Remember: This workflow prevents 99% of merge conflicts. Follow it religiously! 🙏**