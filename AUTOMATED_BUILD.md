# Automated Installer Builds

This project uses GitHub Actions to automatically build the Windows installer when you push to Git.

## How It Works

Just like your PowerBI viewer program, when you push code to GitHub, it automatically:
1. ✅ Builds the Python executable with PyInstaller
2. ✅ Creates the Windows installer with InnoSetup
3. ✅ Uploads the installer as a downloadable artifact

**No manual build steps required!**

## Getting Your Installer

### Option 1: Every Push to Master (Recommended)

Every time you push to the `master` branch, GitHub Actions builds an installer automatically.

**To download it:**
1. Go to: https://github.com/BCABC4353/835/actions
2. Click on the latest workflow run
3. Scroll down to "Artifacts"
4. Download `835-EDI-Parser-Setup-XXXXXXX.exe`

The installer is kept for **30 days**.

### Option 2: Create a Release

For official releases, create a version tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This will:
1. Build the installer
2. Create a GitHub Release
3. Attach the installer to the release
4. Keep the installer **forever** (not just 30 days)

**To download:**
1. Go to: https://github.com/BCABC4353/835/releases
2. Download `835-EDI-Parser-Setup-v1.0.0.exe`

## Workflow Files

Two workflows are configured:

1. **build-on-push.yml** - Builds on every push to master
   - Artifacts stored for 30 days
   - Quick testing builds

2. **build-installer.yml** - Builds when you create a version tag
   - Creates GitHub Releases
   - Artifacts stored permanently
   - For distributing to users

## Your Workflow

### For Development/Testing:
```bash
# Make changes to code
git add .
git commit -m "Your changes"
git push

# Wait 3-5 minutes
# Go to GitHub Actions and download the installer
```

### For Official Releases:
```bash
# Update version in pyproject.toml first
# Then create and push a tag
git tag v1.0.1
git push origin v1.0.1

# Wait 3-5 minutes
# Go to GitHub Releases and download the installer
```

## Comparison to PowerBI Viewer

This works **exactly the same** as your PowerBI viewer:

| PowerBI Viewer | 835 EDI Parser |
|----------------|----------------|
| Push to Git → automatic build | Push to Git → automatic build |
| Download from Actions | Download from Actions |
| Tag for release | Tag for release |
| No local build needed | No local build needed |

The only difference is the underlying technology:
- PowerBI Viewer: Electron app
- 835 Parser: Python app

But the **workflow is identical**!

## Manual Build (If Needed)

You can still build locally if needed:
```bash
pip install pyinstaller
pyinstaller build_installer.spec
# Then compile installer.iss in InnoSetup
```

But there's no need - just push to Git!

## Checking Build Status

After pushing, you can see the build progress:
1. Go to: https://github.com/BCABC4353/835/actions
2. Click on the running workflow
3. Watch the live build logs

Typical build time: **3-5 minutes**

## Troubleshooting

**Q: Where's my installer?**
A: GitHub Actions → Click the workflow run → Scroll to "Artifacts" section at the bottom

**Q: Build failed?**
A: Check the workflow logs for errors. Most common: missing dependencies or syntax errors in Python code

**Q: Can I trigger a build without pushing code?**
A: Yes! Go to Actions → "Build Installer on Push" → "Run workflow"

## Summary

**You asked:** "Why is this different from my powerbi-viewer program?"

**Answer:** It's not different anymore!

✅ Push to Git → Installer builds automatically
✅ Download from GitHub Actions
✅ No manual build steps
✅ Same workflow as your PowerBI viewer

Just push your code and grab the installer from GitHub Actions. That's it!
