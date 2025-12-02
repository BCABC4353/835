# Version Management

## IMPORTANT: Version Must Be Updated in Two Places

When releasing a new version, update the version number in **BOTH** of these files:

### 1. pyproject.toml (Line 3)
```toml
[project]
name = "edi-835-parser"
version = "1.0.0"  # UPDATE THIS
```

### 2. installer.iss (Line 10)
```iss
#define MyAppName "835 EDI Parser"
#define MyAppVersion "1.0.0"  # UPDATE THIS
#define MyAppPublisher "BCABC"
```

## Release Checklist

- [ ] Update version in `pyproject.toml`
- [ ] Update version in `installer.iss`
- [ ] Verify both versions match
- [ ] Update CHANGELOG (if exists)
- [ ] Commit: `git commit -m "Release vX.X.X"`
- [ ] Tag: `git tag vX.X.X`
- [ ] Push: `git push && git push --tags`
- [ ] Build installer
- [ ] Test installer on clean VM
- [ ] Create GitHub Release with installer

## Versioning Scheme

Follow Semantic Versioning (semver): `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes (e.g., 1.0.0 → 2.0.0)
- **MINOR**: New features, backward compatible (e.g., 1.0.0 → 1.1.0)
- **PATCH**: Bug fixes, backward compatible (e.g., 1.0.0 → 1.0.1)

## Examples

### Bug Fix Release (1.0.0 → 1.0.1)
- Fixed issue with path expansion
- Fixed error message PHI exposure
- No new features, no breaking changes

### Feature Release (1.0.1 → 1.1.0)
- Added recent folders dropdown
- Added About dialog
- Backward compatible with 1.0.x

### Breaking Change (1.1.0 → 2.0.0)
- Changed output CSV column structure
- Requires re-training users
- Not backward compatible

## Automation Note

In the future, consider using a single-source version management tool like:
- `bump2version` or `bumpversion`
- GitHub Actions to auto-update versions
- `setuptools_scm` to derive version from git tags

For now, **manually verify both files before every release**.
