#!/bin/bash
# Script to test the build process locally, simulating GitHub Actions

echo "=== Simulating GitHub Actions Build Process ==="
echo

# Clean up any existing build artifacts
echo "1. Cleaning build artifacts..."
rm -rf dist/ build/ *.egg-info

# Install dependencies
echo -e "\n2. Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install -r test-requirements.txt

# Simulate bump2version (without actually bumping)
echo -e "\n3. Current version in pyproject.toml:"
grep "version = " pyproject.toml

# Run tests
echo -e "\n4. Running tests..."
python -m unittest discover -s tests || { echo "Tests failed!"; exit 1; }

# Install build tools
echo -e "\n5. Installing build tools..."
pip install --upgrade build twine

# Build the package
echo -e "\n6. Building package..."
python -m build --sdist --wheel --outdir dist/

# Check with twine
echo -e "\n7. Checking distribution with twine..."
twine check dist/*

# List dist contents
echo -e "\n8. Contents of dist directory:"
ls -la dist/

# Check wheel metadata
echo -e "\n9. Checking wheel metadata:"
python -c "import zipfile, glob; whl = glob.glob('dist/*.whl')[0]; z = zipfile.ZipFile(whl); print(z.read([f for f in z.namelist() if f.endswith('METADATA')][0]).decode()[:500])"

# Simulate the pypa/gh-action-pypi-publish check
echo -e "\n10. Simulating PyPI publish action check..."
python -c "
import zipfile
import glob
import sys

wheel_file = glob.glob('dist/*.whl')[0]
print(f'Checking wheel: {wheel_file}')

with zipfile.ZipFile(wheel_file, 'r') as z:
    metadata_files = [f for f in z.namelist() if f.endswith('METADATA')]
    if not metadata_files:
        print('ERROR: No METADATA file found in wheel!')
        sys.exit(1)
    
    metadata_content = z.read(metadata_files[0]).decode('utf-8')
    
    # Check for required fields
    required_fields = ['Name:', 'Version:']
    missing_fields = []
    
    for field in required_fields:
        if field not in metadata_content:
            missing_fields.append(field)
    
    if missing_fields:
        print(f'ERROR: Missing required fields: {missing_fields}')
        print('First 1000 chars of METADATA:')
        print(metadata_content[:1000])
        sys.exit(1)
    
    # Extract name and version
    for line in metadata_content.split('\\n'):
        if line.startswith('Name:'):
            print(f'  {line}')
        elif line.startswith('Version:'):
            print(f'  {line}')
    
    print('✓ Metadata check passed')
"

echo -e "\n=== Build test complete ==="