"""courtesy of gpt 4"""

def parse_requirements(file_path):
    with open(file_path, 'r') as f:
        # Extract package names, ignore versions or specifiers after '==', '>=', etc.
        # Also, strip whitespace and ignore lines that are comments.
        return {line.split('==')[0].split('>=')[0].split('<=')[0].split('!=')[0].split('~=')[0].strip()
                for line in f if not line.startswith('#') and line.strip()}

def main():
    # Paths to the two requirements.txt files
    file1 = 'requirements1.txt'
    file2 = 'requirements2.txt'

    packages_file1 = parse_requirements(file1)
    packages_file2 = parse_requirements(file2)

    only_in_file1 = packages_file1 - packages_file2
    only_in_file2 = packages_file2 - packages_file1

    if only_in_file1:
        print(f"Packages only in {file1}:")
        for pkg in sorted(only_in_file1):
            print(f"  {pkg}")
    else:
        print(f"All packages in {file1} are also in {file2}.")

    if only_in_file2:
        print(f"\nPackages only in {file2}:")
        for pkg in sorted(only_in_file2):
            print(f"  {pkg}")
    else:
        print(f"\nAll packages in {file2} are also in {file1}.")

if __name__ == "__main__":
    main()
