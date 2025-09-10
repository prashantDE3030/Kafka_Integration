def update_env_file(key,value):
    # Read existing lines
    try:
        with open(".env", "r") as file:
            lines = file.readlines()
    except FileNotFoundError:
        lines = []

    # Track if key exists
    key_exists = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            key_exists = True
            break

    # If key not found, append it
    if not key_exists:
        lines.append(f"\n{key}={value}\n")

    # Write back to file
    with open(".env", "w") as file:
        file.writelines(lines)