# VHS documentation
#
# Output:
#   Output <path>.gif               Create a GIF output at the given <path>
#   Output <path>.mp4               Create an MP4 output at the given <path>
#   Output <path>.webm              Create a WebM output at the given <path>
#
# Settings:
#   Set FontSize <number>           Set the font size of the terminal
#   Set FontFamily <string>         Set the font family of the terminal
#   Set Height <number>             Set the height of the terminal
#   Set Width <number>              Set the width of the terminal
#   Set LetterSpacing <float>       Set the font letter spacing (tracking)
#   Set LineHeight <float>          Set the font line height
#   Set Theme <json|string>         Set the theme of the terminal
#   Set Padding <number>            Set the padding of the terminal
#   Set Framerate <number>          Set the framerate of the recording
#   Set PlaybackSpeed <float>       Set the playback speed of the recording
#
# Sleep:
#   Sleep <time>                    Sleep for a set amount of <time> in seconds
#
# Type:
#   Type "string"                   Type a string into the terminal
#   Type@<time> "string"            Type a string with a set delay between each character
#
# Keys:
#   Backspace [number]              Press the Backspace key
#   Down [number]                   Press the Down key
#   Enter [number]                  Press the Enter key
#   Space [number]                  Press the Space key
#   Tab [number]                    Press the Tab key
#   Left [number]                   Press the Left Arrow key
#   Right [number]                  Press the Right Arrow key
#   Up [number]                     Press the Up Arrow key
#   PageUp [number]                 Press the Page Up key
#   PageDown [number]               Press the Page Down key
#   Ctrl+<key>                      Press the Control key + <key> (e.g. Ctrl+C)
#
# Display:
#   Hide                            Hide the subsequent commands from the output
#   Show                            Show the subsequent commands in the output

Output playground.gif

Set FontSize 18
Set Width 1200
Set Height 800
Set Padding 20
Set Theme "Catppuccin Macchiato"

# This command starts the docker container.
# We hide it initially to skip startup noise if desired,
# but showing it is good for transparency.
# We'll speed up the startup significantly.
Type "docker compose run greenmask"
Enter
Sleep 3s

# Clear screen to start fresh inside the container
Type "clear"
Enter

# 1. Inspect original data
Type "# Step 1: Inspect the original data (birthdate column)"
Enter
Type "psql_o -c 'SELECT businessentityid, jobtitle, birthdate FROM humanresources.employee LIMIT 5;'"
Enter
Sleep 1s

# 2. View configuration
Type "# Step 2: Check the configuration file (applying NoiseDate transformation)"
Enter
Type "cat config.yml | sed -n 23,32p"
Enter
Sleep 3s
Type "clear"
Enter

# 3.Validate data change before dumping
Type "# Step 3: Validate the transformation logic without dumping"
Enter
Type "greenmask --config config.yml validate \"
Enter
Type "  --warnings --data \"
Enter
Type "  --diff --transformed-only \"
Enter
Type "  --rows-limit 2"
Enter
Sleep 1s

# 3. Dump the database
Type "# Step 4: Run the dump command to generate masked data"
Sleep 1s
Enter
Type "greenmask --config config.yml dump"
Sleep 1s
Enter
# Wait for dump to complete
Sleep 1s

# 4. List dumps
Type "# Step 5: Check the list of dumps to confirm success"
Sleep 1s
Enter
Type "greenmask --config config.yml list-dumps"
Sleep 1s
Enter
Sleep 1s

# 5. Restore the database
Type "# Step 6: Restore the masked data into a new database"
Sleep 1s
Enter
Type "greenmask --config config.yml restore latest"
Sleep 1s
Enter

# 6. Verify the result
Type "# Step 7: Verify the results in the target database"
Sleep 1s
Enter
Type "psql_t -c 'SELECT businessentityid, jobtitle, birthdate FROM humanresources.employee LIMIT 5;'"
Sleep 1s
Enter
Sleep 1s

# 7. Verify Determinism
Type "# Step 8: Transformation is deterministic - validation yields same results"
Sleep 1s
Enter
Type "greenmask --config config.yml validate \"
Enter
Type "  --warnings --data \"
Enter
Type "  --diff --transformed-only \"
Enter
Type "  --rows-limit 2"
Enter
Sleep 3s

# Exit the container
Type "exit"
Sleep 1s
Enter
