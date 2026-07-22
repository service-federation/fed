Output docs/fed-demo.gif

Require fed
Require git
Require docker
Require sed

Set Shell bash
Set Width 1100
Set Height 720
Set FontSize 18
Set LineHeight 1.2
Set Padding 18
Set Framerate 15
Set TypingSpeed 12ms
Set CursorBlink false
Set Theme "Catppuccin Mocha"

# Begin in the demo directory with a short, path-free prompt. The final clear
# removes every setup command from the terminal buffer before recording starts.
Hide
Sleep 1s
Type `PS1='> '`
Enter
Type "cd docs/demo"
Enter
Type "clear; sed -n '1,24p' fed.yaml"
Enter
Sleep 500ms
Show

# Beat 1: the config is the star. Both sed commands execute while hidden;
# recording resumes only after their output is already on screen.
Sleep 3s
Hide
Type "clear; sed -n '25,50p' fed.yaml"
Enter
Sleep 500ms
Show
Sleep 3s

# Beat 2: containers, migration, native process, and health gates.
Type "clear"
Enter
Type "fed start"
Enter
Wait+Screen /All services started successfully!/
Sleep 5s

# Beat 3: create another checkout, isolate it, and run both stacks together.
Type "clear"
Enter
Type "git worktree add -b demo/isolation ../../../fed-demo-isolated"
Enter
Wait+Screen /HEAD is now at/
Sleep 2s
Type "clear"
Enter

# Until this demo is committed, copy the working-tree version into the new checkout.
# Once committed this remains a harmless, hidden refresh of the same demo files.
Hide
Type "mkdir -p ../../../fed-demo-isolated/docs/demo/scripts && cp fed.yaml app.py README.md ../../../fed-demo-isolated/docs/demo/ && cp scripts/migrate.sh ../../../fed-demo-isolated/docs/demo/scripts/"
Enter
Type "cd ../../../fed-demo-isolated/docs/demo"
Enter
Type "clear"
Enter
Show

Sleep 1s
Type "fed isolate enable"
Enter
Wait+Screen /Services will use unique container names and these ports./
Sleep 4s

Type "clear"
Enter
Type "fed start"
Enter
Wait+Screen /All services started successfully!/
Sleep 5s

# Close on the isolated stack status and both database containers on distinct ports.
Type "clear"
Enter
Type "fed status"
Enter
Sleep 2s
Type `docker ps -f name=database --format 'table {{.Names}}\t{{.Ports}}'`
Enter
Wait+Screen /fed-.*-database/
Sleep 6s
