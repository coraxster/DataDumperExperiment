param([String]$file="C:\tmp\data\1in/01.jpg") #Must be the first statement in your script

#Open the file in read only mode, without sharing (I.e., locked as requested)
$f = [System.io.File]::Open($file, 'Open', 'Read', 'None')

#Wait in the above (file locked) state until the user presses a key
Write-Host "Press any key to unlock ..."
$null = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

#Close the file (This releases the current handle and unlocks the file)
$f.Close()