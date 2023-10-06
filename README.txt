Project has to be imported in eclipse with everything in this root folder
 Common Source has also been moved into src
 
 No environment variables are needed as /libs has both thrid party directory and simba directory 
  Simba directory has the base jars needed from Simba DSII project.

Adding all jars as placeholders, during setup, all of them to be updated in referenced paths and xml files.

TODO: 
 [High Prio - Harman/Akash] Move conf file for BofA and Morgan into the config directory 
    Name the files as ResterJDBC.conf.bofa
                   and ResterJDBC.conf.ms
 [Hig Prio Andy] Keep separate builds for Morgan and BofA - this will move the apprporiate config file to 
    the output folder as a last step  
 [Low Prio Harman]  Fix 5 warnings that come during compilation
   
NOTE: For Sharing source code - Akash
  Clean up all subfoders under bin and the jar under output
  This gives us only the files (including jars) needed for rebuilding inside BofA

  
