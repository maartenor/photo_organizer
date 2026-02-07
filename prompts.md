please create a python script that move files in folders:

for image files, using the 'Date Taken' attribute of the file

using the 'Date Taken', create folder hierarchy in the working directory for \ year \ month in order to partition/group files

and move the file the year-month folder the image was taken, don't copy.

If year-month folder already exist, no need to recreate it

if no Date Taken value exists for a specific year-month combination there is no need to create it.

for video files, using the 'Media Created' attribute

do same as image

Step 1 (EXIF): Data taken. 
If this fails - no EXIF data
Step 2 (File stats): Uses min(File.Data_Creation, File.Date_Modified) = 2016-04-21 → Returns ("2016", "04")
If even that fails
Step 3 (file name)

else if no date attribute value exists in that file, save to separate folder 'to sort folder' and do the same.

for files in 'to sort folder', use the filename and try to use that to create a timestamp 

where timestamp can only be the past but not be earlier than year 1980

move that file based on filename extracted timestamp to correct year-month folder

in addition to log the same as regular processing and also add a warning to database and use 'issue_description' to save the timestamp value there

else keep file in 'to sort folder'

if a file is encountered that is nor image nor video file type or unable to read attributes from, move them in folder 'unprocessable' and raise warning.

image, video and other files are not allowed to get lost



a sqlite or other light weight in code database is instantiated, where the move operations are registered.

so a database table exists 'process_log' with columns 'filename | target_folder |  processing_timestamp_utc' to store processing values

and a table for 'filename | warning_code | error_code | issue_description | processing_timestamp_utc' 

where warning_code and issue_description are filled on when warnings are raised in code

and error_code and issue_description for rasied errors

add duplicate handling to the code

create the requirements.txt for this



Create kubernetes setup yaml files and additional py script files, 

so a user can upload image and video files that need sorting to a local network folder

and select the 'network storage target folder' under '\\192.168.1.2\pictures\' (default) for example '\\192.168.1.2\pictures\myself' or '\\192.168.1.2\pictures\otherfolks'

The 'network storage target folder' values should be defined in a separate yaml file, and check on valid (network) paths requirements by a script. 

the 'network storage target folder' and uploading of files should be able to be done by user from a smartphone or tablet

Once a user uploads new files in that folder, it should trigger a kubernetes worker pod to be started

which performs the file organizing py script above

This setup should not be able to remove files nor delete/destroy the network storage target files nor directories

ensure the file_organizer.db process database and stdout are stored in a persistent volume