please create a python script that move files in folders:

for image files, using the 'Date Taken' attribute of the file

using the 'Date Taken', create folder hierarchy in the working directory for \ year \ month in order to partition/group files

and move the file the year-month folder the image was taken, don't copy.

If year-month folder already exist, no need to recreate it

if no Date Taken value exists for a specific year-month combination there is no need to create it.

for video files, using the 'Media Created' attribute

do same as image

else if no date attribute value exists in that file, save to separate folder 'to sort folder' and do the same.

for files in 'to sort folder', use the filename and try to use that to create a timestamp 

where timestamp can only be the past

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

create the requirements.txt for this

