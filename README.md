# Generic Flask Website

## Dependencies
* python (3.5+)
* flask
* Some WebServer (Apache, waitress)
* ffmpeg

### Optional Dependencies
* waitress (python based web server)
* boto3 (connection to Amazon S3)

## Installing the Site
Installing dependencies

`python3 -m pip install flask`

## Setup and Running
It is recommended that these environment variables are put in your .bashrc
```sh
export FLASK_APP=site
export FLASK_ENV=production

# MySQL setup
export SQLALCHEMY_DATABASE_URI="mysql+mysqlconnector://<DB_USERNAME>:<DB_PASSWORD>@<DB_HOSTNAME>/<DB_NAME>"

##export SQLALCHEMY_DATABASE_URI="mysql+mysqlconnector://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOSTNAME}/${DB_NAME}?charset=utf8mb4_ai_ci&collation=utf8mb4_general_ci"
# ALTER TABLE clan_description_history MODIFY description VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
# ALTER TABLE player MODIFY name VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
#ALTER TABLE player_name_history MODIFY name VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
# ALTER TABLE clan_name_history MODIFY name VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
# Sqlite3 setup
export SQLALCHEMY_DATABASE_URI="sqlite:////`pwd`/instance/coc.sqlite"

python3 -m flask init-db (one time only)


### If using pythonanywhere (see setting environment variables for your app)
# https://help.pythonanywhere.com/pages/environment-variables-for-web-apps/
```


You must place a config.py configuration file within the instance directory. This has options for where the data is stored.

Example: instance/config.py
```python
D = "/path/to/directory"
DOWNLOAD_DIR = f'{D}/raw_audio_files'
LEFT_AUDIO_DIR = f'{D}/0'
RIGHT_AUDIO_DIR = f'{D}/1'

S3_BUCKET = 'voice-recordings'
S3_BUCKET_DIRS = ["prod/dir/11", "prod/dir/22"]

SHOW_SPEAKER_OPTIONS = True
```


### Starting the development server
```sh
python3 -m flask run
```


### Starting using waitress
From the site directory
```sh
waitress-serve --listen=*:8083 --call 'gsite:create_app'
```

# coc_site
