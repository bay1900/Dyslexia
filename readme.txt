
# CREATE VENV
python3 -m venv dyslexia_env

# ACTIVATE
source dyslexia_env/bin/activate

# DEACTIVATE
deactivate

# CREATE REQUIUREMENT FILE
pip freeze > requirements.txt


### NOTE ###

Update pip after install pandas
pip install --upgrade pip


pip install -r requirements.txt

