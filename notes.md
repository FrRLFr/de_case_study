create git repository
install requirements with python venv

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip --version
python3 -m pip install -r requirements.txt

dbt init xeneta

select duckdb


dbt run-operation ensure_table 

dbt seed 

dbt run

dbt test

-> some test fail, even though edscription says, that they are unique