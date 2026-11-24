import json
import os
from datetime import timedelta
from pendulum import datetime
from airflow.sdk import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.datasets import Dataset_cocktail

CONN_ID = "cocktail_db"


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[Dataset_cocktail],
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["team_a", "loader", "etl"],
    description="Loads cocktail data from JSON dataset into PostgreSQL",
)
def cocktail_loader():

    @task()
    def create_table():
        """Create the cocktails table if it does not already exist."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS cocktails (
                id SERIAL PRIMARY KEY,
                drink_id VARCHAR(20) UNIQUE,
                name VARCHAR(255),
                category VARCHAR(100),
                alcoholic VARCHAR(50),
                glass VARCHAR(100),
                instructions TEXT,
                thumbnail VARCHAR(500),
                ingredients TEXT,
                measures TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        print("Table 'cocktails' verified / created.")

    @task()
    def read_cocktail_json() -> dict:
        """Read and parse the cocktail JSON file produced by the extractor."""
        path = Dataset_cocktail.uri
        if not os.path.exists(path):
            raise AirflowFailException(f"Cocktail JSON not found at {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        drinks = data.get("drinks", [])
        if not drinks:
            raise AirflowFailException(f"No drink entries found in JSON file at {path}")

        drink = drinks[0]

        ingredients = [
            drink.get(f"strIngredient{i}").strip()
            for i in range(1, 16)
            if drink.get(f"strIngredient{i}")
        ]
        measures = [
            drink.get(f"strMeasure{i}").strip()
            for i in range(1, 16)
            if drink.get(f"strMeasure{i}")
        ]

        return {
            "drink_id": drink.get("idDrink"),
            "name": drink.get("strDrink"),
            "category": drink.get("strCategory"),
            "alcoholic": drink.get("strAlcoholic"),
            "glass": drink.get("strGlass"),
            "instructions": drink.get("strInstructions"),
            "thumbnail": drink.get("strDrinkThumb"),
            "ingredients": json.dumps(ingredients),
            "measures": json.dumps(measures),
        }

    @task()
    def store_to_database(drink_data: dict):
        """Insert or update the cocktail record in PostgreSQL."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run(
            """
            INSERT INTO cocktails (
                drink_id, name, category, alcoholic, glass,
                instructions, thumbnail, ingredients, measures
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (drink_id) DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                alcoholic = EXCLUDED.alcoholic,
                glass = EXCLUDED.glass,
                instructions = EXCLUDED.instructions,
                thumbnail = EXCLUDED.thumbnail,
                ingredients = EXCLUDED.ingredients,
                measures = EXCLUDED.measures,
                loaded_at = CURRENT_TIMESTAMP;
            """,
            parameters=(
                drink_data["drink_id"],
                drink_data["name"],
                drink_data["category"],
                drink_data["alcoholic"],
                drink_data["glass"],
                drink_data["instructions"],
                drink_data["thumbnail"],
                drink_data["ingredients"],
                drink_data["measures"],
            ),
        )
        print(f"Stored cocktail '{drink_data['name']}' (ID: {drink_data['drink_id']}) to database.")

    @task()
    def verify_storage(drink_data: dict):
        """Verify the record was stored successfully in PostgreSQL."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        records = hook.get_records(
            "SELECT drink_id, name, category, alcoholic FROM cocktails WHERE drink_id = %s",
            parameters=(drink_data["drink_id"],),
        )

        if not records:
            raise AirflowFailException(f"Record {drink_data['drink_id']} not found after insert!")

        print(f"Verified stored record: {records[0]}")
        count = hook.get_records("SELECT COUNT(*) FROM cocktails")
        print(f"Total cocktails currently in database: {count[0][0]}")

    # Orchestrate pipeline flow
    table_ready = create_table()
    drink = read_cocktail_json()
    stored = store_to_database(drink)
    verified = verify_storage(drink)

    table_ready >> drink >> stored >> verified


cocktail_loader()
