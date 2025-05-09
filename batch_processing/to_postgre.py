import pandas as pd
from sqlalchemy import create_engine

def main():
    # Read the processed output
    df = pd.read_csv("C:\\Users\\Dell\\OneDrive - National Economics University\\code\\MLOps\\final_data\\diabetes_1.csv")

    # Create SQLAlchemy engine pointing at Dockerized Postgres
    engine = create_engine(
        "postgresql+psycopg2://user:password@localhost:5432/diabetes"
    )

    # Write DataFrame into Postgres
    df.to_sql(
        name="diabetes_clean",
        con=engine,
        if_exists="replace",
        index=False
    )
    print("Data loaded into Postgres successfully.")

if __name__ == "__main__":
    main()
