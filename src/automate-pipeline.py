from scripts.ingest import ingest_data
from scripts.transform import transform_data

def main_pipeline():
    ingest_data()
    transform_data()
    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main_pipeline()