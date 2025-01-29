import ingestion
import transform
import data_analysis 
import atlanta_united_metrics

def main_pipeline():
    ingestion.main()
    transform.main()
    data_analysis.main()
    atlanta_united_metrics.main()
    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main_pipeline()