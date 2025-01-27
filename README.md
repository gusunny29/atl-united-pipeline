# Atlanta United Database Pipeline

## Overview

I am tasked with developing an automated data pipeline that ingests data from the American soccer analysis app (https://app.americansocceranalysis.com/#!/mls)
where I need to:

- Create tables:
  - Match Data
  - Salary Analysis: Aggregate team level salaries for the 2024 MLS season
  - Player Stats: Individual playuer performance metrics like expected goals, expected assists and goals added
- Automated ingestion and transformation process
- Data Visualizations to depicts relationships as examples of future analysis
- Discuss challenges, assumptions and deployment instructions

## Database Shell Selection

I've chosen to use PostgreSQL as my database of choice, I've used PostgreSQL in the past and it seems to work well with the task at hand. I decided to use a local databse to conduct this project

## Set up

1. Clone the repository
2. Install the dependencies: `pip3 install -r requirements.txt`
3. Run the pipeline: `python scripts/automate_pipeline.py

## Ingestion and Cleaning
