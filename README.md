# Car Crash Analysis Project

## Overview
This project analyzes vehicle crash data using Apache Spark to derive various insights about accidents, vehicles, and drivers. The analysis includes studying fatalities, vehicle types, crash factors, and geographic patterns.

## Table of Contents
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Configuration](#configuration)
- [Running the Analysis](#running-the-analysis)
- [Analysis Details](#analysis-details)
- [Logging](#logging)
- [Testing](#testing)

## Requirements
- Python 3.8+ (I've used 3.12.7)
- Apache Spark 3.0+ (I've used 3.5.4)
- PyYAML

```bash
pip install -r requirements.txt
```

## Project Structure
```
car_crash_analysis/
│
├── README.md               # Project documentation
├── requirements.txt        # Python dependencies
├── config/
│   ├── config.yaml        # Application configuration
│   └── logging_config.yaml # Logging configuration
│
├── logs/                   # Log files directory
│   |── .gitkeep
│   └── car_crash_analysis.log
│
├── src/
│   ├── __init__.py
│   ├── main.py            # Main execution script
│   ├── analysis/          # Analysis modules
│   │   ├── __init__.py
│   │   └── crash_analyzer.py
│   └── utils/             # Utility modules
│       ├── __init__.py
│       ├── spark_helper.py
│       ├── result_writer.py
│       └── logger.py
│
└── output/
    └── analysis/          # Results for each analysis
       └── results.yaml
    

```

## Setup
1. Clone the repository or unzip the car_crash_analysis folder locally:
```bash
git clone [repository-url]
cd car_crash_analysis
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv  # Or python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure the input and output paths in `config/config.yaml`

## Configuration
### Input Data
Update `config/config.yaml` with the appropriate paths:
```yaml
input_paths:
  charges: "path/to/charges.csv" #Currently it is in Data Folder
  damages: "path/to/damages.csv"
  endorse: "path/to/endorse.csv"
  primary_person: "path/to/primary_person.csv"
  restrict: "path/to/restrict.csv"
  units: "path/to/units.csv"

output_path: "path/to/output/" #Currently results are stored in output folder
```

### Logging
Logging configuration can be modified in `config/logging_config.yaml`. By default, logs are:
- Written to `logs/car_crash_analysis.log`
- LOGS FOR NEW EXECTION GET APPENDED AT THE END 
- Rotated when size exceeds 10MB
- Includes both console and file output
- Console: INFO level
- File: INFO level

## Running the Analysis
Execute the analysis from the project root directory:
```bash
spark-submit src/main.py
```

## Analysis Details
The project performs the following analyses:

1. **Male Fatalities Analysis**
   - Counts crashes where more than 2 males were killed

2. **Two Wheeler Crashes**
   - Counts crashes involving motorcycles

3. **Fatal Crashes Analysis**
   - Top 5 vehicle makes in crashes with fatalities and non-deployed airbags

4. **Hit and Run Analysis**
   - Counts vehicles in hit-and-run crashes with valid licenses

5. **State-wise Accident Analysis**
   - Identifies state with highest number of accidents without female involvement

6. **Vehicle Makes and Injuries**
   - Identifies 3rd to 5th vehicle makes contributing to most injuries

7. **Ethnic Group Analysis**
   - Maps top ethnic user groups for each vehicle body style

8. **Alcohol-Related Crashes**
   - Top 5 ZIP codes with highest alcohol as contributing factor

9. **Damage Analysis**
   - Counts crashes with no damage but damage scale > 4

10. **Speed-Related Offenses**
    - Top 5 vehicle makes involved in speeding offenses with specific criteria


## Error Handling
The application includes comprehensive error handling:
- Input file validation
- Data processing error handling
- Logging of all errors with stack traces
- Graceful error reporting

