# Simple PySpark example

This small example demonstrates a minimal PySpark program (word count using DataFrame API) that runs locally.

Files created
- `simple_spark.py` — the PySpark example script (reads `sample_data.txt`).
- `sample_data.txt` — small example input.
- `requirements.txt` — Python dependency listing (`pyspark`).

Prerequisites
- Java JDK (8, 11, or 17) installed and JAVA_HOME set.
- Python 3.8+ (we used 3.10+ in testing suggestions).

Install dependencies (PowerShell)
```
python -m pip install -r requirements.txt
```

Run the example (PowerShell)
```
python simple_spark.py
```

What it does
- Reads `sample_data.txt` (one sentence per line).
- Splits lines into words, normalizes to lower-case, and counts occurrences.
- Prints word counts to the console.

Notes
- Running Spark on Windows requires a compatible Java installation. If you see JVM errors, ensure `java -version` works in PowerShell and JAVA_HOME points to a JDK installation.
- If you want to run on a Spark cluster, remove `.master("local[*]")` and configure the cluster settings.
