# end-to-end-mlops-pipeline

This repository contains an example end-to-end MLOps pipeline orchestrated with Apache Airflow. The README below follows the steps demonstrated in the referenced tutorial video and shows how to prepare, containerize, and schedule a machine learning pipeline using Airflow.

Overview
-
The pipeline demonstrates a typical ML workflow implemented as Airflow tasks (DAG): data preparation, train/test split, model training, prediction on test data, metric calculation and saving outputs.

High-level steps (from the tutorial)
-
1. Define Python functions for every pipeline stage (data cleaning, feature engineering, train/test split, model training, prediction, metrics, etc.).
2. Initialize an Airflow DAG and define DAG-level configuration (schedule, default_args, start_date, retries).
3. Bind the Python functions to the DAG using PythonOperator (or TaskFlow API) so each function runs as its task.
4. Define the task sequence / dependencies in the order they should execute (set_upstream/set_downstream or using >>/<< operators).
5. Modify docker-compose.yml (Airflow services) to add any ML-specific system dependencies and volumes needed by the pipeline.
6. Create a Dockerfile for the custom environment (install ML libraries, copy code & DAGs).
7. Create requirements.txt listing ML dependencies (scikit-learn, pandas, numpy, joblib, etc.).
8. Initialize Airflow (db init), start webserver & scheduler, and visualize the DAG & task outputs in the Airflow UI.

Repository structure (example)
-
- dags/                 # Airflow DAG definitions (bind Python functions here)
- src/                  # Python modules with pipeline functions (data prep, train, predict, metrics)
- Dockerfile            # Build custom image with ML dependencies and project code
- docker-compose.yml    # Airflow composition (webserver, scheduler, db, redis/flower if used)
- requirements.txt      # Python dependencies for ML & Airflow extras
- readme.md             # This file

Quick start (local, Docker + Airflow)
-
Prerequisites:
- Docker & Docker Compose installed
- (Optional) git to clone the repo

1) Edit requirements.txt
- Add ML libraries your pipeline needs, e.g.:
  pandas
  numpy
  scikit-learn
  joblib
  matplotlib

2) Update Dockerfile
- Ensure the Dockerfile installs requirements.txt and copies project code and DAGs into the image. The Airflow scheduler & webserver should use this custom image so tasks run with ML dependencies.

3) Update docker-compose.yml
- Point the Airflow services (scheduler, webserver, worker if used) to the custom image or build from the local Dockerfile.
- Mount DAGs directory into the Airflow container (commonly /opt/airflow/dags).
- Add any OS-level packages required by the ML libraries (e.g., system libs for pandas/scipy). Example under service: build: context: . ; environment: ... ; volumes: ['dags/:/opt/airflow/dags']

4) Initialize and start Airflow
- From repo root (where docker-compose.yml lives):
  docker-compose up --build -d

- Initialize Airflow metadata DB (if not handled in compose):
  docker-compose exec webserver airflow db init
  docker-compose exec webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com

- Start scheduler & webserver (if not already running via compose):
  docker-compose up -d scheduler webserver

5) Validate DAG & Trigger
- Open Airflow UI: http://localhost:8080
- Ensure the custom DAG appears in the DAG list. If not, check container logs and DAG folder mount.
- Turn the DAG on, and either wait for its schedule or trigger it manually to run.

6) Inspect task logs & outputs
- In the Airflow UI, click on a task and view logs to see outputs from bound Python functions.
- Tasks should store artifacts (models, metrics, plots) to mounted volumes or object storage configured for the project.

Implementation notes
-
- Use PythonOperator or TaskFlow API to bind functions to tasks. Example:
  from airflow.operators.python_operator import PythonOperator
  def prepare_data(**kwargs):
      # read raw csv, clean, save processed file
      ...
  prepare_task = PythonOperator(task_id='prepare_data', python_callable=prepare_data, dag=dag)

- Use XComs or shared storage if tasks need to pass large artifacts (prefer shared filesystem like a mounted volume or object store for model files).
- Keep long-running training tasks on workers with sufficient RAM/CPU; use KubernetesExecutor or CeleryExecutor for distributed workers if needed.

Useful commands
-
- Build and start services: docker-compose up --build -d
- View logs for a service: docker-compose logs -f webserver
- Enter a running container: docker-compose exec webserver bash
- Initialize airflow DB: docker-compose exec webserver airflow db init
- Create admin user: docker-compose exec webserver airflow users create --username admin --role Admin --email admin@example.com --firstname Admin --lastname User

References
-
- Tutorial video used for these steps: "How to Build and schedule Machine Learning Pipeline using Airflow" by Ashutosh Tripathi (YouTube)
- Apache Airflow docs: https://airflow.apache.org/docs/

Contributing
-
Contributions, fixes, and improvements are welcome. Open an issue or submit a pull request with changes.

License
-
This project is provided as-is for learning and experimentation.

If anything in this README should be tailored to the exact code layout in this repository (DAG names, Python module names, or custom commands), share those details and the README will be adjusted accordingly.
