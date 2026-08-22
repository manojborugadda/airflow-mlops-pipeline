"""
Hello, Airflow!
This is a simple DAG that demonstrates the basic structure of an Airflow DAG.

We will understand the following concepts:

- What is a DAG?
- python operator --> running python functions
- bash operator --> running bash commands
- Task dependencies with the >> operator and << operator
- xcom --> lightweight data passing between tasks

commands for running this DAG:
airflow dag trigger -d 01_hello_airflow
airflow tasks test
"""

from datetime import datetime, timedelta

from sklearn import metrics
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split as sk_train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score,recall_score,log_loss, f1_score, confusion_matrix



def prepare_data():
    import pandas as pd
    print("Preparing data...")
    url = "https://raw.githubusercontent.com/TripathiAshutosh/dataset/main/iris.csv"
    # Try reading with headers first; if the expected target column isn't present, fall back to header=None
    df = pd.read_csv(url, header=0)
    if 'class' not in df.columns:
        df = pd.read_csv(url, header=None)
        # ensure the last column is named 'class' (common for Iris-style CSVs)
        df = df.rename(columns={df.columns[-1]: 'class'})
    df = df.dropna()
    df.to_csv("final_df.csv", index=False)

    
def split_train_test_data():
    print("Splitting data into train and test sets...")
    final_data = pd.read_csv("final_df.csv")
    target_column = 'class'
    if target_column not in final_data.columns:
        raise ValueError(f"Expected target column '{target_column}' not found in final_df.csv. Columns found: {list(final_data.columns)}")
    # X as DataFrame, y as Series (1d) for sklearn
    X = final_data.drop(columns=[target_column])
    y = final_data[target_column]
    X_train, X_test, y_train, y_test = sk_train_test_split(X, y, test_size=0.3, stratify=y, random_state=47)
    np.save(f'X_train.npy', X_train)
    np.save(f'X_test.npy', X_test)
    np.save(f'y_train.npy', y_train)
    np.save(f'y_test.npy', y_test)
    
    print("\n----X_train----")
    print("\n")
    print(X_train.head(5))
    
    print("\n----X_test----")
    print("\n")
    print(X_test.head(5))
    
    print("\n----y_train----")
    print("\n")
    print(y_train.head(5))
    
    print("\n----y_test----")
    print("\n")
    print(y_test.head(5))
    
    
def training_basic_classifier():
    print("Training a basic classifier...")
    X_train = np.load('X_train.npy', allow_pickle=True)
    y_train = np.load('y_train.npy', allow_pickle=True)
    
    classifier = LogisticRegression(max_iter=200)
    classifier.fit(X_train, y_train)
    
    import pickle
    with open('model.pkl', 'wb') as f:
        pickle.dump(classifier, f)
    print("\n logisitc regression classifier is trained  on IRIS dataset and saved as 'model.pkl'")
    
    
def predict_on_test_data():
    print("Predicting on test data...")
    X_test = np.load('X_test.npy', allow_pickle=True)
    y_test = np.load('y_test.npy', allow_pickle=True)
    
    import pickle
    with open('model.pkl', 'rb') as f:
        logistic_reg_model = pickle.load(f)
    
    X_test = np.load('X_test.npy', allow_pickle=True)
    y_pred = logistic_reg_model.predict(X_test)
    np.save(f'y_pred.npy', y_pred)

    print("\n----PREDICTED CLASSES---y_pred----")
    print("\n")
    print(y_pred)
    

def predic_prob_on_test_data():
    print("Predicting probabilities on test data...")
    import pickle
    with open('model.pkl', 'rb') as f:
        logistic_reg_model = pickle.load(f)
    
    X_test = np.load('X_test.npy', allow_pickle=True)
    y_pred_proba = logistic_reg_model.predict_proba(X_test)
    np.save(f'y_pred_proba.npy', y_pred_proba)

    print("\n----PREDICTED PROBABILITIES---y_pred_proba----")
    print("\n")
    print(y_pred_proba)
    
def get_metrics():
    print("Calculating metrics...")
    y_test = np.load('y_test.npy', allow_pickle=True)
    y_pred = np.load('y_pred.npy', allow_pickle=True)
    y_pred_proba = np.load('y_pred_proba.npy', allow_pickle=True)    
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='micro')
    recall = recall_score(y_test, y_pred, average='micro')
    entropy = log_loss(y_test, y_pred_proba)
    

    print("\n----METRICS----")
    print("\n")
    print(metrics.classification_report(y_test, y_pred))
    print("\n MODEL metrics are as follows:")
    print(f"Accuracy: {round(accuracy, 2)}")
    print(f"Precision: {round(precision, 2)}")
    print(f"Recall: {round(recall, 2)}")
    print(f"Entropy: {round(entropy, 2)}")
    
    
# defining the DAG

with DAG(
    dag_id="hello_airflow_ML_Pipeline",
    start_date=datetime(2024, 6, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    task_prepare_data = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
    )
    
    task_split_train_test_data = PythonOperator(
        task_id="train_test_split",
        python_callable=split_train_test_data,
    )
    
    task_training_basic_classifier = PythonOperator(
        task_id="training_basic_classifier",
        python_callable=training_basic_classifier,
    )
    
    task_predict_on_test_data = PythonOperator(
        task_id="predict_on_test_data",
        python_callable=predict_on_test_data,
    )
    
    task_predic_prob_on_test_data = PythonOperator(
        task_id="predic_prob_on_test_data",
        python_callable=predic_prob_on_test_data,
    )
    
    task_get_metrics = PythonOperator(
        task_id="get_metrics",
        python_callable=get_metrics,
    )
    
    # all the tasks will be executed in the order of their dependencies.
    # The first task will be executed first, and 
    # then the second task will be executed after the first task is completed, and so on.
    task_prepare_data >> task_split_train_test_data >> task_training_basic_classifier >> task_predict_on_test_data >> \
    task_predic_prob_on_test_data >> task_get_metrics