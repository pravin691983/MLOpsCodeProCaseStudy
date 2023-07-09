# MLOps CodePro Case Study

> In this assignment, we will focus on reproducibility, automation and a few aspects of collaboration of MLOps.

## Table of Contents

- [Overview Business Understanding](#overview-business-understanding)
- [Problem Statement Business Objectives](#problem-statement-business-objectives)
- [Data in depth](#data-in-depth)
- [Approach](#approach)
- [Technologies Used](#technologies-used)

<!-- You can include any other section that is pertinent to your problem -->

## Overview Business Understanding

CodePro is an EdTech startup that had a phenomenal seed A funding round.
It used the money to increase its brand awareness. As the marketing spend increased, it got several leads from different sources. Although it had spent significant money on acquiring customers, it had to be profitable in the long run to sustain the business.
The major cost that the company is incurring is the customer acquisition cost (CAC). Now, we will discuss what customer acquisition means.

At the initial stage, customer acquisition cost is required to be high in companies. But as their businesses grow, these companies start focussing on profitability. Many companies first offer their services for free or provide offers at the initial stages but later start charging customers for these services. For example, Google Pay used to provide many offers, and Reliance Jio in India offered free mobile data services for over a year. Once these brands were established and brand awareness was generated, these businesses started growing organically. At this point, they began charging customers.

## Problem Statement Business Objectives

Businesses want to reduce their customer acquisition costs in the long run. Also CodePro had to reduce its customer acquisition cost to increase its profitability.

### Want to

- Build a system that categorises leads based on the likelihood of their purchasing CodePro’s course.
- Build end-to-end MLOps pipeline

## Data in depth

To classify any lead, we will require two types of information about it:

- Origin of the lead: To better predict the likelihood of a lead purchasing a course, we need some information on the origin of the lead. The columns from ‘city_mapped’ to ‘reffered_leads’ contain the information about the origin of the lead.
- Interaction of the lead with the website/platform: We also require information about how the lead interacted with the platform. The columns from ‘1_on_1_industry_mentorship’ to ‘whatsapp_chat_click’ store the information about a lead’s interaction with the platform.

## Approach

#### Understanding the Dataset

- Defining the path for train and test images

#### Dataset Creation

- Create train & validation dataset from the train directory

#### Dataset visualisation

- Create a code to visualize dataset to perform EDA

#### Model Building & training

- Creating three different pipelines for our use case.

  - Data Pipeline
    - To fetch data from source and preprocess it for training and inference pipeline
  - Training Pipeline
    - In case of data drift under a threshold, retrain the model with new preprocessed data
  - Inference Pipeline
    - To predict the target variable for new data

<!-- You don't have to answer all the questions - just the ones relevant to your project. -->

#### Command to run MLFlow & Airflow server

- mlflow server --backend-store-uri='sqlite:////home/database/Lead_scoring_mlflow_production.db' --default-artifact-root="/home/airflow/mlruns/" --port=6006 --host=0.0.0.0
- airflow db init
- airflow users create --username upgrad --firstname Pravin --lastname Tawade --role Admin --email spiderman@superhero.org --password admin
- airflow webserver
- airflow scheduler

<!-- You don't have to answer all the questions - just the ones relevant to your project. -->

## Technologies Used

- Python
- Numpy
- Panda
- Matplotlib
- Seaborn
- Augmentor
- Tensor
- Keras
- Jupyter Notebook
- Jarvislab.ai
- MLflow
- Airflow
- pycaret
- Pandas Profiling

<!-- As the libraries versions keep on changing, it is recommended to mention the version of library used in this project -->

## Contact

Created by [@pravin691983] - feel free to contact me!

<!-- Optional -->
<!-- ## License -->
<!-- This project is open source and available under the [... License](). -->

<!-- You don't have to include all sections - just the one's relevant to your project -->
