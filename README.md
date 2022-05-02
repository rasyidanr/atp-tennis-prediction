# MSIN0166 Data Engineering Group Project

The project consists of two parts, namely:
<br> Part 1: ETL Pipeline (as part of the group coursework)
<br> Part 2: Machine Learning Pipeline

# Architecture

![image](https://user-images.githubusercontent.com/97729083/166318933-baa6010c-e7d7-4ae6-ab4e-8dafcd7eb3cf.png)
<br>Figure 1: ETL Pipeline for Data Extraction and Storage

In Part 1 the ETL pipeline extracts data from three different sources, transform them into suitable format and the save it into a specific data storage. Three different sources and two different databases are used. Firstly, the ATP tour data (tennis player performance) and Bet365 (betting website) are scraped using Selenium and Chrome Driver which are then saved into a postgreSQL database. Secondly, the twitter stream data is extracted based on a rule mentioning the tournament name (Indian Wells), the timeframe and size limitation (e.g. top 50 male tennis players in that tournament), and saved into a MongoDB database.

![image](https://user-images.githubusercontent.com/97729083/166318998-a37af36d-add0-4c6f-949f-3f9eb15f71e8.png)
<br>Figure 2: Machine Learning Pipeline using AWS Sagemaker, Lambda Function and API Gateway

The machine learning pipeline starts by picking up the output from the previous process as explained. In this ML model scope, the focus is on the CSV files scraped from the ATP tour website, containing the information on player’s ranking, their attributes, and the betting data.
The chosen CSV output files are first saved into an Amazon S3 bucket named “atp-tennis-prediction” in AWS. Next, the full machine learning pipeline will be orchestrated through the Amazon Sagemaker Notebook with an interface to an output path in S3 bucket. The processes include Processing Job, Training Job, Amazon Sagemaker built-in XGBoost model, Model Evaluation and Sagemaker Endpoint. Note that we will deep dive into the Sagemaker Notebook in detail in Section 4.
After model deployment, we will pick up the model endpoints and pass it into an AWS lambda function to start a serverless architecture. AWS lambda will then pass the values through an API Gateway. Through API Gateway, we can obtain an invoke URL which can be used to test the model prediction in a user-interface. In this case we are using Postman to test the performance.


# Part 1: ETL Pipeline

Web scraping project of several websites:
- ATP [Association of Tennis Professionals] Tours website
- Bet365 website, specifically for current (same day and next day) tennis matches
- Twitter data - tweets about ATP

Using several tools:
- Docker containers
- PySpark
- PostgreSQL database
- MongoDB database
- Python programming language
- Selenium
- ChromeDriver and Google Chrome

## Clone the Project

1. Navigate to a desired path on your machine.
2. Run the below command using terminal:

```bash
git clone https://github.com/maldyvinandar/data-eng-group-coursework
```

## Usage

Run the docker containers:
1. Navigate to the cloned project folder.
2. Go to /group-project folder.
3. Copy files from [Google Drive](https://drive.google.com/drive/folders/18epE1RWgFrJ-LhO0jJZVdnwbcQ4a37La?usp=sharing) to /group-project/pyspark/src/. The .env file contains the API key to webscrape Twitter data.
4. When you see the Makefile and Dockerfile, run the below command to run all the necessary docker containers using terminal:
```bash
make all
```
5. To webscrape all listed websites, run:
```bash
make spark-submit
```
6. To reset (delete all table contents) the PostgreSQL database, run:
```bash
sh reset_postgres_database.sh
```
7. To copy the output files from docker container to your machine, run:
```bash
sudo docker cp -a spark:/opt/bitnami/spark/output_files/ ["target path in your machine"]

example:

sudo docker cp -a spark:/opt/bitnami/spark/output_files/ ~/Desktop/ATP_webscraping_files/
```


## Files

Location of main python file:
```
group-project/pyspark/src/main.py
```
Location of output files (CSV, JSON, Parquet) - inside the 'spark' docker container:
```
spark:/opt/bitnami/spark/output_files/
```

# Part 2: Machine Learning Pipeline

##  Data Preparation

We will begin the data preparation task by following below steps:
1.	Importing necessary libraries
2.	Creating S3 bucket (to save the models into AWS sagemaker)
3.	Mapping train and test data in S3
4.	Mapping the path of the models in S3
5.	Web-scrape the dataset and save it on S3
In this section, the key point is to create an S3 bucket automatically from the Sagemaker notebook without actually going into the S3 interface. Sample code of this automation is presented as below.
 ![image](https://user-images.githubusercontent.com/97729083/166319321-fbb5959f-2bb1-4304-96f8-a466f62829c7.png)
<br>
Figure 3: Sample Code to Create an S3 Bucket from Console
After completing the above steps, summary statistics and correlation table were produced to analyse the data types and handle null values.
 ![image](https://user-images.githubusercontent.com/97729083/166319336-4b88da80-ec45-47e2-a102-a335a209d853.png)
<br>
Figure 4: Correlation Heatmap of Numerical Variables
In addition, we will also deep dive into three variables with the highest correlation with the target dependant variable to see its skewness, namely the three variables representing the betting data from three different sites.
 ![image](https://user-images.githubusercontent.com/97729083/166319365-de102a29-aa90-4834-93ce-52be6a520361.png)
<br>
Figure 5: Outlier Detection for Top 3 Variables with the Highest Correlation with the Dependent Variable

The result suggests that all betting data are negatively skewed with a long tail towards high scores, which is expected because there should be only a small number of players that acquire a very high bets on tournament average.

## 	Exploratory Data Analysis

As the task is to predict the likelihood of a player to be categorized as top 10 world player based on their performance and betting data, we would like to see what the current distribution of top 10 tennis players in the world is. The below figure showcases the said distribution.
![image](https://user-images.githubusercontent.com/97729083/166319401-e57b645f-ccd6-4f85-bc09-2069d3678446.png)
<br>
Figure 6: Top 10 Tennis Players in the Indian Wells Tournament

##  Dataset Splitting and Saving into S3

Next, to build the machine learning model, we will split the dataset into two, namely the train and test set with a proportion of 70:30. In this case we disregard the need of using a validation set since the dataset size is too small to be split into three. Each of the train and test set will be saved into a csv format in the pre-defined S3 bucket as created before.
![image](https://user-images.githubusercontent.com/97729083/166319441-920ee475-7bd3-4483-8b6d-f5a0f3dfe6e2.png)
<br>
Figure 7: Sample Code to Save Train and Test Data into a Specified S3 Path

## 	Model Building & Training

With the objective to predict the likelihood of a player to be categorized as top 10 world rank tennis player based on their past performances and betting data, we will be using an built-in algorithm in Amazon Sagemaker, namely the built-in XGBoost algorithm. As the task is a binary logistic regression, we will be using a XGBoost classification algorithm, to be exact.

The built-in algorithm in Amazon Sagemaker is present in the form of container or image that needs to be pulled into our “local” instance that is currently run. To pull this, we use the library “get_image_uri”. Once we run this, we will get the XGBoost algorithm into our local instance.
![image](https://user-images.githubusercontent.com/97729083/166319478-6b59802b-60b4-40b3-afbb-eb4ac70374e3.png)
<br>
Figure 8: Using a Library to Pull the XGBoost Algorithm

The next step is to do hyperparameter tuning, by setting a binary logistic objective and several specific parameters unique to this model.

### Sagemaker Estimator

To call the XGBoost image, we will need to use the sagemaker estimator library, by specifying the parameters, such as the IAM role, train instance count, train instance type, train spot instances and the train run limits. The IAM role is important as we need to connect with the execution role which has permission to access the output path we specify in the S3 bucket. Furthermore, as we don’t need a parallel process,the train instance count is defined as “1”. One of the most important thing is the train use spot instances parameter, which is set as “True”. This is useful to limit the building time or limit the building hours allocated to run the model. 

Once all the parameters are specified, we can fit the model.

![image](https://user-images.githubusercontent.com/97729083/166319522-da93d5eb-d541-4027-a3a5-5a2747413c2e.png)
<br>
Figure 9: Constructing a Sagemaker Estimator that Calls the XGBoost Container

##  ML Model Deployment and Prediction

### Deployment

Once the machine learning model is created, we will be able to see a new model created inside the output folder as specified in S3. We can see that there are two models here because every time a model is run, it will be saved into a separate entity based on a timestamp, therefore model versioning is applicable in this case.

![image](https://user-images.githubusercontent.com/97729083/166319564-cfc852ee-2d81-4879-a4b0-d87fd913841f.png)
<br>
Figure 10: Models Saved into Pre-defined S3 Buckets

To deploy the machine learning model, we need to specify the initial instance count (1) and instance type (ml.m4.xlarge).

## Prediction

Before doing the prediction, we need to import a library called CSV serializer. This is because, before passing a dataset into an endpoint the dataset needs to be serialized for the model to process.
![image](https://user-images.githubusercontent.com/97729083/166319597-a306617f-0542-4946-9dad-5626a7b6acd1.png)
<br>
Figure 11: CSV Serializer

After being serialized, we also need to attach a decoding operator to convert the result into (utf-8) format, as the prediction output will be presented in an encoded format.

![image](https://user-images.githubusercontent.com/97729083/166319628-da542745-2933-4772-8891-bb0f82004288.png)
<br>
Figure 12: Predicting Test Data with Decoding Process

## Model Evaluation

To evaluate the performance of the XGBoost classification model, we will create a confusion matrix to show the precision and recall scores of the model. In this case, the overall F1 score of the XGBoost model is 63.9%.
![image](https://user-images.githubusercontent.com/97729083/166319667-e34b60f0-5b67-473f-aa3a-5120ee864a0f.png)
<br>
Figure 13: Confusion Matrix to Evaluate the Model Results

##  Deleting Endpoints

One of the most important things after running any model in Sagemaker is to delete the endpoints. This is to ensure that there are no inferences that are still running in the background. Below code will help to delete all endpoints, inferences and the buckets.
 ![image](https://user-images.githubusercontent.com/97729083/166319706-ab34a367-4309-48c8-938b-a78607303a87.png)
<br>
Figure 14: Delete Endpoints
 
# Serving with AWS Lambda, API Gateway and Postman

After deployment of an endpoint, we will call the model using a serverless architecture. The structure of the architecture is presented in the diagram below.
![image](https://user-images.githubusercontent.com/97729083/166319827-9ca00bc7-ce6b-4731-ad17-f66c31fdc532.png)
<br>
Figure 15: Serverless Architecture using AWS Lambda and API Gateway
Source:https://aws.amazon.com/blogs/machine-learning/call-an-amazon-sagemaker-model-endpoint-using-amazon-api-gateway-and-aws-lambda/

As explained in the AWS documentation (Amazon Web Services, 2022), from the perspective of the client side, a client script calls an Amazon API Gateway API action and passes parameter values to the Lambda function. API Gateway serves as a layer that provides the API and at the same time making sure that AWS Lambda stays in a protected environment (Amazon Web Services, 2022). Next, the Lambda function parses the value and passes it to the Sagemaker model endpoint. In return, the model will perform the prediction and give back the prediction result to lambda and it will in turn pass it to the API gateway and the client. Below is the workflow of building a serverless architecture, we will be using Postman for testing purposes.

### Building a Serverless Architecture

First, from the deployed model we pick up the model endpoints as stated. We will then create a new Lambda function and set the model endpoints in the environment variable.

 <img width="468" alt="image" src="https://user-images.githubusercontent.com/97729083/166319851-05c1fdc0-378d-4c06-bf96-58ec7b08bada.png">
<br>
Figure 16: Environment Variable in the AWS Lambda Function

Next, we define the lambda function required to pass the model into API gateway
![image](https://user-images.githubusercontent.com/97729083/166319872-87009b51-dd69-49a7-9d53-398c0910035b.png)
<br>
Figure 17: Lambda Function

Before passing into API gateway, it is possible to test the environment using the ‘configure test setting’ option. In below case, the testing was successful and the model is able to be called and return a prediction result. Here the prediction result shows that the test data presented is categorized as Top 10 World Rank Tennis Player.
![image](https://user-images.githubusercontent.com/97729083/166319889-f8a6757d-c420-4a94-90d6-68c13e8d3b91.png)
<br>  
Figure 18: Configure Test Setting in Lambda

Once the prediction is correctly identified, we can proceed to create a REST API (integration request setup) and deploy the API to a stage so we obtain the invoke URL.
![image](https://user-images.githubusercontent.com/97729083/166319907-5984fff4-5734-41c3-bf8a-cad3f00267dc.png)
<br>
Figure 19: Sample API Gateway using POST

Lastly, as we already have the Lambda function, REST API and test data in place, we can test it using Postman, an HTPP client for testing web services. Below figure shows how the model has been successfully deployed and into an HTPP client and returns a prediction, by passing some test data.
![image](https://user-images.githubusercontent.com/97729083/166319927-e6e32406-1f18-4c70-9048-01ba09102ad3.png)
<br> 
Figure 20: Postman Interface
 

