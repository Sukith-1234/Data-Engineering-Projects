#!/usr/bin/env python
# coding: utf-8

# In[45]:


# Import required Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency
from kmodes.kmodes import KModes
import sqlite3
import pyodbc


# Establish the connection between Azure SQL Database and DataBricks (Python Enviorenment)
# 
# Below code use to connect SQL to Databricks only

# In[ ]:


connectionString="jdbc:sqlserver://SERVER NAME;database=DATABASE NAME;user=USER NAME;password={PASSWORD};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# In[ ]:


#Fetch the data from Azure SQL Databse to the Python Enviorenment and convert the data to Data Frame using Pandas Librarey
data=spark.read.jdbc(connectionString,"TABLE NAME")
df=data.select("*").toPandas()
df


# If you are use direct connection SQL to Python you need to follow below procedure

# In[71]:


#Import pyodbc library and create connection with SQL and Python using below code and inser Server name,database  name,user name & password

import pyodbc
connect=pyodbc.connect(
    
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=SERVER NAME;"
    "Database=DATABASE NAME;"
    "Uid=USER NAME;"
    "Pwd={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
    
)


# In[72]:


#Direct Azure SQL to Python SQL Query to fetch all the data

sql_query= """
SELECT * FROM dbo.UserCSV
"""


# In[76]:


# convert SQL data to Pandas data frame

dfdirect=pd.read_sql(sql_query,connect)
dfdirect


# In the data set all of the important data are categorical data such as gender,employee skill,employee skill,subscription plan,statu,payment method,term etc.

# In[ ]:


# check the shape of the dataset
print(df.shape)


# Solutions for Part 2-Q1 & Q2

# Plot the histogram to check the Gender distribution
# 
#     According to the output Highest number of Users are Males and Lowest gender type is Genderfluid

# In[ ]:


plt.figure(figsize=(14,5))
sns.countplot(x='gender',data=df, palette='rainbow',order = df['gender'].value_counts().index)
plt.title("Gender Distribution")
plt.ylabel('Total no of Users')


# Analyse the user Employment Title Data
# 
#     In the data set there 888 types of Employment titles and Below filter out top 10 Titles only
#    
#     Marketing consultants,Mining designers,Advertising designers,Consulting coordinators title are highest number of employee titles

# In[ ]:


plt.figure(figsize=(18,5))
chart=sns.countplot(x='emplyment_title',data=df,order = df['emplyment_title'].value_counts().iloc[:10].index, palette='ch:.25')
plt.title(" Employment Title")
plt.ylabel('Total no of Users')
chart.set_xticklabels(chart.get_xticklabels(), rotation=45)


# Analyse the user Employment Key Skill Data
# 
#     Self Motivated,Leadership,Networking are the highest employee skills & Technical savy is the least identify skill

# In[ ]:


plt.figure(figsize=(18,5))
chart=sns.countplot(x='employment_key_skill',data=df,order = df['employment_key_skill'].value_counts().index, palette='Set2')
plt.title(" Employment key skill ")
plt.ylabel('Total no of Users')
chart.set_xticklabels(chart.get_xticklabels(), rotation=45)


# Plot the histogram to check the Gender distribution by subscription plan
# 
#     Males are mostly used subscription plan type is Business
# 
#     Most Females,Agender,Bigender are used Free trial version
# 
#     Polygenders,Non binary are used Professional plan
# 
#     Genderqueer used Gold plan

# In[ ]:


plt.figure(figsize=(18,5))
sns.countplot(x='gender',data=df,hue='subscription_plan', palette='Set1')
plt.title("Gender Type & Subscription Plan")
plt.ylabel('Total no of Users')


# Plot the histogram to check the Gender distribution by subscription status
# 
#     According to graph highest active users are Males & Non binary 
# 
#     Most of females are Idle compare to Active status 
# 
#     Genderqueer has the highest number which blocked thier subscriptions

# In[ ]:


plt.figure(figsize=(18,5))
sns.countplot(x='gender',data=df,hue='subscription_status', palette='Set1')
plt.title("Gender Type & subscription_status")
plt.ylabel('Total no of Users')


# Plot the histogram to check the Gender distribution by Payment Method
# 
#     Most males are prefer the cheques and debit card for the payments
# 
#     Polygenders are mostyly use Credit Cards as thier main payment method
# 
#     Females  preferes as thier  highest payment method is  Bitcoins

# In[ ]:


plt.figure(figsize=(18,5))
sns.countplot(x='gender',data=df,hue='subscription_payment_method', palette='Set1')
plt.title("Gender Type & Payment method")
plt.ylabel('Total no of Users')


# Plot the histogram to check the Gender distribution by Subscription-Term
# 
#     Males,Genderqueer,Non binary and polygender are prefer to get full subscription & Payment in Advance
#     
#     Females are less prefer in advance payment

# In[ ]:



plt.figure(figsize=(18,5))
sns.countplot(x='gender',data=df,hue='subscription_term', palette='Set1')
plt.title("Gender Type & subscription_term")
plt.ylabel('Total no of Users')


# Plot the histogram to check the Subscription status and payment method
# 
#     Active users  are mostly use Google pay and least users use payment method Debit Card
#     
#     Most block users use thier payment method is Cheques
#     
#     Subscribers who are idel,most uses Bitcoins

# In[ ]:


plt.figure(figsize=(18,5))
sns.countplot(x='subscription_status',data=df,hue='subscription_payment_method', palette='Set2')
plt.title("subscription_status & subscription_payment_method")
plt.ylabel('Total no of Users')


# Pie chart for show the total distribution of no users by subscription status
# 
#     According to the pie chart 50% of users  subscription status is Idle and Pending
#     
#     23% users are Idle and 27% users are Active users

# In[ ]:


def label_function(val):
    return f'{val / 100 * len(df):.0f}\n{val:.0f}%'

df.groupby('subscription_status').size().plot(kind='pie',autopct=label_function, textprops={'fontsize': 12},
                                 colors=['violet', 'lime','red','green'])
plt.title('No of Users by Subscription Status', size=12)


# Identify the top 10  states users live
# 
#     Montana has the highest number of Users

# In[ ]:


plt.figure(figsize=(18,5))
chart=sns.countplot(x='StateName',data=df,order = df['StateName'].value_counts().iloc[:10].index, palette='cubehelix')
plt.title(" Top 10 States")
plt.ylabel('Total no of Users')
chart.set_xticklabels(chart.get_xticklabels(), rotation=90)


# Identify the most common First names details (Top 10)
# 
#     Robbie is the most common first name and cleo/arron etc is the next common first name

# In[ ]:


plt.figure(figsize=(18,5))
chart2=sns.countplot(x='FirstName',data=df,order = df['FirstName'].value_counts().iloc[:10].index, palette='cubehelix')
plt.title(" Most Common First Names")
plt.ylabel('Total no of Users')
chart2.set_xticklabels(chart2.get_xticklabels(), rotation=45)


# Most use Last names of the users (Top 10)
# 
#     Wilderman is the most commonly use Last name

# In[ ]:


plt.figure(figsize=(18,5))
chart3=sns.countplot(x='LastName',data=df,order = df['LastName'].value_counts().iloc[:10].index, palette='gist_earth')
plt.title(" Most common Last Names")
plt.ylabel('Total no of Users')
chart3.set_xticklabels(chart3.get_xticklabels(), rotation=45)


# Identify the correlation between categorical varibles
# 
#     Assumption(H0): The two columns are NOT related to each other
#     
#     Result of Chi-Sq Test:if the value of Chi-sq test is less than 0.05 we can assume there is correlation between identified two varibales &
#     if the value is greater than 0.05 we can assume there is no correlation between given variables

# In[ ]:


# checking correlation between gender & subscription plan

# Cross tabulation between gender and subscription_plan
CrosstabResult=pd.crosstab(index=df['gender'],columns=df['subscription_plan'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.888330398649698,hence gender & subscription plan is not correlated

# In[ ]:


# checking correlation between subscription term and subscription payment method

# Cross tabulation between subscription_term and subscription_payment_method

CrosstabResult=pd.crosstab(index=df['subscription_term'],columns=df['subscription_payment_method'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.6026003428424316,hence subscription_term and subscription_payment_method is not correlated

# In[ ]:


# checking correlation between subscription_plan & subscription_term 

# Cross tabulation between subscription_plan & subscription_term 

CrosstabResult=pd.crosstab(index=df['subscription_plan'],columns=df['subscription_term'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)

print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.4137233786082608 hence there is no correlation between subscription_plan & subscription_term

# In[ ]:


# checking correlation between subscription_plan & subscription_status 

# Cross tabulation between subscription_plan & subscription_status 

CrosstabResult=pd.crosstab(index=df['subscription_plan'],columns=df['subscription_status'])
print(CrosstabResult)

 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.5277753429729202 hence there is no correlation between subscription_plan & subscription_status

# In[ ]:


# checking correlation between subscription_plan & StateName 

# Cross tabulation between subscription_plan & StateName

CrosstabResult=pd.crosstab(index=df['StateName'],columns=df['subscription_plan'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.8971667040935034 hence there is no correlation between subscription_plan & State

# In[ ]:


# checking correlation between subscription_plan & employee title 

# Cross tabulation between subscription_plan & employee title

CrosstabResult=pd.crosstab(index=df['emplyment_title'],columns=df['subscription_plan'])
print(CrosstabResult)

# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)

print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.3700476816527415 hence there is no correlation between subscription_plan & employee title

# In[ ]:


# checking correlation between subscription_status & employee title 

# Cross tabulation between subscription_status	 & employee title

CrosstabResult=pd.crosstab(index=df['emplyment_title'],columns=df['subscription_status'])
print(CrosstabResult)

# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.4492485038421585,hence there is no correlation between subscription_status & employee title

# In[ ]:


# checking correlation between subscription_term & employee title 

# Cross tabulation between subscription_term & employee title

CrosstabResult=pd.crosstab(index=df['emplyment_title'],columns=df['subscription_term'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.6662423143078474,hence there is no correlation between subscription_term & employee title

# In[ ]:


# checking correlation between subscription_payment_method & employee title 

# Cross tabulation between subscription_payment_method	 & employee title

CrosstabResult=pd.crosstab(index=df['emplyment_title'],columns=df['subscription_payment_method'])
print(CrosstabResult)
  
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# The P-Value of the ChiSq Test is: 0.3457651247975165,hence there is no correlation between payment_method & employee title

# In[ ]:


# checking correlation between subscription_payment_method & employment_key_skill 

# Cross tabulation between subscription_payment_method	 & employment_key_skill

CrosstabResult=pd.crosstab(index=df['emplyment_title'],columns=df['employment_key_skill'])
print(CrosstabResult)
 
# Performing Chi-sq test
ChiSqResult = chi2_contingency(CrosstabResult)
 
print('The P-Value of the ChiSq Test is:', ChiSqResult[1])


# Grouping the customers by identifying similaraties and dissimalrities of the data set (Clustering)
# 
#     Identify the suitable categorical data and create anoth Data frame to proceed clustering/grouping
# 
#     Use K-mean clustering method

# In[ ]:


df1=df[['id','user_id','gender','emplyment_title','employment_key_skill','StateName','subscription_plan','subscription_status','subscription_payment_method','subscription_term']]
df1 = df1.set_index('id')
df1


# Draw the plot between  Cost vs No of cluster to draw  Elbow curve to find optimal K value
# 
#     For KModes, plot cost for a range of K values. Cost is the sum of all the dissimilarities between the clusters
#     
#     Select the K where observe an elbow-like bend with a lesser cost value

# In[ ]:


cost = []
K = range(1,12)
for num_clusters in list(K):
    kmode = KModes(n_clusters=num_clusters, init = "random", n_init = 5, verbose=1)
    kmode.fit_predict(df1)
    cost.append(kmode.cost_)
    
plt.plot(K, cost, 'bx-')
plt.xlabel('No. of clusters')
plt.ylabel('Cost')
plt.title('Elbow Method For Optimal k')
plt.show()


# Suitable K value is 9 ,according to the gaph

# In[ ]:


# Building the model with 9 clusters

kmode = KModes(n_clusters=9, init = "random", n_init = 9, verbose=1)
clusters = kmode.fit_predict(df1)
clusters


# Output table of categorized data

# In[ ]:


# Apply cluster number to the df1 Table

df1.insert(0, "Cluster", clusters, True)
df1


# Histogram of user clusters

# In[ ]:


#Plot the Histogram to identify the user distribution of each cluster
plt.figure(figsize=(18,5))
sns.countplot(x='Cluster',data=df1,palette='Set1')
plt.title("Clustering Users ")
plt.ylabel('Total no of Users')

