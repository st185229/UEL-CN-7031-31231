#################################################################################
############UEL Big Data Course Work#
############ Suresh Thomas  
#### Course short name
##### UEL-CN-7031-31231
###  Course nameBig Data Analytics (31231)
##################################################

#!/usr/bin/env python
# coding: utf-8


# importing required libraries
import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

import pickle 
from os import path

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

from sklearn import metrics
from sklearn import preprocessing
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split 
from sklearn.metrics import classification_report

from sklearn.svm import SVC
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier


#% Load UNSW Data From CSV File - Cleaned data to reduce the memory requirement NB15_UEL_ST (Suresh Thomas"

data = pd.read_csv("/home/suresh/git/UEL-CN-7031-31231/solution/hdfs/spark/files/UNSW_NB15_UEL_ST.csv")

#   Many elements have service = - , Here we replace the same 
data[data['service']=='-']
# % Replace the service = - with naan"
data['service'].replace('-',np.nan,inplace=True)

data.dropna(inplace=True)

# Loading the fatures  to derive data types
# Comment to the UEL/Unicaf tutor from Suresh - The feature had some unicode charars in the description removed them and replaced them in the file 
features = pd.read_csv("/home/suresh/git/UEL-CN-7031-31231/solution/hdfs/spark/files/UNSW_NB15_features_UEL_ST.csv")
# Some of the Elements have Integer and some other integer - So to  all daat tyoes are coverted to lower case
features['Type '] = features['Type '].str.lower()

# Collect all variables with nominal, integer , binary and float - leaving timestamp and object 
# selecting column names of all data types
nominal_names = features['Name'][features['Type ']=='nominal']
integer_names = features['Name'][features['Type ']=='integer']
binary_names = features['Name'][features['Type ']=='binary']
float_names = features['Name'][features['Type ']=='float']

# selecting common column names from dataset and feature dataset
# Now  the data type collected from the feature file is matched with actual data UNSW
cols = data.columns
nominal_names = cols.intersection(nominal_names)
integer_names = cols.intersection(integer_names)
binary_names = cols.intersection(binary_names)
float_names = cols.intersection(float_names)

# Change all integers to numeric type
for c in integer_names:
  pd.to_numeric(data[c])

# Converting binary columns to numeric
for c in binary_names:
  pd.to_numeric(data[c])

# Converting float columns to numeric
for c in float_names:
  pd.to_numeric(data[c])

# Just checking the lables label =1 for attack records label = 0 for normal records
data.label.value_counts()

# This is the Binary classifier class
# Design and build a binary classifier over the dataset. Explain your algorithm and its configuration. 
# Explain your findings into both numerical and graphical representations. Evaluate the performance of the model and verify the accuracy and the effectiveness of your model.
plt.figure(figsize=(8,8))
plt.pie(data.label.value_counts(),labels=['normal-traffic','attack-traffic'],autopct='%0.2f%%')
plt.title("Pie chart distribution of normal and abnormal labels",fontsize=16)
plt.legend()
plt.savefig('./generated_diagrams/Pie_chart_bin_cls_sureshThomas.png')


# This is the multi class diagram
plt.figure(figsize=(8,8))
plt.pie(data.attack_cat.value_counts(),labels=list(data.attack_cat.unique()),autopct='%0.2f%%')
plt.title('Pie chart distribution of multi-class labels')
plt.legend(loc='best')
plt.savefig('./generated_diagrams/Pie_chart_multi_sureshthomas.png')

# Store number of columns
num_col = data.select_dtypes(include='number').columns

# selecting categorical data attributes
cat_col = data.columns.difference(num_col)
cat_col = cat_col[1:]
cat_col

# creating a dataframe with only categorical attributes
data_cat = data[cat_col].copy()
data_cat.head(20)

# attributes using pandas.get_dummies() function
# https://pandas.pydata.org/docs/reference/api/pandas.get_dummies.html Convert categorical variable into dummy/indicator variables.
data_cat = pd.get_dummies(data_cat,columns=cat_col)
data = pd.concat([data, data_cat],axis=1)
data.drop(columns=cat_col,inplace=True)

# selecting numeric attributes columns from data
num_col = list(data.select_dtypes(include='number').columns)
num_col.remove('id')
num_col.remove('label')
print("Number of columns %s", num_col)

# using minmax scaler for normalizing data
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html
#Transform features by scaling each feature to a given range.
#This estimator scales and translates each feature individually such that it is in the given range on the training set, e.g. between zero and one.
minmax_scale = MinMaxScaler(feature_range=(0, 1))
def normalization(df,col):
  for i in col:
    arr = df[i]
    arr = np.array(arr)
    df[i] = minmax_scale.fit_transform(arr.reshape(len(arr),1))
  return df

# calling normalization() function - see function above
data = normalization(data.copy(),num_col)
## **Binary Labels**
# changing attack labels into two categories 'normal' and 'abnormal'
bin_label = pd.DataFrame(data.label.map(lambda x:'normal' if x==0 else 'abnormal'))

# creating a dataframe with binary labels (normal,abnormal)
bin_data = data.copy()
bin_data['label'] = bin_label

# label encoding (0,1) binary labels
le1 = preprocessing.LabelEncoder()
enc_label = bin_label.apply(le1.fit_transform)
bin_data['label'] = enc_label
## the class numpy is stored locally
np.save("./generated_numpy/le1_classes.npy",le1.classes_,allow_pickle=True)
# one-hot-encoding attack label
multi_data = data.copy()
multi_label = pd.DataFrame(multi_data.attack_cat)
multi_data = pd.get_dummies(multi_data,columns=['attack_cat'])
# label encoding (0,1,2,3,4,5,6,7,8) multi-class labels
le2 = preprocessing.LabelEncoder()
enc_label = multi_label.apply(le2.fit_transform)
multi_data['label'] = enc_label
np.save("generated_numpy/le2_classes.npy",le2.classes_,allow_pickle=True)
num_col.append('label')

## **Correlation Matrix for Binary Labels**
print("Correlation Matrix for Binary Labels")
plt.figure(figsize=(20,8))
corr_bin = bin_data[num_col].corr()
sns.heatmap(corr_bin,vmax=1.0,annot=False)
plt.title('Correlation Matrix for Binary Labels',fontsize=16)
plt.savefig('./generated_diagrams/correlation_matrix_bin.png')

## **Correlation Matrix for Multi-class Labels**
num_col = list(multi_data.select_dtypes(include='number').columns)

# Correlation Matrix for Multi-class Labels
plt.figure(figsize=(20,8))
corr_multi = multi_data[num_col].corr()
sns.heatmap(corr_multi,vmax=1.0,annot=False)
plt.title('Correlation Matrix for Multi Labels',fontsize=16)
plt.savefig('./generated_diagrams/correlation_matrix_multi.png')

# finding the attributes which have more than 0.3 correlation with encoded attack label attribute 
corr_ybin = abs(corr_bin['label'])
highest_corr_bin = corr_ybin[corr_ybin >0.3]
highest_corr_bin.sort_values(ascending=True)

# selecting attributes found by using pearson correlation coefficient
bin_cols = highest_corr_bin.index
bin_cols

# Binary labelled Dataset
bin_data = bin_data[bin_cols].copy()
bin_data
# Save into dataset folder
bin_data.to_csv('./generated_datasets/bin_data.csv')


# finding the attributes which have more than 0.3 correlation with encoded attack label attribute 
# Why 0.3  - Shows a postive correlation
corr_ymulti = abs(corr_multi['label'])
highest_corr_multi = corr_ymulti[corr_ymulti >0.3]
highest_corr_multi.sort_values(ascending=True)

# selecting attributes found by using pearson correlation coefficient
multi_cols = highest_corr_multi.index

# Multi-class labelled Dataset
multi_data = multi_data[multi_cols].copy()

### **Saving Generated Dataset **
multi_data.to_csv('./generated_datasets/multi_data.csv')

# **BINARY CLASSIFICATION**
print( "Binary classification")
## **Data Splitting**
X = bin_data.drop(columns=['label'],axis=1)
Y = bin_data['label']
# Training Set
X_train,X_test,y_train,y_test = train_test_split(X,Y,test_size=0.20, random_state=50)

## **Linear Regression**
lr_bin = LinearRegression()
lr_bin.fit(X_train, y_train)
y_pred = lr_bin.predict(X_test)

# Prepared  based ib 0.6  threshold
round = lambda x:1 if x>0.6 else 0
vfunc = np.vectorize(round)
y_pred = vfunc(y_pred)

print("MAE - Mean Absolute Error - " , metrics.mean_absolute_error(y_test, y_pred))
print("MSE - Mean Squared Error - " , metrics.mean_squared_error(y_test, y_pred))
print("RMSE- Root Mean Squared Error - " , np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
print("R2 Score - " , metrics.explained_variance_score(y_test, y_pred)*100)
print("Accuracy - ",accuracy_score(y_test,y_pred)*100)

cls_report= classification_report(y_true=y_test, y_pred=y_pred,target_names=le1.classes_)
print("Printing report")
print(cls_report)
# Saving Data Set
lr_bin_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
lr_bin_df.to_csv('./generated_predictions/lr_real_predication_bin.csv')
lr_bin_df
# Saving Binary class diagram
plt.figure(figsize=(20,8))
plt.plot(y_pred[:200], label="prediction", linewidth=2.0,color='blue')
plt.plot(y_test[:200].values, label="real_values", linewidth=2.0,color='lightcoral')
plt.legend(loc="best")
plt.title("Linear Regression Binary Classification")
plt.savefig('./generated_diagrams/lr_real_pred_bin.png')

pkl_filename = "./generated_models/linear_regressor_binary_suresh.pkl"
if (not path.isfile(pkl_filename)):
  # saving the trained model to disk 
  with open(pkl_filename, 'wb') as file:
    pickle.dump(lr_bin, file)
  print("Saved model to disk")
else:
  print("Previous Model exists on the disk! Please Remove")

logr_bin = LogisticRegression(random_state=123, max_iter=5000)
logr_bin

logr_bin.fit(X_train,y_train)

y_pred = logr_bin.predict(X_test)

#%%
print("\nNew Scores")
print("Mean Absolute Error - " , metrics.mean_absolute_error(y_test, y_pred))
print("Mean Squared Error - " , metrics.mean_squared_error(y_test, y_pred))
print("Root Mean Squared Error - " , np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
print("R2 Score - " , metrics.explained_variance_score(y_test, y_pred)*100)
print("Accuracy - ",accuracy_score(y_test,y_pred)*100)
cls_report= classification_report(y_true=y_test, y_pred=y_pred,target_names=le1.classes_)
print(cls_report)
print ("\n prediceted")
logr_bin_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
logr_bin_df.to_csv('./generated_predictions/logr_real_pred_bin.csv')
logr_bin_df

plt.figure(figsize=(20,8))
plt.plot(y_pred[:200], label="prediction", linewidth=2.0,color='blue')
plt.plot(y_test[:200].values, label="real_values", linewidth=2.0,color='lightcoral')
plt.legend(loc="best")
plt.title("Linear Regression Binary Classification")
plt.savefig('./generated_diagrams/linearrrg_real_vs_pred_bin.png')

### **Saving Trained Model to Disk**

pkl_filename = "./generated_models/logistic_regressor_binary_1.pkl"
if (not path.isfile(pkl_filename)):
  # saving the trained model to disk 
  with open(pkl_filename, 'wb') as file:
    pickle.dump(logr_bin, file)
  print("Saved model to disk")
else:
  print("Model already saved")

# **MULTI-CLASS CLASSIFICATION**
print("Multi class")
X = multi_data.drop(columns=['label'],axis=1)
Y = multi_data['label']
X_train,X_test,y_train,y_test = train_test_split(X,Y,test_size=0.30, random_state=100)
## **Linear Regression**
lr_multi = LinearRegression()
lr_multi.fit(X_train, y_train)
y_pred = lr_multi.predict(X_test)
for i in range(len(y_pred)):
  y_pred[i] = int(round(y_pred[i]))
print("Mean Absolute Error - " , metrics.mean_absolute_error(y_test, y_pred))
print("Mean Squared Error - " , metrics.mean_squared_error(y_test, y_pred))
print("Root Mean Squared Error - " , np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
print("R2 Score - " , metrics.explained_variance_score(y_test, y_pred)*100)
print("Accuracy - ",accuracy_score(y_test,y_pred)*100)
print(classification_report(y_test, y_pred,target_names=le2.classes_))

#%%

lr_multi_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
lr_multi_df.to_csv('./generated_predictions/lr_real_pred_multi.csv')
lr_multi_df

#%%
plt.figure(figsize=(20,8))
plt.plot(y_pred[100:200], label="prediction", linewidth=2.0,color='blue')
plt.plot(y_test[100:200].values, label="real_values", linewidth=2.0,color='lightcoral')
plt.legend(loc="best")
plt.title("Linear Regression Multi-class Classification")
plt.savefig('./generated_diagrams/lr_real_vs_pred_multi_class.png')

pkl_filename = "./generated_models/linear_regressor_multi.pkl"
if (not path.isfile(pkl_filename)):
  # saving the trained model to disk 
  with open(pkl_filename, 'wb') as file:
    pickle.dump(lr_multi, file)
  print("Saved model to disk")
else:
  print("Model already saved")

## **End**


