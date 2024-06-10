import pandas as pd
data1 = pd.read_csv("Data Collection and processing\Classes April-May 2018.csv")
data2 = pd.read_csv("Data Collection and processing\Classes June 2018.csv")
#Deleting duplicates
data1 = data1.drop_duplicates()
data2 = data2.drop_duplicates()

#missing values with the median
med1 =data1['Price (INR)'].median()
med2 = data2['Price (INR)'].median() 
data1['Price (INR)'] = data1['Price (INR)'].fillna(med1)
data2['Price (INR)'] = data2['Price (INR)'].fillna(med2)

#Correcting datatype to datetime
data1['BookingEndDateTime (Month / Day / Year)'] = pd.to_datetime(data1['BookingEndDateTime (Month / Day / Year)'], format='%d-%b-%y')
#changing the date format to DD-MM-YYYY for better understanding
data1['BookingEndDateTime (Month / Day / Year)'] = data1['BookingEndDateTime (Month / Day / Year)'].dt.strftime('%d-%m-%Y')
#changing the column name
data1 = data1.rename(columns={'BookingEndDateTime (Month / Day / Year)': 'BookingEndDateTime (Day/Month/Year)'})
#changing the time format of a column in data1
data1['BookingStartTime'] = pd.to_datetime(data1['BookingStartTime'], format='%H:%M:%S').dt.strftime('%I:%M %p')

#changing the date format and column name for data2 same as data1
data2['BookingEndDateTime (Month / Day / Year)'] = pd.to_datetime(data2['BookingEndDateTime (Month / Day / Year)'], format='%d-%b-%y')
data2['BookingEndDateTime (Month / Day / Year)'] = data2['BookingEndDateTime (Month / Day / Year)'].dt.strftime('%d-%m-%Y')
data2 = data2.rename(columns={'BookingEndDateTime (Month / Day / Year)': 'BookingEndDateTime (Day/Month/Year)'})
data2['BookingStartTime'] = pd.to_datetime(data2['BookingStartTime'], format='%H:%M:%S').dt.strftime('%I:%M %p')

#Correcting data types
data1 =data1.astype({'Price (INR)' : 'int64'})
data2 =data2.astype({'Price (INR)' : 'int64'})

#Merging datasets
merged_df = pd.concat([data1,data2], ignore_index=True)

#Ensuring data consistency - checking datetime formats and datatypes of the columns
merged_df['BookingEndDateTime (Day/Month/Year)'] = pd.to_datetime(merged_df['BookingEndDateTime (Day/Month/Year)'],format ="%d-%m-%Y").dt.strftime('%d-%m-%Y')
merged_df['BookingStartTime'] = pd.to_datetime(merged_df['BookingStartTime'], format='%I:%M %p').dt.strftime('%I:%M %p')

#Cross referencing
def no_of_rows():
    return len(data1) +len(data2) == len(merged_df)
    
quality_check = {
    "missing": merged_df.isnull().sum(), #returns the sum of missing values in each column
    "duplicate": merged_df.duplicated().sum(), #returns the sum of duplicate values in each row
    "datatypes": merged_df.dtypes, #returns the datatypes of each column
    "summary": merged_df.describe(), #summarizes the data
    "Equal no. of rows" : no_of_rows(), #returns true if the number of rows in merged dataset is equal to the number of rows in data1 combined with data2
}

# Print the quality report
for key, value in quality_check.items():
    print(f"{key}: \n{value}\n")


data1.to_csv('C:/Users/gulat/Documents/Cult internship/Cleaned_datasets/Csv data/Data1.csv',index=False)
data2.to_csv('C:/Users/gulat/Documents/Cult internship/Cleaned_datasets/Csv data/Data2.csv',index=False)
merged_df.to_csv('C:/Users/gulat/Documents/Cult internship/Cleaned_datasets/Csv data/Cleaned.csv',index=False)
print(merged_df.head())