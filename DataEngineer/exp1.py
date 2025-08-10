#pip install pandas
import pandas as pd
import mysql.connector
conn=mysql.connector.connect(host="localhost",user="root",password="",database="products")
#1.extract data from csv
df=pd.read_csv("sample.csv")
print(df)
print(df.head(0))
print(df['proname'])
#2.transform
df["overallprice"]=df["proquantity"]*df['proprice']
cur=conn.cursor()
print(df)
for _,row in df.iterrows():
    cur.execute("insert into prod_tb1(proname,proprice,proquantity)values(%s,%s,%s)",(row['proname'],int(row['proprice']),int(row['proquantity'])))
conn.commit()
cur.close()
conn.close()
print("succesfully inserted into db")