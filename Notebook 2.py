#!/usr/bin/env python
# coding: utf-8

# ## Notebook 2
# 
# New notebook

# In[7]:


import csv 
import requests
import xml.etree.ElementTree as ET

# URL of the RSS feed
rss_url = "https://www.hindustantimes.com/feeds/rss/environment/rssfeed.xml"

# Fetching the XML data
response = requests.get(rss_url)
xml_data = response.content
print(xml_data)


# In[8]:


import pandas as pd
import xml.etree.ElementTree as ET

def get_xml_data():
    """
    Function to define XML data.
    """
    xml_data = """
    <bookstore specialty='novel'>
      <book style='autobiography'>
        <title>Seven Years in Trenton</title>
        <author>
          <first-name>Joe</first-name>
          <last-name>Bob</last-name>
          <award>Trenton Literary Review Honorable Mention</award>
        </author>
        <price>12</price>
      </book>
      <book style='textbook'>
        <title>History of Trenton</title>
        <author>
          <first-name>Mary</first-name>
          <last-name>Bob</last-name>
          <publication>
            Selected Short Stories of
            <first-name>Mary</first-name> <last-name>Bob</last-name>
          </publication>
        </author>
        <price>55</price>
      </book>
      <magazine style='glossy' frequency='monthly'>
        <title>Tracking Trenton</title>
        <price>2.50</price>
        <subscription price='24' per='year'/>
      </magazine>
      <book style='novel' id='myfave'>
        <title>Trenton Today, Trenton Tomorrow</title>
        <author>
          <first-name>Toni</first-name>
          <last-name>Bob</last-name>
          <degree from='Trenton U'>B.A.</degree>
          <degree from='Harvard'>Ph.D.</degree>
          <award>Pulitzer</award>
          <publication>Still in Trenton</publication>
          <publication>Trenton Forever</publication>
        </author>
        <price intl='canada' exchange='0.7'>6.50</price>
        <excerpt>
          <p>It was a dark and stormy night.</p>
          <p>But then all nights in Trenton seem dark and
          stormy to someone who has gone through what
          <emph>I</emph> have.</p>
          <definition-list>
            <term>Trenton</term>
            <definition>misery</definition>
          </definition-list>
        </excerpt>
      </book>
      <my:book style='leather' price='29.50' xmlns:my='http://www.placeholder-name-here.com/schema/'>
        <my:title>Who's Who in Trenton</my:title>
        <my:author>Robert Bob</my:author>
      </my:book>
    </bookstore>
    """
    return xml_data

def parse_xml(xml_data):
    root = ET.fromstring(xml_data)
    books = []
    for book in root.findall('book'):
        title = book.find('title').text
        style = book.get('style')
        author = book.find('author')
        author_first_name = author.find('first-name').text
        author_last_name = author.find('last-name').text
        price = book.find('price').text
        books.append({
            'Title': title,
            'Style': style,
            'Author First Name': author_first_name,
            'Author Last Name': author_last_name,
            'Price': price
        })
    return books

def savetocsv(books):
    df = pd.DataFrame(books)
    filename='xmldata.csv'
    csv_output_path=f"abfss://6691817e-5dcf-43f4-a007-24106381cf71@onelake.dfs.fabric.microsoft.com/dba6dd94-a91d-4e94-a8dc-9066d156f76d/Files/{filename}"
    df.to_csv(csv_output_path, index=False)
    print(f"Data saved to {filename}")

def main():
    xml_data = get_xml_data()
    books = parse_xml(xml_data)
    savetocsv(books)

#abfss://6691817e-5dcf-43f4-a007-24106381cf71@onelake.dfs.fabric.microsoft.com/dba6dd94-a91d-4e94-a8dc-9066d156f76d/Files/{filename}



# In[11]:


main()
csv_output_path=f"https://onelake.dfs.fabric.microsoft.com/6691817e-5dcf-43f4-a007-24106381cf71/dba6dd94-a91d-4e94-a8dc-9066d156f76d/Files/xmldata.csv"
raw_df = spark.read.csv(filepath, header=True, inferSchema=True)
table_name = "xmldata"
raw_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

