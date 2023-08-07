#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
import numpy as np


# In[2]:


conn_url = 'postgresql://postgres:123@localhost/5310project'
engine = create_engine(conn_url)
connection = engine.connect()


# ### Insert into cities

# In[ ]:


connection.execute("DROP TABLE IF EXISTS cities CASCADE")
stmt = """
CREATE TABLE cities (
    city_id VARCHAR(10) PRIMARY KEY NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    city_description VARCHAR(255)
);
    """
connection.execute(stmt)


# In[ ]:


cities_df = pd.read_csv('cities.csv')
cities_df.to_sql(name='cities', con=engine, if_exists='append', index=False)


# In[10]:


results = connection.execute("SELECT * from cities").fetchall()
column_names = results[0].keys()
cities = pd.DataFrame(results, columns=column_names)
cities.to_csv("cities.csv", index=False)
cities.head()


# ### Insert into Ask_for_drive

# In[19]:


connection.execute("DROP TABLE IF EXISTS drive_satisfaction CASCADE")
connection.execute("DROP TABLE IF EXISTS ask_for_drive CASCADE")
                   
stmt = """
CREATE TABLE ask_for_drive (
    customer_id INT PRIMARY KEY,
    app_name VARCHAR(10),
    city_id VARCHAR(10),
    pick_up_place VARCHAR(255),
    drop_off_place VARCHAR(255),
    duration INT,
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE drive_satisfaction (
    rating_id INT PRIMARY KEY,
    renter_id INT,
    reviews VARCHAR(255),
    rating DECIMAL(3,1),
    FOREIGN KEY (renter_id) REFERENCES Ask_for_Drive(customer_id)
);
    """
connection.execute(stmt)


# In[20]:


drive_df = pd.read_csv('ask_for_drive.csv')
drive_df.to_sql(name='ask_for_drive', con=engine, if_exists='append', index=False)


# In[13]:


results = connection.execute("SELECT * from ask_for_drive").fetchall()
column_names = results[0].keys()
ask_for_drive = pd.DataFrame(results, columns=column_names)
ask_for_drive.to_csv("ask_for_drive.csv", index=False)
ask_for_drive.head()


# In[21]:


drive_review_df = pd.read_csv('drive_satisfaction.csv')
drive_review_df.to_sql(name='drive_satisfaction', con=engine, if_exists='append', index=False)


# In[15]:


results = connection.execute("SELECT * from drive_satisfaction").fetchall()
column_names = results[0].keys()
drive_satisfaction = pd.DataFrame(results, columns=column_names)
drive_satisfaction.to_csv("drive_satisfaction.csv", index=False)
drive_satisfaction.head()


# ### Insert into Car_rental

# In[31]:


connection.execute("DROP TABLE IF EXISTS car_satisfaction CASCADE")
connection.execute("DROP TABLE IF EXISTS car_rental CASCADE")
                   
stmt = """
CREATE TABLE car_rental (
    renter_id INT PRIMARY KEY,
    city_id VARCHAR(10),
    start_place VARCHAR(255),
    return_place VARCHAR(255),
    duration DECIMAL(4,2),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE car_satisfaction (
    rating_id INT PRIMARY KEY,
    renter_id INT,
    reviews VARCHAR(255),
    rating DECIMAL(3, 1),
    FOREIGN KEY (renter_id) REFERENCES car_rental(renter_id)
);
    """
connection.execute(stmt)


# In[32]:


car_df = pd.read_csv('car_rental.csv')
car_df.to_sql(name='car_rental', con=engine, if_exists='append', index=False)


# In[16]:


results = connection.execute("SELECT * from car_rental").fetchall()
column_names = results[0].keys()
car_rental = pd.DataFrame(results, columns=column_names)
car_rental.to_csv("car_rental.csv", index=False)
car_rental.head()


# In[33]:


car_review_df = pd.read_csv('car_reviews.csv')
car_review_df.to_sql(name='car_satisfaction', con=engine, if_exists='append', index=False)


# In[17]:


results = connection.execute("SELECT * from car_satisfaction").fetchall()
column_names = results[0].keys()
car_satisfaction = pd.DataFrame(results, columns=column_names)
car_satisfaction.to_csv("car_satisfaction.csv", index=False)
car_satisfaction.head()


# ### Insert into Bike_rental

# In[56]:


connection.execute("DROP TABLE IF EXISTS bike_satisfaction CASCADE")
connection.execute("DROP TABLE IF EXISTS bike_rental CASCADE")
                   
stmt = """
CREATE TABLE bike_rental (
    renter_id INT PRIMARY KEY,
    city_id VARCHAR(10),
    start_place VARCHAR(255),
    return_place VARCHAR(255),
    duration DECIMAL(4,2),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE bike_satisfaction (
    rating_id INT PRIMARY KEY,
    renter_id INT,
    reviews VARCHAR(255),
    rating DECIMAL(3, 1),
    FOREIGN KEY (renter_id) REFERENCES bike_rental(renter_id)
);
    """
connection.execute(stmt)


# In[57]:


bike_df = pd.read_csv('bike_rental.csv')
bike_df.to_sql(name='bike_rental', con=engine, if_exists='append', index=False)


# In[18]:


results = connection.execute("SELECT * from bike_rental").fetchall()
column_names = results[0].keys()
bike_rental = pd.DataFrame(results, columns=column_names)
bike_rental.to_csv("bike_rental.csv", index=False)
bike_rental.head()


# In[58]:


bike_review_df = pd.read_csv('bike_reviews.csv')
bike_review_df.to_sql(name='bike_satisfaction', con=engine, if_exists='append', index=False)


# In[19]:


results = connection.execute("SELECT * from bike_satisfaction").fetchall()
column_names = results[0].keys()
bike_satisfaction = pd.DataFrame(results, columns=column_names)
bike_satisfaction.to_csv("bike_satisfaction.csv", index=False)
bike_satisfaction.head()


# ### Insert into Restaurants

# In[83]:


connection.execute("DROP TABLE IF EXISTS restaurant_inspections CASCADE")
connection.execute("DROP TABLE IF EXISTS restaurants CASCADE")
                   
stmt = """
CREATE TABLE restaurants (
    restaurant_id INT PRIMARY KEY,
    city_id VARCHAR(10),
    name VARCHAR(255) NOT NULL,
    region VARCHAR(100) NOT NULL,
    street VARCHAR(255) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    cuisine_type VARCHAR(100),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE restaurant_inspections (
    inspection_id INT PRIMARY KEY,
    restaurant_id INT,
    inspection_date DATE,
    violations TEXT,
    critical_flag VARCHAR(20),
    grade VARCHAR(20),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id)
);
    """
connection.execute(stmt)


# In[84]:


restaurant_df = pd.read_csv('restaurants.csv', encoding='latin-1')
restaurant_df.to_sql(name='restaurants', con=engine, if_exists='append', index=False)


# In[20]:


results = connection.execute("SELECT * from restaurants").fetchall()
column_names = results[0].keys()
restaurants = pd.DataFrame(results, columns=column_names)
restaurants.to_csv("restaurants.csv", index=False)
restaurants.head()


# In[85]:


restaurant_inspection_df = pd.read_csv('restaurant inspections.csv')
restaurant_inspection_df.to_sql(name='restaurant_inspections', con=engine, if_exists='append', index=False)


# In[21]:


results = connection.execute("SELECT * from restaurant_inspections").fetchall()
column_names = results[0].keys()
restaurant_inspections = pd.DataFrame(results, columns=column_names)
restaurant_inspections.to_csv("restaurant_inspections.csv", index=False)
restaurant_inspections.head()


# ### Insert into Weather

# In[148]:


connection.execute("DROP TABLE IF EXISTS weather")
                   
stmt = """
CREATE TABLE weather (
    datetime TIMESTAMP,
    city_id VARCHAR(10),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    temperature DECIMAL(5,2),
    wind_speed INT,
    wind_direction INT,
    pressure INT,
    humidity INT,
    description VARCHAR(255),
    PRIMARY KEY (datetime,city_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);
    """
connection.execute(stmt)


# In[149]:


attribute_df = pd.read_csv('city_attributes.csv')
windSpeed_df = pd.read_csv('wind_speed.csv')
windDirection_df = pd.read_csv('wind_direction.csv')
description_df = pd.read_csv('weather_description.csv')
temperature_df = pd.read_csv('temperature.csv')
pressure_df = pd.read_csv('pressure.csv')
humidity_df = pd.read_csv('humidity.csv')


# In[150]:


melted_windSpeed_df = pd.melt(windSpeed_df, id_vars=['datetime'], var_name='city_id', value_name='wind_speed')
melted_windDirection_df = pd.melt(windDirection_df, id_vars=['datetime'], var_name='city_id', value_name='wind_direction')
melted_description_df = pd.melt(description_df, id_vars=['datetime'], var_name='city_id', value_name='description')
melted_temperature_df = pd.melt(temperature_df, id_vars=['datetime'], var_name='city_id', value_name='temperature')
melted_pressure_df = pd.melt(pressure_df, id_vars=['datetime'], var_name='city_id', value_name='pressure')
melted_humidity_df = pd.melt(humidity_df, id_vars=['datetime'], var_name='city_id', value_name='humidity')


# In[151]:


melted_windSpeed_df.head()


# In[152]:


merged_df = pd.merge(melted_temperature_df, melted_windSpeed_df, on = ['datetime','city_id'])
merged_df = merged_df.merge(melted_windDirection_df, on = ['datetime','city_id'])
merged_df = merged_df.merge(melted_pressure_df, on = ['datetime','city_id'])
merged_df = merged_df.merge(melted_humidity_df, on = ['datetime','city_id'])
merged_df = merged_df.merge(melted_description_df, on = ['datetime','city_id'])


# In[153]:


merged_df.head()


# In[154]:


merged_df = merged_df.merge(attribute_df, on = 'city_id')


# In[155]:


merged_df.head()


# In[156]:


merged_df.to_sql(name='weather', con=engine, if_exists='append', index=False)


# In[22]:


results = connection.execute("SELECT * from weather").fetchall()
column_names = results[0].keys()
weather = pd.DataFrame(results, columns=column_names)
weather.to_csv("weather.csv", index=False)
weather.head()


# ### Insert into Neighborhood

# In[25]:


connection.execute("DROP TABLE IF EXISTS neighbourhood")
                   
stmt = """
CREATE TABLE neighbourhood (
    neighbourhood VARCHAR(50) PRIMARY KEY,
    city_id VARCHAR(10),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);
    """
connection.execute(stmt)


# In[26]:


neighbourhood_df = pd.read_csv('neighbourhoods.csv')
neighbourhood_df.to_sql(name='neighbourhood', con=engine, if_exists='append', index=False)


# In[23]:


results = connection.execute("SELECT * from neighbourhood").fetchall()
column_names = results[0].keys()
neighbourhood = pd.DataFrame(results, columns=column_names)
neighbourhood.to_csv("neighbourhood.csv", index=False)
neighbourhood.head()


# ### Insert into Airbnb_Listing

# In[80]:


connection.execute("DROP TABLE IF EXISTS airbnb_listing")
                   
stmt = """
CREATE TABLE airbnb_listing (
    listing_id BIGINT PRIMARY KEY,
    availability INT NOT NULL,
    price INT NOT NULL,
    minimum_nights INT
);
    """
connection.execute(stmt)


# In[81]:


listing_df = pd.read_csv('listings.csv')
listing_df.to_sql(name='airbnb_listing', con=engine, if_exists='append', index=False)


# In[83]:


results = connection.execute("SELECT * from airbnb_listing").fetchall()
column_names = results[0].keys()
airbnb_listing = pd.DataFrame(results, columns=column_names)
airbnb_listing.to_csv("airbnb_listing.csv", index=False)
airbnb_listing.head()


# ### Insert into Host_information

# In[55]:


connection.execute("DROP TABLE IF EXISTS host_info")
                   
stmt = """
CREATE TABLE host_info (
    host_id INT PRIMARY KEY,
    location VARCHAR(255),
    description VARCHAR,
    host_since DATE,
    response_time VARCHAR(50),
    response_rate DECIMAL(5, 2),
    acceptance_rate DECIMAL(5, 2),
    verification VARCHAR(255)
);
    """
connection.execute(stmt)


# In[58]:


host_df = pd.read_csv('host.csv', encoding='latin-1')
host_df.to_sql(name='host_info', con=engine, if_exists='append', index=False)


# In[84]:


results = connection.execute("SELECT * from host_info").fetchall()
column_names = results[0].keys()
host_info = pd.DataFrame(results, columns=column_names)
host_info.to_csv("host_info.csv", index=False)
host_info.head()


# ### Insert into Listing_detail

# In[87]:


connection.execute("DROP TABLE IF EXISTS listing_detail")
                   
stmt = """
CREATE TABLE listing_detail (
    listing_id BIGINT PRIMARY KEY,
    host_id INT,
    neighbourhood VARCHAR(50),
    listing_url VARCHAR(50),
    name VARCHAR,
    description VARCHAR,
    room_type VARCHAR(50),
    property_type VARCHAR(50)
);
    """
connection.execute(stmt)


# In[88]:


listingdetail_df = pd.read_csv('listing_details.csv', encoding='latin-1')
listingdetail_df.to_sql(name='listing_detail', con=engine, if_exists='append', index=False)


# In[85]:


results = connection.execute("SELECT * from listing_detail").fetchall()
column_names = results[0].keys()
listing_detail = pd.DataFrame(results, columns=column_names)
listing_detail.to_csv("listing_detail.csv", index=False)
listing_detail.head()


# ### Insert into Listing_review

# In[111]:


connection.execute("DROP TABLE IF EXISTS listing_review")
                   
stmt = """
CREATE TABLE listing_review (
    review_id BIGINT PRIMARY KEY, 
    reviewer_id BIGINT,
    listing_id BIGINT,
    date DATE,
    reviewer_name VARCHAR,
    comments VARCHAR,
    FOREIGN KEY (listing_id) REFERENCES listing_detail(listing_id)
);
    """
connection.execute(stmt)


# In[113]:


listing_review_df = pd.read_csv('reviews_details.csv', encoding='latin-1')
listing_review_df = listing_review_df.drop_duplicates(subset=['review_id'])


# In[114]:


listing_review_df.to_sql(name='listing_review', con=engine, if_exists='append', index=False)


# In[86]:


results = connection.execute("SELECT * from listing_review").fetchall()
column_names = results[0].keys()
listing_review = pd.DataFrame(results, columns=column_names)
listing_review.to_csv("listing_review.csv", index=False)
listing_review.head()


# In[87]:


connection.close()

