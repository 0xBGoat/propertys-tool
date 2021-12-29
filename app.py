import requests
import json
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np

# Setup config
st.set_page_config(layout = 'wide')

with st.sidebar:
    st.title("Property's Virtual Realty")
    reportType = st.selectbox('Report Type', ['Overview','Owner','City','District','Street'])

# Create the base DataFrame
df = pd.read_json('properties.json')
df['ownerNameLower'] = df['ownerName'].str.lower()
df_simple = df[['ownerAddress', 'ownerName', 'city', 'district', 'street']]

# TODO: Figure out a more efficient way to do this
# Create a GroupByDataFrame grouped by ownerAddress and ownerName for streets
gbOwnerStreet = df.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district', 'street'])
dfOwnerStreet = gbOwnerStreet.size().reset_index(name = 'propertyCount')
dfOwnerStreet['streetCount'] = np.floor_divide(dfOwnerStreet['propertyCount'], 7)

# Now group at the district level
gbOwnerDistrict = dfOwnerStreet.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district'])
dfOwnerDistrict = gbOwnerDistrict.streetCount.agg(sum).reset_index(name = 'streetsInDistrict')
dfOwnerDistrict['districtCount'] = np.floor_divide(dfOwnerDistrict['streetsInDistrict'], 3)

# Now group at the city level
gbOwnerCity = dfOwnerDistrict.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city'])
dfOwnerCity = gbOwnerCity.districtCount.agg(sum).reset_index(name = 'districtsInCity')
dfOwnerCity['cityCount'] = np.floor_divide(dfOwnerCity['districtsInCity'], 3)

# Create a DataFrame of the top 10 owners
dfTopOwners = df.groupby(['ownerAddress', 'ownerName']) \
    .size() \
    .reset_index(name = 'count') \
    .sort_values(by = 'count', ascending = False) \
    .head(10)

dfTopStreetOwners = dfOwnerStreet.groupby(['ownerAddress','ownerName']) \
    .streetCount \
    .agg(sum) \
    .reset_index(name = 'count') \
    .sort_values(by = 'count', ascending = False) \
    .head(10)

dfTopDistrictOwners = dfOwnerDistrict.groupby(['ownerAddress','ownerName']) \
    .districtCount \
    .agg(sum) \
    .reset_index(name = 'count') \
    .sort_values(by = 'count', ascending = False) \
    .head(10)

propsByOwner = df.groupby(['ownerAddress', 'ownerName'])['street']

def buildOverview():
    st.title('Overview')

    numPureStreets = dfOwnerStreet['streetCount'].sum()
    numDistricts = dfOwnerDistrict['districtCount'].sum()
    numCities = dfOwnerCity['cityCount'].sum()
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(label="Unique Owners", value = len(propsByOwner))
        with col2:
            st.metric(label='Pure Streets', value = f"{numPureStreets} / 840")
        with col3:
            st.metric(label='Districts', value = f"{numDistricts} / 280")
        with col4:
            st.metric(label="Cities", value = f"{numCities} / 70")

    with st.container():
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader('🏠 Top Property Owners')
            dfTopOwners.index = pd.RangeIndex(start=1, stop=11, step=1)
            st.table(dfTopOwners[['ownerName','count']])
        with col2:
            st.subheader('🛣️ Top Street Owners')
            dfTopStreetOwners.index = pd.RangeIndex(start=1, stop=11, step=1)
            st.table(dfTopStreetOwners[['ownerName', 'count']])
        with col3:
            st.subheader('🏘️ Top District Owners')
            dfTopDistrictOwners.index = pd.RangeIndex(start=1, stop=11, step=1)
            st.table(dfTopDistrictOwners[['ownerName', 'count']])

    with st.expander(label = "See All Properties"): 
        st.write(df_simple) 

def buildOwnerReport():
    with st.form(key='owner_form'):
        with st.sidebar:
            ownerName = st.text_input('Owner Name or Address')
            ownerNameLower = ownerName.lower()
            ownerSubmit = st.form_submit_button(label='Submit')

    if ownerSubmit:
        st.title(f"Owner Report - {ownerName}")

        if ownerName is not None:
            ownerLabel = 'ownerAddress' if ownerName.startswith('0x') else 'ownerNameLower'

            dfOwner = df.loc[df[ownerLabel]==ownerNameLower][['ownerAddress','ownerName','city','district','street']]
            streetsOwned = dfOwnerStreet.loc[dfOwnerStreet[ownerLabel]==ownerNameLower].streetCount.sum()
            districtsOwned = dfOwnerDistrict.loc[dfOwnerDistrict[ownerLabel]==ownerNameLower].districtCount.sum()
            citiesOwned = dfOwnerCity.loc[dfOwnerCity[ownerLabel]==ownerNameLower].cityCount.sum()

            with st.container():
                col1, col2, col3, col4 = st.columns(4)
            
                with col1:
                    st.metric(label='Properties Owned', value=f"🏠 {len(dfOwner)}")
        
                with col2:
                    st.metric(label='Streets Owned', value=f"🛣️ {streetsOwned}")

                with col3:
                    st.metric(label='Districts Owned', value=f"🏘️ {districtsOwned}")

                with col4:
                    st.metric(label='Cities Owned', value=f"🏙️ {citiesOwned}")

                with st.expander(label="See all owner data"):
                    st.write(dfOwner)

if reportType == 'Overview':
    buildOverview()
elif reportType == 'Owner':
    buildOwnerReport()  
elif reportType == 'City':
    st.title('Coming soon...')
elif reportType == 'District':
    st.title('Coming soon...')
else:
    st.title('Coming soon...')
