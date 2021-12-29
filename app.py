import requests, json
import streamlit as st
import pandas as pd
import numpy as np

# Setup config and sidebar
st.set_page_config(layout='wide')

with st.sidebar:
    st.title("Property's Virtual Realty")
    reportType = st.selectbox('Report Type', ['Overview','Owner','City','District','Street'])

# Load data from disk and memoize it
@st.experimental_memo
def loadData():
    df = pd.read_json('properties.json')
    
    # Add a lowercased owner name column for easier lookups
    df['ownerNameLower'] = df['ownerName'].str.lower()

    return df

@st.experimental_memo
def getDataFrames():
    dfSimple = df[['ownerAddress', 'ownerName', 'city', 'district', 'street']]

    # TODO: Figure out a more efficient way to do this
    # Create a GroupByDataFrame grouped by ownerAddress and ownerName for streets
    gbOwnerStreet = df.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district', 'street'])
    dfOwnerStreet = gbOwnerStreet.size().reset_index(name='propertyCount')
    dfOwnerStreet['streetCount'] = np.floor_divide(dfOwnerStreet['propertyCount'], 7)

    # Now group at the district level
    gbOwnerDistrict = dfOwnerStreet.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district'])
    dfOwnerDistrict = gbOwnerDistrict.streetCount.agg(sum).reset_index(name='streetsInDistrict')
    dfOwnerDistrict['districtCount'] = np.floor_divide(dfOwnerDistrict['streetsInDistrict'], 3)

    # Now group at the city level
    gbOwnerCity = dfOwnerDistrict.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city'])
    dfOwnerCity = gbOwnerCity.districtCount.agg(sum).reset_index(name='districtsInCity')
    dfOwnerCity['cityCount'] = np.floor_divide(dfOwnerCity['districtsInCity'], 3)   

    # Create top 10 dataframes (not worth doing City owners yet)
    dfTopOwners = df.groupby(['ownerAddress', 'ownerName']) \
        .size().reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    dfTopOwners.index = pd.RangeIndex(start=1, stop=11, step=1)

    dfTopStreetOwners = dfOwnerStreet.groupby(['ownerAddress','ownerName']) \
        .streetCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    dfTopStreetOwners.index = pd.RangeIndex(start=1, stop=11, step=1)

    dfTopDistrictOwners = dfOwnerDistrict.groupby(['ownerAddress','ownerName']) \
        .districtCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    dfTopDistrictOwners.index = pd.RangeIndex(start=1, stop=11, step=1)

    return {
        'simple': dfSimple,
        'ownerStreet': dfOwnerStreet,
        'ownerDistrict': dfOwnerDistrict,
        'ownerCity': dfOwnerCity,
        'topOwners': dfTopOwners,
        'topStreetOwners': dfTopStreetOwners,
        'topDistrictOwners': dfTopDistrictOwners
    }

def renderOverview():
    st.title('Overview')

    frames = getDataFrames()
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)

        numStreets = frames['ownerStreet'].streetCount.sum()
        numDistricts = frames['ownerDistrict'].districtCount.sum()
        numCities = frames['ownerCity'].cityCount.sum()

        with col1:
            st.metric(label='Unique Owners', value=len(df.groupby('ownerAddress')))
        with col2:
            st.metric(label='Pure Streets', value=f"{numStreets} / 840")
        with col3:
            st.metric(label='Districts', value=f"{numDistricts} / 280")
        with col4:
            st.metric(label="Cities", value=f"{numCities} / 70")

    with st.container():
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader('🏠 Top Property Owners')
            st.table(frames['topOwners'][['ownerName','count']])
        with col2:
            st.subheader('🛣️ Top Street Owners')
            st.table(frames['topStreetOwners'][['ownerName', 'count']])
        with col3:
            st.subheader('🏘️ Top District Owners')
            st.table(frames['topDistrictOwners'][['ownerName', 'count']])

    with st.expander(label = "See All Properties"): 
        st.write(frames['simple']) 

def renderOwnerReport():   
    with st.form(key='owner_form'):
        with st.sidebar:
            ownerName = st.text_input('Owner Name or Address')
            ownerNameLower = ownerName.lower()
            ownerSubmit = st.form_submit_button(label='Submit')

    if ownerSubmit:
        st.title(f"Owner Report - {ownerName}")

        frames = getDataFrames()
        dfOwnerStreet = frames['ownerStreet']
        dfOwnerDistrict = frames['ownerDistrict']
        dfOwnerCity = frames['ownerCity']

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

df = loadData()

if reportType == 'Overview':
    renderOverview()
elif reportType == 'Owner':
    renderOwnerReport()  
elif reportType == 'City':
    st.title('Coming soon...')
elif reportType == 'District':
    st.title('Coming soon...')
else:
    st.title('Coming soon...')
