import requests, json, urllib.parse, pathlib, datetime, s3fs
import streamlit as st
import pandas as pd
import numpy as np
from web3 import Web3

# Setup config and sidebar
st.set_page_config(
    page_title="Property's",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items = {
        'About': "#### A tool to help users make buying/selling decisions around their Property's Virtual Realty NFTs"
    }
)

st.markdown(
    """
        <style>
            @font-face {
                font-family: 'ARCO';
                font-style: normal;
                font-weight: 700;
                src: local('ARCO'), url('https://fonts.cdnfonts.com/s/23602/ARCO for OSX.woff') format('woff');
            }

            h1, h2 {
                font-family: 'ARCO';
                color: #8f00ff;
            }
        </style>

    """,
    unsafe_allow_html=True,
)

@st.experimental_memo(ttl=300)
def loadData():
    df = pd.read_json(
        's3://propertys-opensea/properties.json',
        storage_options={'key': st.secrets['AWS_ACCESS_KEY_ID'], 'secret': st.secrets['AWS_SECRET_ACCESS_KEY']}
    )
    
    # Add a lowercased owner name column for easier lookups
    df['ownerNameLower'] = df['ownerName'].str.lower()

    return df

df = loadData()

@st.experimental_memo(ttl=60)
def getDataFrames():
    dfSimple = df[['ownerAddress', 'ownerName', 'city', 'district', 'street', 'numSales', 'lastSale', 'salePrice']]

    # TODO: Figure out a more efficient way to do this
    # Street level grouping
    gbOwnerStreet = df.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district', 'street'])
    dfOwnerStreet = gbOwnerStreet.size().reset_index(name='propertyCount')
    dfOwnerStreet['streetCount'] = np.floor_divide(dfOwnerStreet['propertyCount'], 7)

    # District level grouping
    gbOwnerDistrict = dfOwnerStreet.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district'])
    dfOwnerDistrict = gbOwnerDistrict.streetCount.agg(sum).reset_index(name='streetsInDistrict')
    dfOwnerDistrict['districtCount'] = np.floor_divide(dfOwnerDistrict['streetsInDistrict'], 3)

    # City level grouping
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

    dfTopCityOwners = dfOwnerCity.groupby(['ownerAddress','ownerName']) \
        .cityCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    dfTopCityOwners.index = pd.RangeIndex(start=1, stop=11, step=1)

    return {
        'all': df,
        'simple': dfSimple,
        'ownerStreet': dfOwnerStreet,
        'ownerDistrict': dfOwnerDistrict,
        'ownerCity': dfOwnerCity,
        'topOwners': dfTopOwners,
        'topStreetOwners': dfTopStreetOwners,
        'topDistrictOwners': dfTopDistrictOwners,
        'topCityOwners': dfTopCityOwners
    }

def renderOverview():
    st.title('Overview')

    frames = getDataFrames()
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)

        numStreets = frames['ownerStreet'].streetCount.sum()
        numDistricts = frames['ownerDistrict'].districtCount.sum()
        numCities = frames['ownerCity'].cityCount.sum()

        dfAvailableStreets = df.groupby(['city', 'district', 'street'])['salePrice'].apply(lambda x: x.sort_values().head(7).sum() if x.count() > 6 else None).to_frame().dropna().sort_values(by='salePrice').reset_index()

        with col1:
            st.metric(label='Unique Owners', value=f"👥 {len(df.groupby('ownerAddress'))}")
        with col2:
            st.metric(label='Pure Streets', value=f"🛣️ {numStreets} / 840")
        with col3:
            st.metric(label='Districts', value=f"🏘️ {numDistricts} / 280")
        with col4:
            st.metric(label="Cities", value=f"🏙️ {numCities} / 70")

    with st.container():
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.subheader('Top Property Owners')
            st.table(frames['topOwners'][['ownerName','count']])
        with col2:
            st.subheader('Top Street Owners')
            st.table(frames['topStreetOwners'][['ownerName', 'count']])
        with col3:
            st.subheader('Top District Owners')
            st.table(frames['topDistrictOwners'][['ownerName', 'count']])
        with col4:
            st.subheader('Top City Owners')
            st.table(frames['topCityOwners'][['ownerName', 'count']])
    
    with st.container():
        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader('Cheapest Streets')
            st.write(dfAvailableStreets)

        with col2:
            st.subheader('Raw Data')
            with st.expander(label = "See All Properties", expanded=True): 
                st.write(frames['simple'].sort_values(by='street'))

def renderOwnerReport(ownerName):   
    if ownerName != '':
        st.title(f'Owner Report - {ownerName}')
    else:
        st.title('Owner Report')

    ownerNameLower = ownerName.lower()

    frames = getDataFrames()
    dfOwnerStreet = frames['ownerStreet']
    dfOwnerDistrict = frames['ownerDistrict']
    dfOwnerCity = frames['ownerCity']

    if ownerName is not None:
        ownerLabel = 'ownerAddress' if ownerNameLower.startswith('0x') else 'ownerNameLower'

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
            
        with st.expander(label="See all owner data", expanded=True):
            st.write(dfOwner.sort_values(by='street').reset_index(drop=True))

def renderStreetReport(streetName):
    st.title(f'Street Report - {streetName}')
    
    frames = getDataFrames()
    df = frames['all']
    dfStreet = df.loc[df['street']==streetName]
    dfOwnerStreet = frames['ownerStreet']

    imageUrl = dfStreet['imagePreviewUrl'].values[0]
    floorPrice = dfStreet['salePrice'].min() if dfStreet['salePrice'].min() > 0 else 'N/A'
    prices = dfStreet['salePrice'].dropna()
    fullStreetAvailable = len(prices) > 6
    fullStreetPrice = 'N/A'

    if fullStreetAvailable:
        fullStreetPrice = f'{prices.sort_values().head(7).sum():.2f}'    

    dfOwnerStreetFiltered = dfOwnerStreet.loc[dfOwnerStreet['street']==streetName] \
        .sort_values(by='propertyCount', ascending=False).reset_index(drop=True)
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        streetsCompleted = dfOwnerStreetFiltered.streetCount.sum()
        streetOwnerCount = len(dfOwnerStreetFiltered.loc[dfOwnerStreetFiltered['streetCount']>0])

        with col1:
            st.image(imageUrl, use_column_width='auto')
        with col2:
            st.metric(label='Pure Streets', value=f'🛣️ {streetsCompleted} / 10')
            st.metric(label='Street Owners', value=f'👥 {streetOwnerCount}')
            st.metric(label='Floor Price', value=f'💹 {floorPrice}')
        with col3:
            st.metric(label='Cheapest Full Street', value=f'🏷️ {fullStreetPrice}')
        
    with st.expander(label="See all street data", expanded=True):
        st.write(dfOwnerStreetFiltered[['ownerAddress','ownerName','propertyCount','streetCount']])

def initializeApplication():
    reportChoiceKey = 'reportChoice'
    streetChoiceKey = 'streetChoice'
    ownerInputKey = 'ownerInput'

    reportOptions = ['overview', 'street', 'owner']
    streetOptions = df['street'].drop_duplicates().sort_values().to_list()
    
    queryParams = st.experimental_get_query_params()
    queryReportChoice = queryParams['report'][0] if 'report' in queryParams else None
    queryStreetChoice = queryParams['street'][0] if 'street' in queryParams else None
    queryOwnerInput = queryParams['owner'][0] if 'owner' in queryParams else None

    st.session_state[reportChoiceKey] = queryReportChoice if queryReportChoice in reportOptions else reportOptions[0]
    st.session_state[streetChoiceKey] = queryStreetChoice if queryStreetChoice in streetOptions else streetOptions[0]
    st.session_state[ownerInputKey] = queryOwnerInput if queryOwnerInput is not None else ''

    def updateSessionState():
        queryParams = st.experimental_get_query_params()

        reportChoice = st.session_state[reportChoiceKey]
        queryParams['report'] = reportChoice     

        if reportChoice == 'street':
            queryParams['street'] = st.session_state[streetChoiceKey]
            
            if 'owner' in queryParams:
                queryParams.pop('owner')
        elif reportChoice == 'owner':
            queryParams['owner'] = st.session_state[ownerInputKey]
            
            if 'street' in queryParams:
                queryParams.pop('street')
        else:
            if 'owner' in queryParams:
                queryParams.pop('owner')

            if 'street' in queryParams:
                queryParams.pop('street')               

        st.experimental_set_query_params(**queryParams)

    with st.sidebar:
        st.title("Property's Virtual Realty")
        st.selectbox('Select a Report Type', reportOptions, on_change=updateSessionState, key=reportChoiceKey, format_func=lambda x: x.title())

    reportChoice = st.session_state[reportChoiceKey]

    if reportChoice == 'overview':
        renderOverview()
    elif reportChoice == 'street':
        with st.form(key='street_form'):
            with st.sidebar:
                st.selectbox(label='Select a street (or start typing)', options=streetOptions, key=streetChoiceKey)  
                st.form_submit_button(label='Submit', on_click=updateSessionState)
        
        renderStreetReport(st.session_state[streetChoiceKey])
    elif reportChoice == 'owner':
        ownerInput = st.session_state[ownerInputKey]

        with st.form(key='owner_form'):
            with st.sidebar:
                st.text_input('Owner Name or Address', key=ownerInputKey)
                ownerSubmit = st.form_submit_button(label='Submit', on_click=updateSessionState)

        if ownerSubmit or ownerInput != '':
            renderOwnerReport(ownerInput)
        else:
            st.title('Owner Report')

initializeApplication()
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) 
# asyncio.run(main()) 
