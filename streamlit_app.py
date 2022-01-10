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

            tbody th {
                display:none
            }

            .row_heading.level0 {
                display:none
            }
            
            .blank {
                display:none
            }
        </style>

    """,
    unsafe_allow_html=True,
)

WETH_PAYMENT_TOKEN = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

PROP_BRIX_DICT = {
    'Beige Bay': {'house': 10, 'street': 370, 'district': 1470, 'city': 9050},
    'Orange Oasis': {'house': 20, 'street': 490, 'district': 1790, 'city': 11550},
    'Yellow Yards': {'house': 30, 'street': 610, 'district': 2110, 'city': 11520},
    'Green Grove': {'house': 40, 'street': 730, 'district': 2430, 'city': 13560},
    'Purple Palms': {'house': 50, 'street': 850, 'district': 2750, 'city': 15600},
    'Blue Bayside': {'house': 60, 'street': 970, 'district': 3070, 'city': 13730},
    'X AE X-II': {'house': 80, 'street': 1210, 'district': 4110, 'city': 19990}
}

SPECIAL_BRIX_DICT = {
    'Casa Blanca': 250,
    'Mystical Rocks': 250,
    'Spiky Singers': 250,
    'The Guardian': 250,
    'Candy Castle': 600,
    'Cathedral of Wisdom': 600,
    'Palace of Eternity': 600,
    "Peter's Great Wall": 600,
    "Property's Stadium": 600,
    'The Impossible Bridge': 600,
    'Ancient Labyrinth': 1200,
    'Fort in the Leaves': 1200,
    'Great Temple of Peter': 1200,
    'Le Tower': 1200,
    'Mount Proper': 1200,
    'Question Cat': 1200,
    'The Emperors Arena': 1200,
    'The Money Pool': 1200,
    'The Secret Glass Pyramid': 1200,
    'The Sunken City': 1200
}

def make_clickable(url, text):
    return f'<a target="_blank" href="{url}">{text}</a>'

@st.experimental_memo(ttl=300)
def load_data():
    df = pd.read_json(
        's3://propertys-opensea/properties.json',
        storage_options={'key': st.secrets['AWS_ACCESS_KEY_ID'], 'secret': st.secrets['AWS_SECRET_ACCESS_KEY']}
    )
    
    values = {"ownerName": df['ownerAddress']}
    df.fillna(value=values, inplace=True)

    # Add a lowercased owner name column for easier lookups
    df['ownerNameLower'] = df['ownerName'].str.lower()

    return df

df = load_data()

@st.experimental_memo(ttl=60)
def get_data_frames():
    df_simple = df[['ownerAddress', 'ownerName', 'city', 'district', 'street', 'numSales', 'lastSale', 'salePrice']]

    # TODO: Figure out a more efficient way to do this
    # Street level grouping
    gb_owner_street = df.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district', 'street'], dropna=False)
    df_owner_street = gb_owner_street.size().reset_index(name='propertyCount')
    df_owner_street['streetCount'] = np.floor_divide(df_owner_street['propertyCount'], 7)

    # District level grouping
    gb_owner_district = df_owner_street.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city', 'district'], dropna=False)
    df_owner_district = gb_owner_district.streetCount.agg(sum).reset_index(name='streetsInDistrict')
    df_owner_district['districtCount'] = np.floor_divide(df_owner_district['streetsInDistrict'], 3)

    # City level grouping
    gb_owner_city = df_owner_district.groupby(['ownerAddress', 'ownerName', 'ownerNameLower', 'city'], dropna=False)
    df_owner_city = gb_owner_city.districtCount.agg(sum).reset_index(name='districtsInCity')
    df_owner_city['cityCount'] = np.floor_divide(df_owner_city['districtsInCity'], 3)   

    # Create top 10 dataframes (not worth doing City owners yet)
    df_top_owners = df.groupby(['ownerAddress', 'ownerName'], dropna=False) \
        .size().reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    df_top_owners.index = pd.RangeIndex(start=1, stop=11, step=1)

    df_top_street_owners = df_owner_street.groupby(['ownerAddress','ownerName'], dropna=False) \
        .streetCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    df_top_street_owners.index = pd.RangeIndex(start=1, stop=11, step=1)

    df_top_district_owners = df_owner_district.groupby(['ownerAddress','ownerName'], dropna=False) \
        .districtCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    df_top_district_owners.index = pd.RangeIndex(start=1, stop=11, step=1)

    df_top_city_owners = df_owner_city.groupby(['ownerAddress','ownerName'], dropna=False) \
        .cityCount.agg(sum).reset_index(name='count').sort_values(by='count', ascending=False).head(10)
    df_top_city_owners.index = pd.RangeIndex(start=1, stop=11, step=1)

    return {
        'all': df,
        'simple': df_simple,
        'ownerStreet': df_owner_street,
        'ownerDistrict': df_owner_district,
        'ownerCity': df_owner_city,
        'topOwners': df_top_owners,
        'topStreetOwners': df_top_street_owners,
        'topDistrictOwners': df_top_district_owners,
        'topCityOwners': df_top_city_owners.loc[df_top_city_owners['count']>0]
    }

def render_overview():
    st.title('Overview')

    frames = get_data_frames()

    df_buy_now_properties = df.where((df['salePrice'] > 0) & (df['paymentToken'] != WETH_PAYMENT_TOKEN))
    df_available_streets = df_buy_now_properties.groupby(['city', 'district', 'street'])['salePrice'] \
            .apply(lambda x: x.sort_values().head(7).sum() if x.count() > 6 else None) \
            .to_frame().dropna().sort_values(by='salePrice').reset_index()

    df_available_streets = df_available_streets[df_available_streets.city != 'Special']
    df_available_streets['brixYield'] = df_available_streets \
        .apply(lambda x: PROP_BRIX_DICT[str(x['city']).strip()]['street'], axis=1)
    df_available_streets['brix/eth'] = df_available_streets['brixYield'] / df_available_streets['salePrice']
    df_available_streets = df_available_streets.round({'salePrice': 2, 'brix/eth': 2})
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)

        num_streets = frames['ownerStreet'].streetCount.sum()
        num_districts = frames['ownerDistrict'].districtCount.sum()
        num_cities = frames['ownerCity'].cityCount.sum()

        with col1:
            st.metric(label='Unique Owners', value=f"👥 {len(df.groupby('ownerAddress'))}")
        with col2:
            st.metric(label='Pure Streets', value=f"🛣️ {num_streets} / 840")
        with col3:
            st.metric(label='Districts', value=f"🏘️ {num_districts} / 280")
        with col4:
            st.metric(label="Cities", value=f"🏙️ {num_cities} / 70")

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
            st.subheader('🏷️ Cheapest Streets')
            st.write(df_available_streets)

        with col2:
            st.subheader('📋 Raw Data')
            st.write(frames['simple'].sort_values(by='street'))

    with st.container():
        st.subheader('Release Notes')

        with st.expander(label="Click to expand"):
            st.subheader('v0.2.2')
            st.markdown("""
                    ##### 🐞 Bug Fixes
                    * Fixed issue with auctions affecting cheapest street prices and removed them from market listings
                """,
                unsafe_allow_html=True
            )

            st.subheader('v0.2.1')
            st.markdown("""
                    ##### 🐞 Bug Fixes
                    * Fixed a bug that was excluding owners with empty OpenSea usernames from calculations/reports
                       * The owner address will be used as the owner name for these users
                    * Fixed issue where non-city owners were showing up in the Top City Owners section
                """,
                unsafe_allow_html=True
            )

            st.subheader('v0.2.0')
            st.markdown("""
                    ##### ⭐ New Features
                    * **Market Listings for Streets** - added current market listings to the Street reports with links to view on OpenSea
                    <br><br>
                    ##### ✔️ Other Changes
                    * Stopped putting expand/collapse containers around some of the tabular data sections
                """,
                unsafe_allow_html=True
            )

            st.subheader('v0.1.0')
            st.markdown("""
                    ##### 📔 Notes
                    * Adding a release notes section
                    * The tool provides these initial report types:
                       * **Overview** - metrics and information about to the entire NFT collection
                       * **Street Report** - metrics and information about specific streets
                       * **Owner Report** - metrics and information about a specific property owner  
                    <br>
                    ##### ⭐ New Features
                    * **Cheapest Streets** - a new section on the Overview report that identifies the 
                            cheapest available streets with at least 7 NFTs listed on OpenSea (does not account for bundles)
                    * The table also includes a **BRIX-to-ETH** ratio to help identify the most cost-effective BRIX earning opportunities
                    * Added the cheapest street price and $BRIX generation information to the individual street reports
                """,
                unsafe_allow_html=True
            )


def render_owner_report(owner_name):   
    if owner_name != '':
        st.title(f'Owner Report - {owner_name}')
    else:
        st.title('Owner Report')

    owner_name_lower = owner_name.lower()

    frames = get_data_frames()
    df_owner_street = frames['ownerStreet']
    df_owner_district = frames['ownerDistrict']
    df_owner_city = frames['ownerCity']

    if owner_name is not None:
        owner_label = 'ownerAddress' if owner_name_lower.startswith('0x') else 'ownerNameLower'

        df_owner = df.loc[df[owner_label]==owner_name_lower][['ownerAddress','ownerName','city','district','street']]
        streets_owned = df_owner_street.loc[df_owner_street[owner_label]==owner_name_lower].streetCount.sum()
        districts_owned = df_owner_district.loc[df_owner_district[owner_label]==owner_name_lower].districtCount.sum()
        cities_owned = df_owner_city.loc[df_owner_city[owner_label]==owner_name_lower].cityCount.sum()

        with st.container():
            col1, col2, col3, col4 = st.columns(4)
        
            with col1:
                st.metric(label='Properties Owned', value=f"🏠 {len(df_owner)}")        
            with col2:
                st.metric(label='Streets Owned', value=f"🛣️ {streets_owned}")
            with col3:
                st.metric(label='Districts Owned', value=f"🏘️ {districts_owned}")
            with col4:
                st.metric(label='Cities Owned', value=f"🏙️ {cities_owned}")
            
        st.subheader('📋 Property Holdings')
        st.write(df_owner.sort_values(by='street').reset_index(drop=True))

def render_street_report(street_name):
    st.title(f'Street Report - {street_name}')

    frames = get_data_frames()
    
    df = frames['all']
    df_owner_street = frames['ownerStreet']

    df_street = df.loc[df['street']==street_name]
    city_name = df_street.iloc[0].city.strip()
    image_url = df_street['imagePreviewUrl'].values[0]

    listings = df_street.loc[(df_street['salePrice'] > 0) & (df_street['paymentToken'] != WETH_PAYMENT_TOKEN)].sort_values(by='salePrice').fillna('Mint')
    floor_price = df_street['salePrice'].min() if df_street['salePrice'].min() > 0 else 'N/A'
    prices = df_street['salePrice']
    full_street_price = f'{prices.sort_values().head(7).sum():.2f}' if len(listings) > 6 else 'N/A'    

    df_owner_street_filtered = df_owner_street.loc[df_owner_street['street']==street_name] \
        .sort_values(by='propertyCount', ascending=False).reset_index(drop=True)
    
    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        streets_completed = df_owner_street_filtered.streetCount.sum()
        street_owner_count = len(df_owner_street_filtered.loc[df_owner_street_filtered['streetCount']>0])

        with col1:
            st.image(image_url, use_column_width='auto')
        with col2:
            if city_name != 'Special':
                st.metric(label='Pure Streets', value=f'🛣️ {streets_completed} / 10')
                st.metric(label='Street Owners', value=f'👥 {street_owner_count}')
            else:
                st.metric(label='$BRIX per Special', value=f'🧱 {SPECIAL_BRIX_DICT[street_name]}')

            st.metric(label='Floor Price', value=f'Ξ {floor_price}')
        with col3:
            if city_name != 'Special':
                st.metric(label='Cheapest Full Street', value=f'🏷️ {full_street_price}')
                st.metric(label='$BRIX per House', value=f"🧱 {PROP_BRIX_DICT[city_name]['house']}")
                st.metric(label='$BRIX per Street', value=f"🧱 {PROP_BRIX_DICT[city_name]['street']}")
        
    with st.container():
        col1, col2 = st.columns(2)

        with col1:
            st.subheader('📋 Owner Data')
            st.write(df_owner_street_filtered[['ownerAddress','ownerName','propertyCount','streetCount']])
        
        with col2:
            st.subheader('🛒 Market Listings')

            if len(listings) > 0:
                listings = listings[['ownerAddress', 'ownerName', 'salePrice', 'lastSale', 'osLink']]
                listings['osLink'] = listings['osLink'].apply(make_clickable, args=('View on OpenSea',))
                st.write(listings.to_html(escape=False, render_links=True, index=False), unsafe_allow_html=True)
            else:
                st.subheader('No Listings!')

def init():
    report_choice_key = 'reportChoice'
    street_choice_key = 'streetChoice'
    owner_input_key = 'ownerInput'

    report_options = ['overview', 'street', 'owner']
    street_options = df['street'].drop_duplicates().sort_values().to_list()
    
    query_params = st.experimental_get_query_params()
    query_report_choice = query_params['report'][0] if 'report' in query_params else None
    query_street_choice = query_params['street'][0] if 'street' in query_params else None
    query_owner_input = query_params['owner'][0] if 'owner' in query_params else None

    st.session_state[report_choice_key] = query_report_choice if query_report_choice in report_options else report_options[0]
    st.session_state[street_choice_key] = query_street_choice if query_street_choice in street_options else street_options[0]
    st.session_state[owner_input_key] = query_owner_input if query_owner_input is not None else ''

    def update_session_state():
        query_params = st.experimental_get_query_params()

        report_choice = st.session_state[report_choice_key]
        query_params['report'] = report_choice     

        if report_choice == 'street':
            query_params['street'] = st.session_state[street_choice_key]
            
            if 'owner' in query_params:
                query_params.pop('owner')
        elif report_choice == 'owner':
            query_params['owner'] = st.session_state[owner_input_key]
            
            if 'street' in query_params:
                query_params.pop('street')
        else:
            if 'owner' in query_params:
                query_params.pop('owner')

            if 'street' in query_params:
                query_params.pop('street')               

        st.experimental_set_query_params(**query_params)

    with st.sidebar:
        st.title("Property's Virtual Realty Assistant")
        st.caption('v0.2.2')
        st.selectbox('Select a Report Type', report_options, on_change=update_session_state, key=report_choice_key, format_func=lambda x: x.title())

    report_choice = st.session_state[report_choice_key]

    if report_choice == 'overview':
        render_overview()
    elif report_choice == 'street':
        with st.form(key='street_form'):
            with st.sidebar:
                st.selectbox(label='Select a street (or start typing)', options=street_options, key=street_choice_key)  
                st.form_submit_button(label='Submit', on_click=update_session_state)
        
        render_street_report(st.session_state[street_choice_key])
    elif report_choice == 'owner':
        owner_input = st.session_state[owner_input_key]

        with st.form(key='owner_form'):
            with st.sidebar:
                st.text_input('Owner Name or Address', key=owner_input_key)
                owner_submit = st.form_submit_button(label='Submit', on_click=update_session_state)

        if owner_submit or owner_input != '':
            render_owner_report(owner_input)
        else:
            st.title('Owner Report')

init()
