import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# Page configuration
tabs = st.tabs(["Sprint 3", "Sprint 4"])


# Firebase initialization
@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        key_dict = dict(st.secrets["firebase"])
        if isinstance(key_dict.get('private_key'), str):
            key_dict['private_key'] = key_dict['private_key'].replace('\\n', '\n')
        try:
            cred = credentials.Certificate(key_dict)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Firebase initialization error: {str(e)}")
            return None
    return firestore.client()

with tabs[0]:
    st.title("📊 Sprint 3 Dashboard")
    # QR Data fetching
    @st.cache_data(ttl=300)
    def get_qr_data():
        try:
            db = init_firebase()
            if db is None:
                return pd.DataFrame()
                
            qr_times_ref = db.collection('AnalyticsBusinessQuestions/sprint2/businessQuestionQR')
            docs = qr_times_ref.stream()
            
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                data.append({
                    'user_id': doc_data.get('userId', 'unknown'),
                    'render_time': doc_data.get('qr_rtime', 0),
                    'timestamp': doc_data.get('timestamp', datetime.now())
                })
            
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Error fetching QR data: {str(e)}")
            return pd.DataFrame()

    # Loyalty Cards Data fetching
    @st.cache_data(ttl=300)
    def get_loyalty_data():
        try:
            db = init_firebase()
            if db is None:
                return pd.DataFrame()

            # Get loyalty cards data
            loyalty_cards = db.collection('loyaltyCards').stream()
            loyalty_data = [{'storeId': card.to_dict()['storeId']} for card in loyalty_cards]
            loyalty_df = pd.DataFrame(loyalty_data)

            # Get store details
            store_ids = loyalty_df['storeId'].unique()
            stores_data = []
            for store_id in store_ids:
                store_doc = db.collection('stores').document(store_id).get()
                if store_doc.exists:
                    store_data = store_doc.to_dict()
                    store_data['storeId'] = store_id
                    stores_data.append(store_data)

            stores_df = pd.DataFrame(stores_data)
            
            # Merge data
            merged_df = pd.merge(loyalty_df, stores_df[['storeId', 'name']], on='storeId', how='left')
            return merged_df
        except Exception as e:
            st.error(f"Error fetching loyalty data: {str(e)}")
            return pd.DataFrame()

    # Main title
    st.markdown("---")

    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        target_time = st.slider(
            "Target Time (ms)", 
            min_value=50, 
            max_value=200, 
            value=100
        )
        st.info("Auto-refresh every 5 minutes")

    # Load data
    with st.spinner('Loading data...'):
        qr_df = get_qr_data()
        loyalty_df = get_loyalty_data()

    # QR Render Time Section
    st.header("QR Render Time Analysis")

    if not qr_df.empty:
        # Main metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Average Time", 
                f"{qr_df['render_time'].mean():.2f}ms",
                delta=f"{qr_df['render_time'].mean() - target_time:.1f}ms"
            )
        
        with col2:
            st.metric(
                "Maximum Time",
                f"{qr_df['render_time'].max():.2f}ms"
            )
        
        with col3:
            over_target = (qr_df['render_time'] > target_time).mean() * 100
            st.metric(
                "% Over Target",
                f"{over_target:.1f}%"
            )
        
        with col4:
            st.metric(
                "Total Measurements",
                len(qr_df)
            )

        # QR render time plot
        fig = go.Figure()
        fig.add_scatter(
            x=list(range(len(qr_df))),
            y=qr_df['render_time'],
            mode='markers',
            name='Individual times'
        )
        fig.add_hline(
            y=qr_df['render_time'].mean(),
            line_dash="dash",
            annotation_text=f"Average: {qr_df['render_time'].mean():.2f}ms"
        )
        fig.add_hline(
            y=target_time,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Target: {target_time}ms"
        )
        fig.update_layout(
            title="QR Render Time Distribution",
            xaxis_title="Measurement",
            yaxis_title="Time (ms)"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Detailed analysis
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Performance Distribution")
            performance_categories = pd.cut(
                qr_df['render_time'],
                bins=[0, 50, 75, target_time, float('inf')],
                labels=['Excellent', 'Good', 'Acceptable', 'Unsatisfactory']
            )
            fig_pie = px.pie(
                values=performance_categories.value_counts(),
                names=performance_categories.value_counts().index,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_pie)

        with col2:
            st.subheader("Detailed Statistics")
            stats_df = qr_df['render_time'].describe().round(2)
            st.dataframe(stats_df, use_container_width=True)

        # Detailed data table
        st.subheader("Detailed Data")
        st.dataframe(
            qr_df.sort_values('render_time', ascending=False),
            use_container_width=True
        )

    # Loyalty Programs Section
    st.header("Most Popular Loyalty Programs")

    if not loyalty_df.empty:
        # Calculate popularity
        popularity_count = loyalty_df['name'].value_counts()
        
        # Create bar chart using plotly
        fig_loyalty = px.bar(
            x=popularity_count.index,
            y=popularity_count.values,
            labels={'x': 'Stores', 'y': 'Number of Loyalty Cards'},
            title="Store Loyalty Program Distribution"
        )
        fig_loyalty.update_layout(
            showlegend=False,
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_loyalty, use_container_width=True)

        # Detailed statistics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top 5 Stores")
            st.dataframe(
                popularity_count.head().reset_index()
                .rename(columns={'index': 'Store', 'name': 'Card Count'}),
                use_container_width=True
            )
        
        with col2:
            st.subheader("Program Statistics")
            stats = {
                'Total Programs': len(popularity_count),
                'Total Cards': popularity_count.sum(),
                'Average Cards per Store': round(popularity_count.mean(), 2),
                'Max Cards in Store': popularity_count.max()
            }
            st.dataframe(
                pd.DataFrame(stats.items(), columns=['Metric', 'Value']),
                use_container_width=True
            )

    # Update button
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()

with tabs[1]:
    st.title("📊 Sprint 4 Dashboard")
    st.markdown("---")
    st.header("Language Switching Analysis")
    st.markdown("""
    **Business Question:** How frequently are users switching between language options in the app?
    
    This analysis tracks language preference changes to understand user behavior and app accessibility.
    """)
    
    @st.cache_data(ttl=300)
    def get_language_data():
        try:
            db = init_firebase()
            if db is None:
                return pd.DataFrame()
                
            lang_ref = db.collection('AnalyticsBusinessQuestions/sprint4/businessQuestion5')
            docs = lang_ref.stream()
            
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                data.append({
                    'user_id': doc_data.get('userId', 'unknown'),
                    'language': doc_data.get('lan', 'unknown'),
                    'timestamp': doc_data.get('timestamp', datetime.now())
                })
            
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Error fetching language data: {str(e)}")
            return pd.DataFrame()
    
    # Load data
    with st.spinner('Loading language data...'):
        lang_df = get_language_data()
    
    if not lang_df.empty:
        # Key metrics
        total_users = len(lang_df['user_id'].unique())
        lang_users = len(lang_df['user_id'].unique()) - len(lang_df.groupby('user_id').filter(lambda x: len(x) == 1)['user_id'].unique())
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Users", total_users)
        with col2:
            st.metric("Users Who Changed Language", lang_users)
        with col3:
            st.metric("Language Change Rate", f"{(lang_users/total_users)*100:.1f}%")
        
        # Language preference distribution
        st.subheader("Language Preference Distribution")
        lang_dist = lang_df.groupby('language').size()
        fig_lang = px.pie(
            values=lang_dist.values,
            names=lang_dist.index,
            title="Language Selection Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_lang, use_container_width=True)
        
        st.subheader("Language Usage")
        lang_dist = lang_df.groupby('language').size().reset_index()
        lang_dist.columns = ['Language', 'Count']

        fig_lang = px.bar(
            lang_dist,
            x='Language',
            y='Count',
            title="Language Distribution",
            color='Language',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_lang, use_container_width=True)
        # Detailed data
        st.subheader("User Language Change Details")
        user_changes = lang_df.groupby('user_id').agg({
            'language': 'count',
            'timestamp': ['min', 'max']
        }).round(2)
        user_changes.columns = ['Total Changes', 'First Change', 'Last Change']
        st.dataframe(user_changes.sort_values('Total Changes', ascending=False))