import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="QR Performance Dashboard",
    page_icon="⚡",
    layout="wide"
)

# Inicialización de Firebase usando secrets
@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        # Convertir los secrets a un diccionario y usarlo directamente
        key_dict = dict(st.secrets["firebase"])
        # Asegurarse de que el private_key sea una string
        if isinstance(key_dict.get('private_key'), str):
            key_dict['private_key'] = key_dict['private_key'].replace('\\n', '\n')
        try:
            cred = credentials.Certificate(key_dict)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Error inicializando Firebase: {str(e)}")
            return None
    return firestore.client()

# Obtención de datos
@st.cache_data(ttl=300)
def get_data():
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
        st.error(f"Error al obtener datos: {str(e)}")
        return pd.DataFrame()

# Interfaz de usuario
st.title("⚡ QR Performance Dashboard")
st.markdown("---")

# Sidebar para filtros
with st.sidebar:
    st.header("Configuración")
    target_time = st.slider(
        "Tiempo objetivo (ms)", 
        min_value=50, 
        max_value=200, 
        value=100
    )
    st.info("Actualización automática cada 5 minutos")

# Cargar datos
with st.spinner('Cargando datos...'):
    df = get_data()

if df.empty:
    st.warning("No se pudieron cargar los datos. Por favor verifica la conexión con Firebase.")
else:
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Tiempo Promedio", 
            f"{df['render_time'].mean():.2f}ms",
            delta=f"{df['render_time'].mean() - target_time:.1f}ms"
        )
    
    with col2:
        st.metric(
            "Tiempo Máximo",
            f"{df['render_time'].max():.2f}ms"
        )
    
    with col3:
        over_target = (df['render_time'] > target_time).mean() * 100
        st.metric(
            "% Sobre Objetivo",
            f"{over_target:.1f}%"
        )
    
    with col4:
        st.metric(
            "Total Mediciones",
            len(df)
        )

    # Gráfico principal
    st.subheader("Análisis de Tiempos de Renderizado")
    
    fig = go.Figure()
    fig.add_scatter(
        x=list(range(len(df))),
        y=df['render_time'],
        mode='markers',
        name='Tiempos individuales'
    )
    fig.add_hline(
        y=df['render_time'].mean(),
        line_dash="dash",
        annotation_text=f"Promedio: {df['render_time'].mean():.2f}ms"
    )
    fig.add_hline(
        y=target_time,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Objetivo: {target_time}ms"
    )
    fig.update_layout(
        title="Tiempos de Renderizado QR",
        xaxis_title="Medición",
        yaxis_title="Tiempo (ms)"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Análisis detallado
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribución de Rendimiento")
        performance_categories = pd.cut(
            df['render_time'],
            bins=[0, 50, 75, target_time, float('inf')],
            labels=['Excelente', 'Bueno', 'Aceptable', 'Insatisfactorio']
        )
        fig_pie = px.pie(
            values=performance_categories.value_counts(),
            names=performance_categories.value_counts().index,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_pie)

    with col2:
        st.subheader("Estadísticas Detalladas")
        stats_df = df['render_time'].describe().round(2)
        st.dataframe(stats_df, use_container_width=True)

    # Tabla de datos
    st.subheader("Datos Detallados")
    st.dataframe(
        df.sort_values('render_time', ascending=False),
        use_container_width=True
    )

# Botón de actualización manual
if st.button("Actualizar Datos"):
    st.cache_data.clear()
    st.experimental_rerun()