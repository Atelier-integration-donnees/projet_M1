import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import warnings

# ==============================================================================
# 0. CONFIGURATION & STYLE
# ==============================================================================
st.set_page_config(
    page_title="OpenFoodFacts Analytics Suite",
    page_icon="🍏",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suppression totale des warnings
warnings.filterwarnings('ignore')

# CSS Pro pour embellir l'interface
st.markdown("""
<style>
    div.block-container {padding-top: 1rem;}
    .metric-card {
        background-color: #ffffff;
        border-left: 5px solid #ff4b4b;
        border-radius: 5px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 1. GESTION DES DONNÉES
# ==============================================================================
@st.cache_resource
def get_db_connection():
    return mysql.connector.connect(
        host="localhost", port=3309, user="root", password="root", database="off_datamart"
    )

@st.cache_data(ttl=600)
def load_all_data():
    conn = get_db_connection()
    
    # Requêtes SQL
    q_main = """
    SELECT 
        p.product_name, b.brand_name, f.nutriscore_grade, f.nova_group,
        f.energy_kcal_100g, f.sugars_100g, f.fat_100g, f.salt_100g, f.proteins_100g, f.fiber_100g,
        f.completeness_score, t.year, f.product_sk
    FROM fact_nutrition_snapshot f
    JOIN dim_product p ON f.product_sk = p.product_sk
    LEFT JOIN dim_brand b ON p.brand_sk = b.brand_sk
    LEFT JOIN dim_time t ON f.time_sk = t.time_sk
    """
    
    q_cat = """
    SELECT f.product_sk, c.category_name_fr 
    FROM fact_nutrition_snapshot f
    JOIN bridge_product_category bpc ON f.product_sk = bpc.product_sk
    JOIN dim_category c ON bpc.category_sk = c.category_sk
    """
    
    q_country = """
    SELECT f.product_sk, c.country_code 
    FROM fact_nutrition_snapshot f
    JOIN bridge_product_country bpc ON f.product_sk = bpc.product_sk
    JOIN dim_country c ON bpc.country_sk = c.country_sk
    """
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_main = pd.read_sql(q_main, conn)
        df_cat = pd.read_sql(q_cat, conn)
        df_country = pd.read_sql(q_country, conn)
    
    conn.close()
    
    # Nettoyage des données
    cols_num = ['energy_kcal_100g', 'sugars_100g', 'fat_100g', 'salt_100g', 'proteins_100g', 'fiber_100g']
    df_main[cols_num] = df_main[cols_num].fillna(0)
    df_main['nutriscore_grade'] = df_main['nutriscore_grade'].fillna('NON CLASSÉ').astype(str).str.upper().replace('NONE', 'NON CLASSÉ')
    
    return df_main, df_cat, df_country

# Chargement
try:
    with st.spinner('Chargement des données...'):
        df, df_cat, df_country = load_all_data()
except Exception as e:
    st.error(f"❌ Erreur BDD : {e}")
    st.stop()

# ==============================================================================
# 2. BARRE LATÉRALE
# ==============================================================================
# CORRECTION : On force une largeur fixe pour éviter le warning 'use_container_width'
st.sidebar.image("https://static.openfoodfacts.org/images/logos/off-logo-horizontal-light.svg", width=250)
st.sidebar.title("Navigation")

page = st.sidebar.radio("Aller vers :", [
    "1. 🏠 Vue d'Ensemble",
    "2. 🌍 Géographie & Marchés",
    "3. 🔬 Laboratoire Nutrition",
    "4. 🏭 Marques & Catégories",
    "5. ⚙️ Santé des Données"
])

st.sidebar.markdown("---")
st.sidebar.subheader("Filtres")

all_scores = sorted(df['nutriscore_grade'].unique())
sel_scores = st.sidebar.multiselect("Nutri-Score", all_scores, default=all_scores)

if not sel_scores:
    df_filtered = df.copy()
else:
    df_filtered = df[df['nutriscore_grade'].isin(sel_scores)]

st.sidebar.success(f"📦 **{len(df_filtered):,}** Produits")

# ==============================================================================
# PAGE 1 : VUE D'ENSEMBLE
# ==============================================================================
if "1." in page:
    st.title("🏠 Vue d'Ensemble Stratégique")
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Volume Total", f"{len(df_filtered):,}", delta="Produits")
    c2.metric("Énergie Moyenne", f"{round(df_filtered['energy_kcal_100g'].mean(), 1)} kcal")
    c3.metric("Sucre Moyen", f"{round(df_filtered['sugars_100g'].mean(), 1)} g")
    c4.metric("Qualité Data", f"{round(df_filtered['completeness_score'].mean()*100, 1)}%")
    c5.metric("Marques", f"{df_filtered['brand_name'].nunique():,}")
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📊 1. Distribution Nutri-Score")
        custom_order = {'nutriscore_grade': ['A', 'B', 'C', 'D', 'E', 'NON CLASSÉ']}
        fig1 = px.histogram(df_filtered, x="nutriscore_grade", color="nutriscore_grade",
                            category_orders=custom_order,
                            color_discrete_map={'A':'#008000', 'B':'#85BB2F', 'C':'#FFD700', 'D':'#FF8C00', 'E':'#FF0000', 'NON CLASSÉ':'#808080'},
                            title="Répartition des notes (Volume)")
        # Correction warning: use_container_width=True est encore valide pour les charts
        st.plotly_chart(fig1, use_container_width=True)
        
    with col2:
        st.subheader("🏭 2. NOVA")
        df_nova = df_filtered['nova_group'].fillna(0).astype(int).astype(str).replace('0', 'Inconnu')
        fig2 = px.pie(names=df_nova, title="Produits transformés", hole=0.5)
        st.plotly_chart(fig2, use_container_width=True)
        
    st.subheader("📈 3. Évolution par Année")
    df_trend = df_filtered.groupby('year')['energy_kcal_100g'].mean().reset_index()
    fig3 = px.line(df_trend, x='year', y='energy_kcal_100g', markers=True, title="Moyenne calorique par an")
    st.plotly_chart(fig3, use_container_width=True)

# ==============================================================================
# PAGE 2 : GÉOGRAPHIE
# ==============================================================================
elif "2." in page:
    st.title("🌍 Analyse Géographique")
    
    df_geo = df_filtered.merge(df_country, on="product_sk")
    top_countries = df_geo['country_code'].value_counts().head(15).reset_index()
    top_countries.columns = ['Pays', 'Volume']
    
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("🗺️ 4. Carte Mondiale")
        fig4 = px.choropleth(top_countries, locations="Pays", locationmode="country names",
                             color="Volume", color_continuous_scale="Viridis")
        st.plotly_chart(fig4, use_container_width=True)
        
    with c2:
        st.subheader("🏆 5. Top Pays")
        fig5 = px.bar(top_countries.head(10), x="Volume", y="Pays", orientation='h', color="Volume")
        st.plotly_chart(fig5, use_container_width=True)
        
    st.subheader("🔍 6. France vs Monde (Sucres/Gras/Sel)")
    df_geo['is_france'] = df_geo['country_code'].apply(lambda x: 'France' if 'fr' in str(x).lower() else 'Monde')
    df_comp = df_geo.groupby('is_france')[['sugars_100g', 'fat_100g', 'salt_100g']].mean().reset_index()
    df_comp_melt = df_comp.melt(id_vars='is_france', var_name='Nutriment', value_name='Moyenne (g)')
    
    fig6 = px.bar(df_comp_melt, x='Nutriment', y='Moyenne (g)', color='is_france', barmode='group')
    st.plotly_chart(fig6, use_container_width=True)

# ==============================================================================
# PAGE 3 : LABORATOIRE
# ==============================================================================
elif "3." in page:
    st.title("🔬 Laboratoire Nutritionnel")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🍭 7. Sucre vs Calories")
        sample_df = df_filtered.sample(min(2000, len(df_filtered)))
        sample_df['size_ref'] = sample_df['fat_100g'].apply(lambda x: max(0.1, x))
        
        fig7 = px.scatter(sample_df, 
                          x="sugars_100g", y="energy_kcal_100g", color="nutriscore_grade",
                          size="size_ref",
                          title="Matrice Sucre/Énergie (Taille = Gras)",
                          color_discrete_map={'A':'green', 'E':'red'})
        st.plotly_chart(fig7, use_container_width=True)
        
    with col2:
        st.subheader("🧂 8. Sel et Nutri-Score")
        df_salt = df_filtered[df_filtered['salt_100g'] < 10]
        fig8 = px.box(df_salt, x="nutriscore_grade", y="salt_100g", 
                      category_orders={'nutriscore_grade': ['A','B','C','D','E']})
        st.plotly_chart(fig8, use_container_width=True)
        
    col3, col4, col5 = st.columns(3)
    with col3:
        st.subheader("🥩 9. Protéines")
        fig9 = px.histogram(df_filtered, x="proteins_100g", nbins=40, color_discrete_sequence=['#6c5ce7'])
        st.plotly_chart(fig9, use_container_width=True)
    with col4:
        st.subheader("🍞 10. Fibres")
        fig10 = px.histogram(df_filtered, x="fiber_100g", nbins=40, color_discrete_sequence=['#00b894'])
        st.plotly_chart(fig10, use_container_width=True)
    with col5:
        st.subheader("🥑 11. Gras")
        fig11 = px.histogram(df_filtered, x="fat_100g", nbins=40, color_discrete_sequence=['#fab1a0'])
        st.plotly_chart(fig11, use_container_width=True)

# ==============================================================================
# PAGE 4 : MARQUES
# ==============================================================================
elif "4." in page:
    st.title("🏭 Marques & Catégories")
    
    tab_brands, tab_cats = st.tabs(["Marques", "Catégories"])
    
    with tab_brands:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("📢 12. Top Volumes")
            top_brands = df_filtered['brand_name'].value_counts().head(15).reset_index()
            fig12 = px.bar(top_brands, y='brand_name', x='count', orientation='h')
            st.plotly_chart(fig12, use_container_width=True)
            
        with c2:
            st.subheader("🌟 13. Top Qualité")
            mapping = {'A':1, 'B':2, 'C':3, 'D':4, 'E':5}
            df_score = df_filtered.copy()
            df_score['score'] = df_score['nutriscore_grade'].map(mapping)
            grp = df_score.groupby('brand_name').filter(lambda x: len(x) > 10)
            best_brands = grp.groupby('brand_name')['score'].mean().nsmallest(15).reset_index()
            fig13 = px.bar(best_brands, x='score', y='brand_name', orientation='h', color='score', color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig13, use_container_width=True)
            
    with tab_cats:
        df_cat_join = df_filtered.merge(df_cat, on="product_sk")
        top_cats = df_cat_join['category_name_fr'].value_counts().head(20).reset_index()
        st.subheader("📦 14. Top Catégories")
        fig14 = px.treemap(top_cats, path=['category_name_fr'], values='count')
        st.plotly_chart(fig14, use_container_width=True)

# ==============================================================================
# PAGE 5 : DATA HEALTH
# ==============================================================================
elif "5." in page:
    st.title("⚙️ Monitoring Pipeline")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("📉 15. Complétude")
        gauge_val = df_filtered['completeness_score'].mean() * 100
        fig15 = go.Figure(go.Indicator(
            mode = "gauge+number", value = gauge_val,
            gauge = {'axis': {'range': [None, 100]}, 'bar': {'color': "#0984e3"}}
        ))
        st.plotly_chart(fig15, use_container_width=True)
        
    with col2:
        st.subheader("⚠️ 16. Produits Non Classés")
        missing_ns = len(df[df['nutriscore_grade'] == 'NON CLASSÉ'])
        df_missing = pd.DataFrame({'Statut': ['Classé', 'Non Classé'], 'Volume': [len(df)-missing_ns, missing_ns]})
        fig16 = px.bar(df_missing, x='Volume', y='Statut', orientation='h', color='Statut', 
                       color_discrete_map={'Classé':'#00b894', 'Non Classé':'#d63031'})
        st.plotly_chart(fig16, use_container_width=True)
        
    st.subheader("📝 17. Données Brutes")
    st.dataframe(df_filtered.head(100), use_container_width=True)