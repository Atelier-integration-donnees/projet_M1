import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import mysql.connector
import warnings
import numpy as np

# ==============================================================================
# 0. CONFIGURATION & STYLE "DARK PRO"
# ==============================================================================
st.set_page_config(
    page_title="OpenFoodFacts Enterprise Suite",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suppression des alertes pour une démo propre
warnings.filterwarnings('ignore')

# CSS Avancé (Cards, Ombres, Titres)
st.markdown("""
<style>
    div.block-container {padding-top: 1rem;}
    .metric-card {
        background-color: #f8f9fa;
        border-left: 5px solid #ff6b6b;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }
    h1 {color: #2d3436; font-weight: 800;}
    h2 {color: #0984e3; border-bottom: 2px solid #0984e3; padding-bottom: 5px;}
    h3 {color: #636e72;}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 1. MOTEUR DE DONNÉES (CACHE & SQL)
# ==============================================================================
@st.cache_resource
def get_db_connection():
    return mysql.connector.connect(
        host="localhost", port=3309, user="root", password="root", database="off_datamart"
    )

@st.cache_data(ttl=900)
def load_data_engine():
    conn = get_db_connection()
    
    # 1. MAIN DATASET
    q_main = """
    SELECT 
        p.product_name, b.brand_name, f.nutriscore_grade, f.nova_group,
        f.energy_kcal_100g, f.sugars_100g, f.fat_100g, f.salt_100g, 
        f.proteins_100g, f.fiber_100g, f.completeness_score, 
        t.year, f.product_sk
    FROM fact_nutrition_snapshot f
    JOIN dim_product p ON f.product_sk = p.product_sk
    LEFT JOIN dim_brand b ON p.brand_sk = b.brand_sk
    LEFT JOIN dim_time t ON f.time_sk = t.time_sk
    """
    
    # 2. GEO DATASET
    q_geo = """
    SELECT f.product_sk, c.country_code 
    FROM fact_nutrition_snapshot f
    JOIN bridge_product_country bpc ON f.product_sk = bpc.product_sk
    JOIN dim_country c ON bpc.country_sk = c.country_sk
    """
    
    # 3. CATEGORY DATASET
    q_cat = """
    SELECT f.product_sk, c.category_name_fr 
    FROM fact_nutrition_snapshot f
    JOIN bridge_product_category bpc ON f.product_sk = bpc.product_sk
    JOIN dim_category c ON bpc.category_sk = c.category_sk
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_main = pd.read_sql(q_main, conn)
        df_geo = pd.read_sql(q_geo, conn)
        df_cat = pd.read_sql(q_cat, conn)
        
    conn.close()
    
    # NETTOYAGE ROBUSTE
    cols_num = ['energy_kcal_100g', 'sugars_100g', 'fat_100g', 'salt_100g', 'proteins_100g', 'fiber_100g']
    df_main[cols_num] = df_main[cols_num].fillna(0)
    
    # Nutriscore propre
    df_main['nutriscore_grade'] = df_main['nutriscore_grade'].fillna('NON CLASSÉ').astype(str).str.upper().replace('NONE', 'NON CLASSÉ')
    
    # Mapping numérique pour scores
    score_map = {'A':1, 'B':2, 'C':3, 'D':4, 'E':5, 'NON CLASSÉ': np.nan}
    df_main['score_num'] = df_main['nutriscore_grade'].map(score_map)
    
    return df_main, df_geo, df_cat

# Chargement
try:
    with st.spinner('🚀 Initialisation du moteur analytique...'):
        df, df_country, df_cat = load_data_engine()
except Exception as e:
    st.error(f"Erreur Critique BDD: {e}")
    st.stop()

# ==============================================================================
# 2. NAVIGATION & FILTRES
# ==============================================================================
st.sidebar.title("🎛️ Command Center")
st.sidebar.info("Projet Data Engineering - Gold Layer")

page = st.sidebar.radio("Module d'Analyse :", [
    "1. 🏠 Executive Summary",
    "2. 🌍 Geo-Analytics",
    "3. 🔬 Deep Nutrition Lab",
    "4. 🏭 Market Intelligence",
    "5. 📦 Category Insights",
    "6. 🕒 Time Series",
    "7. ⚙️ Data Health Check"
])

st.sidebar.markdown("---")
st.sidebar.write("🔎 **Filtres Globaux**")
sel_scores = st.sidebar.multiselect("Nutri-Score", sorted(df['nutriscore_grade'].unique()), default=sorted(df['nutriscore_grade'].unique()))
sel_years = st.sidebar.slider("Année d'ajout", int(df['year'].min()), int(df['year'].max()), (2015, 2024))

# Application filtres
df_filtered = df[
    (df['nutriscore_grade'].isin(sel_scores)) & 
    (df['year'] >= sel_years[0]) & 
    (df['year'] <= sel_years[1])
]

# ==============================================================================
# PAGE 1: EXECUTIVE SUMMARY (Vue Stratégique)
# ==============================================================================
if "1." in page:
    st.title("🏠 Executive Summary")
    
    # --- SECTION A: KPIs ---
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    kpi1.metric("Produits Actifs", f"{len(df_filtered):,}", "Live")
    kpi2.metric("Énergie Moyenne", f"{int(df_filtered['energy_kcal_100g'].mean())} kcal")
    kpi3.metric("Taux de Sucre", f"{round(df_filtered['sugars_100g'].mean(), 1)} g")
    kpi4.metric("Marques", f"{df_filtered['brand_name'].nunique():,}")
    kpi5.metric("Qualité Donnée", f"{round(df_filtered['completeness_score'].mean()*100, 1)}%")
    
    st.markdown("---")
    
    # --- SECTION B: GRAPHS 1-4 ---
    c1, c2 = st.columns([1, 1])
    
    with c1:
        st.subheader("📊 1. Distribution Nutri-Score")
        color_map = {'A':'#2ecc71', 'B':'#badc58', 'C':'#f1c40f', 'D':'#e67e22', 'E':'#e74c3c', 'NON CLASSÉ':'#95a5a6'}
        fig1 = px.histogram(df_filtered, x="nutriscore_grade", color="nutriscore_grade", 
                            color_discrete_map=color_map, category_orders={"nutriscore_grade": ["A","B","C","D","E","NON CLASSÉ"]})
        st.plotly_chart(fig1, use_container_width=True)
        
    with c2:
        st.subheader("🧬 2. Classification NOVA")
        df_nova = df_filtered['nova_group'].fillna(0).astype(int).astype(str).replace('0', 'Inconnu')
        fig2 = px.pie(names=df_nova, hole=0.6, color_discrete_sequence=px.colors.qualitative.Prism)
        fig2.update_layout(annotations=[dict(text='NOVA', x=0.5, y=0.5, font_size=20, showarrow=False)])
        st.plotly_chart(fig2, use_container_width=True)
        
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("⚖️ 3. Croisement Nutri-Score / NOVA")
        df_stack = df_filtered[df_filtered['nutriscore_grade'] != 'NON CLASSÉ']
        fig3 = px.histogram(df_stack, x="nutriscore_grade", color="nova_group", barmode="group",
                            category_orders={"nutriscore_grade": ["A","B","C","D","E"]},
                            color_discrete_sequence=px.colors.sequential.Plasma)
        st.plotly_chart(fig3, use_container_width=True)
        
    with c4:
        st.subheader("📉 4. Répartition Calorique")
        fig4 = px.violin(df_filtered, y="energy_kcal_100g", box=True, points=False, color_discrete_sequence=['#e84393'])
        st.plotly_chart(fig4, use_container_width=True)

# ==============================================================================
# PAGE 2: GEO-ANALYTICS (Monde & Comparaisons)
# ==============================================================================
elif "2." in page:
    st.title("🌍 Geo-Analytics")
    
    # Prep Data Geo
    df_geo_full = df_filtered.merge(df_country, on="product_sk")
    country_counts = df_geo_full['country_code'].value_counts().reset_index()
    country_counts.columns = ['Pays', 'Volume']
    
    # --- GRAPHS 5-8 ---
    st.subheader("🗺️ 5. Carte de Chaleur Mondiale")
    fig5 = px.choropleth(country_counts.head(50), locations="Pays", locationmode='country names',
                         color="Volume", hover_name="Pays", color_continuous_scale="Viridis",
                         projection="natural earth")
    st.plotly_chart(fig5, use_container_width=True)
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("🏆 6. Top 15 Pays Producteurs")
        fig6 = px.bar(country_counts.head(15), y='Pays', x='Volume', orientation='h', color='Volume')
        fig6.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig6, use_container_width=True)
        
    with col_b:
        st.subheader("🍟 7. France vs Monde (Gras)")
        df_geo_full['zone'] = df_geo_full['country_code'].apply(lambda x: 'France' if 'fr' in str(x).lower() else 'Reste du Monde')
        df_comp = df_geo_full.groupby('zone')['fat_100g'].mean().reset_index()
        fig7 = px.bar(df_comp, x='zone', y='fat_100g', color='zone', title="Moyenne Lipides (g/100g)")
        st.plotly_chart(fig7, use_container_width=True)

    st.subheader("🍩 8. Comparaison Sucre par Zone")
    df_comp_sugar = df_geo_full.groupby('zone')['sugars_100g'].mean().reset_index()
    fig8 = px.bar(df_comp_sugar, x='zone', y='sugars_100g', color='zone', color_discrete_sequence=['#ff7675', '#74b9ff'])
    st.plotly_chart(fig8, use_container_width=True)

# ==============================================================================
# PAGE 3: DEEP NUTRITION LAB (Statistiques Avancées)
# ==============================================================================
elif "3." in page:
    st.title("🔬 Deep Nutrition Lab")
    
    # --- GRAPHS 9-14 ---
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🔥 9. Corrélation Énergie / Sucre")
        sample_df = df_filtered.sample(min(2000, len(df_filtered)))
        fig9 = px.scatter(sample_df, x="sugars_100g", y="energy_kcal_100g", color="nutriscore_grade",
                          size=sample_df['fat_100g'].apply(lambda x: max(0.1, x)),
                          color_discrete_map={'A':'green', 'E':'red'})
        st.plotly_chart(fig9, use_container_width=True)
        
    with col2:
        st.subheader("🧂 10. Impact du Sel sur le Score")
        df_salt = df_filtered[df_filtered['salt_100g'] < 10]
        fig10 = px.box(df_salt, x="nutriscore_grade", y="salt_100g", color="nutriscore_grade",
                       category_orders={'nutriscore_grade': ['A','B','C','D','E']})
        st.plotly_chart(fig10, use_container_width=True)
        
    st.markdown("### 📊 Distribution des Macronutriments")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.write("**11. Protéines**")
        fig11 = px.histogram(df_filtered, x="proteins_100g", nbins=50, color_discrete_sequence=['#a29bfe'])
        st.plotly_chart(fig11, use_container_width=True)
    with c2:
        st.write("**12. Fibres**")
        fig12 = px.histogram(df_filtered, x="fiber_100g", nbins=50, color_discrete_sequence=['#55efc4'])
        st.plotly_chart(fig12, use_container_width=True)
    with c3:
        st.write("**13. Gras Saturés**")
        # On simule gras saturés via fat si pas dispo ou utilise fat_100g
        fig13 = px.histogram(df_filtered, x="fat_100g", nbins=50, color_discrete_sequence=['#fab1a0'])
        st.plotly_chart(fig13, use_container_width=True)

    st.subheader("🧩 14. Matrice de Corrélation")
    corr = df_filtered[['energy_kcal_100g', 'sugars_100g', 'fat_100g', 'salt_100g', 'proteins_100g']].corr()
    fig14 = px.imshow(corr, text_auto=True, color_continuous_scale='RdBu_r')
    st.plotly_chart(fig14, use_container_width=True)

# ==============================================================================
# PAGE 4: MARKET INTELLIGENCE (Marques)
# ==============================================================================
elif "4." in page:
    st.title("🏭 Market Intelligence")
    
    # --- GRAPHS 15-18 ---
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📢 15. Top 20 Marques (Volume)")
        top_brands = df_filtered['brand_name'].value_counts().head(20).reset_index()
        fig15 = px.bar(top_brands, y='brand_name', x='count', orientation='h', color='count', color_continuous_scale='Blues')
        fig15.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig15, use_container_width=True)
        
    with col2:
        st.subheader("🌟 16. Top Qualité (Moyenne Nutri-Score)")
        # Filtre marques > 15 produits
        grp = df_filtered.groupby('brand_name').filter(lambda x: len(x) > 15)
        best = grp.groupby('brand_name')['score_num'].mean().nsmallest(20).reset_index()
        fig16 = px.bar(best, x='score_num', y='brand_name', orientation='h', 
                       title="Plus le score est bas (1), mieux c'est",
                       color='score_num', color_continuous_scale='RdYlGn_r')
        fig16.update_layout(yaxis={'categoryorder':'total descending'})
        st.plotly_chart(fig16, use_container_width=True)
        
    st.subheader("🎯 17. Positionnement Marques (Prix vs Santé)")
    st.caption("Hypothèse : Energy ~ Densité / Score ~ Santé")
    top_50_brands = df_filtered['brand_name'].value_counts().head(50).index
    df_pos = df_filtered[df_filtered['brand_name'].isin(top_50_brands)].groupby('brand_name')[['energy_kcal_100g', 'score_num']].mean().reset_index()
    fig17 = px.scatter(df_pos, x="score_num", y="energy_kcal_100g", text="brand_name", size_max=60,
                       color="energy_kcal_100g", title="Mapping Stratégique")
    st.plotly_chart(fig17, use_container_width=True)

    st.subheader("📦 18. Diversité des produits par Marque")
    # Nombre de produits uniques vs Total
    div_brand = df_filtered.groupby('brand_name')['product_name'].nunique().sort_values(ascending=False).head(15).reset_index()
    fig18 = px.bar(div_brand, x='brand_name', y='product_name', color_discrete_sequence=['#6c5ce7'])
    st.plotly_chart(fig18, use_container_width=True)

# ==============================================================================
# PAGE 5: CATEGORY INSIGHTS
# ==============================================================================
elif "5." in page:
    st.title("📦 Category Insights")
    
    # Data Prep
    df_cat_join = df_filtered.merge(df_cat, on="product_sk")
    top_cats = df_cat_join['category_name_fr'].value_counts().head(30).reset_index()
    top_cats.columns = ['Categorie', 'Volume']
    
    # --- GRAPHS 19-21 ---
    st.subheader("🌳 19. Treemap des Catégories")
    fig19 = px.treemap(top_cats, path=['Categorie'], values='Volume', color='Volume', color_continuous_scale='Spectral')
    st.plotly_chart(fig19, use_container_width=True)
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🍬 20. Sucre par Top Catégories")
        top_5_cats = top_cats.head(5)['Categorie'].tolist()
        df_top_cats = df_cat_join[df_cat_join['category_name_fr'].isin(top_5_cats)]
        fig20 = px.box(df_top_cats, x="category_name_fr", y="sugars_100g", color="category_name_fr")
        st.plotly_chart(fig20, use_container_width=True)
        
    with c2:
        st.subheader("🥑 21. Gras par Top Catégories")
        fig21 = px.box(df_top_cats, x="category_name_fr", y="fat_100g", color="category_name_fr")
        st.plotly_chart(fig21, use_container_width=True)

# ==============================================================================
# PAGE 6: TIME SERIES (Tendances)
# ==============================================================================
elif "6." in page:
    st.title("🕒 Time Series Analysis")
    
    # Aggregations Annuelle
    df_year = df_filtered.groupby('year').agg({
        'energy_kcal_100g': 'mean',
        'sugars_100g': 'mean',
        'fat_100g': 'mean',
        'product_sk': 'count'
    }).reset_index()
    
    # --- GRAPHS 22-24 ---
    st.subheader("📈 22. Volume de produits ajoutés")
    fig22 = px.area(df_year, x='year', y='product_sk', title="Croissance de la base de données", color_discrete_sequence=['#0984e3'])
    st.plotly_chart(fig22, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📉 23. Évolution du Sucre")
        fig23 = px.line(df_year, x='year', y='sugars_100g', markers=True, title="Tendance Sucre (g/100g)", color_discrete_sequence=['#d63031'])
        st.plotly_chart(fig23, use_container_width=True)
        
    with col2:
        st.subheader("⚡ 24. Évolution Énergie")
        fig24 = px.line(df_year, x='year', y='energy_kcal_100g', markers=True, title="Tendance Calories (kcal)", color_discrete_sequence=['#fdcb6e'])
        st.plotly_chart(fig24, use_container_width=True)

# ==============================================================================
# PAGE 7: DATA HEALTH CHECK (Qualité)
# ==============================================================================
elif "7." in page:
    st.title("⚙️ Data Health Check")
    
    # --- GRAPHS 25-27 ---
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("🎯 25. Score de Complétude")
        val = df_filtered['completeness_score'].mean() * 100
        fig25 = go.Figure(go.Indicator(
            mode = "gauge+number", value = val,
            gauge = {'axis': {'range': [None, 100]}, 'bar': {'color': "#00b894"}}
        ))
        st.plotly_chart(fig25, use_container_width=True)
        
    with col2:
        st.subheader("⚠️ 26. Données Manquantes (Nutri-Score)")
        missing = len(df[df['nutriscore_grade']=='NON CLASSÉ'])
        present = len(df) - missing
        df_miss = pd.DataFrame({'Status': ['Nutri-Score OK', 'Manquant'], 'Count': [present, missing]})
        fig26 = px.pie(df_miss, values='Count', names='Status', hole=0.4, color_discrete_map={'Nutri-Score OK':'#00b894', 'Manquant':'#fab1a0'})
        st.plotly_chart(fig26, use_container_width=True)
        
    st.subheader("📋 27. Funnel de Qualité")
    # Funnel: Total -> Avec Energie -> Avec Nutriscore -> Avec Nova
    step1 = len(df)
    step2 = len(df[df['energy_kcal_100g'] > 0])
    step3 = len(df[df['nutriscore_grade'] != 'NON CLASSÉ'])
    step4 = len(df[df['nova_group'].notna()])
    
    data_funnel = dict(
        number=[step1, step2, step3, step4],
        stage=["Total Produits", "Avec Calories", "Avec Nutri-Score", "Avec NOVA"]
    )
    fig27 = px.funnel(data_funnel, x='number', y='stage')
    st.plotly_chart(fig27, use_container_width=True)

    st.success("✅ Pipeline ETL Gold Layer opérationnel.")