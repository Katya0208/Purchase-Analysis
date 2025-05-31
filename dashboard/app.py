import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from clickhouse_driver import Client
import os

# Настройка страницы
st.set_page_config(
    page_title="Анализ покупок",
    page_icon="📊",
    layout="wide"
)

# Подключение к ClickHouse
@st.cache_resource
def get_clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )

client = get_clickhouse_client()

# Заголовок
st.title("📊 Анализ покупок в магазине")

# Получение данных
@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_top_products(limit=10):
    query = f"""
    SELECT 
        product_id,
        sum(quantity) as total_quantity,
        sum(quantity * price) as total_revenue
    FROM purchases_rt
    GROUP BY product_id
    ORDER BY total_quantity DESC
    LIMIT {limit}
    """
    return pd.DataFrame(client.execute(query), columns=['product_id', 'total_quantity', 'total_revenue'])

@st.cache_data(ttl=300)
def get_sales_by_month():
    query = """
    SELECT 
        toStartOfMonth(ts) as month,
        count() as total_purchases,
        sum(quantity) as total_quantity,
        sum(quantity * price) as total_revenue
    FROM purchases_rt
    GROUP BY month
    ORDER BY month
    """
    return pd.DataFrame(client.execute(query), columns=['month', 'total_purchases', 'total_quantity', 'total_revenue'])

@st.cache_data(ttl=300)
def get_revenue_by_hour():
    query = """
    SELECT 
        toHour(ts) as hour,
        sum(quantity * price) as revenue
    FROM purchases_rt
    GROUP BY hour
    ORDER BY hour
    """
    return pd.DataFrame(client.execute(query), columns=['hour', 'revenue'])

# Создаем две колонки
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Продажи по месяцам")
    sales_df = get_sales_by_month()
    if not sales_df.empty:
        fig = px.line(sales_df, x='month', y='total_revenue',
                     title='Выручка по месяцам')
        st.plotly_chart(fig, use_container_width=True)
        
        fig2 = px.bar(sales_df, x='month', y='total_quantity',
                      title='Количество продаж по месяцам')
        st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("🕒 Выручка по часам")
    revenue_df = get_revenue_by_hour()
    if not revenue_df.empty:
        fig = px.line(revenue_df, x='hour', y='revenue',
                     title='Распределение выручки по часам')
        st.plotly_chart(fig, use_container_width=True)

# Топ товаров
st.subheader("🏆 Топ товаров")
limit = st.slider("Количество товаров", 5, 20, 10)
top_products_df = get_top_products(limit)

if not top_products_df.empty:
    fig = px.bar(top_products_df, x='product_id', y='total_quantity',
                 title='Топ товаров по количеству продаж')
    st.plotly_chart(fig, use_container_width=True)
    
    fig2 = px.bar(top_products_df, x='product_id', y='total_revenue',
                  title='Топ товаров по выручке')
    st.plotly_chart(fig2, use_container_width=True)
    
    st.dataframe(top_products_df)

# Статистика
st.subheader("📊 Общая статистика")
if not sales_df.empty:
    total_revenue = sales_df['total_revenue'].sum()
    total_quantity = sales_df['total_quantity'].sum()
    total_purchases = sales_df['total_purchases'].sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Общая выручка", f"${total_revenue:,.2f}")
    with col2:
        st.metric("Общее количество продаж", f"{total_quantity:,}")
    with col3:
        st.metric("Количество покупок", f"{total_purchases:,}") 