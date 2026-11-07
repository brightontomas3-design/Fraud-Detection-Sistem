import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Detesaun Fraude - Tempu Real", layout="wide")

st.title("🛡️ Detesaun Fraude - Tempu Real")
st.caption("Modo Demo • Dadus simulasaun ba aprezentasaun • Atualiza automatiku")

# --- Dadus Demo ---
np.random.seed(42)
n = 500
agora = datetime.now()
datas = [agora - timedelta(minutes=int(x)) for x in np.random.randint(0, 1440, n)]
montante = np.random.lognormal(mean=5, sigma=1.2, size=n).round(2)
score = np.random.beta(2, 5, size=n).round(3)
def desizaun(s):
    if s > 0.85:
        return "FRAUDE-BLOKEIA"
    elif s > 0.55:
        return "SUSPEITU-REVIZAUN"
    else:
        return "SEGURO-APROVA"

df = pd.DataFrame({
    "Oras": datas,
    "ID Transasaun": [f"TXN-{100000+i}" for i in range(n)],
    "Kartaun": [f"**** {np.random.randint(1000,9999)}" for _ in range(n)],
    "Montante ($)": montante,
    "Score Fraude": score,
    "Desizaun": [desizaun(s) for s in score],
    "Fonte": np.random.choice(["Bank", "E-Wallet", "Kartao"], n),
    "Nasaun": np.random.choice(["Timor-Leste", "Indonesia", "Australia", "Portugal"], n),
})
df = df.sort_values("Oras", ascending=False)

# --- Sidebar ---
st.sidebar.markdown("## 🛡️ Detesaun Fraude")
pagina = st.sidebar.radio("Pajina", ["📊 Painel Tempu-Real", "📋 Tabela Transasaun"])
st.sidebar.markdown("---")
min_score = st.sidebar.slider("Score minimu", 0.0, 1.0, 0.0)
fonte = st.sidebar.multiselect("Fonte dadus", ["Bank", "E-Wallet", "Kartao"], default=["Bank", "E-Wallet", "Kartao"])
df_f = df[(df["Score Fraude"] >= min_score) & (df["Fonte"].isin(fonte))]

# --- KPI ---
total = len(df_f)
fraude = len(df_f[df_f["Desizaun"] == "FRAUDE-BLOKEIA"])
suspeitu = len(df_f[df_f["Desizaun"] == "SUSPEITU-REVIZAUN"])
seguru = len(df_f[df_f["Desizaun"] == "SEGURO-APROVA"])
taxa = round(fraude / total * 100, 2) if total else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Transasaun", total)
c2.metric("🔴 Fraude Blokeia", fraude)
c3.metric("🟡 Suspeitu", suspeitu)
c4.metric("Taxa Fraude %", f"{taxa}%")

if pagina == "📊 Painel Tempu-Real":
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Desizaun - Distribuisaun")
        fig1 = px.pie(df_f, names="Desizaun", color="Desizaun",
                      color_discrete_map={"SEGURO-APROVA": "green", "SUSPEITU-REVIZAUN": "orange", "FRAUDE-BLOKEIA": "red"})
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        st.subheader("Montante vs Score Fraude")
        fig2 = px.scatter(df_f, x="Montante ($)", y="Score Fraude", color="Desizaun",
                          color_discrete_map={"SEGURO-APROVA": "green", "SUSPEITU-REVIZAUN": "orange", "FRAUDE-BLOKEIA": "red"})
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Transasaun Husi Oras")
    df_f2 = df_f.copy()
    df_f2["Oras_H"] = pd.to_datetime(df_f2["Oras"]).dt.strftime("%H:00")
    cont = df_f2.groupby(["Oras_H", "Desizaun"]).size().reset_index(name="Total")
    fig3 = px.bar(cont, x="Oras_H", y="Total", color="Desizaun",
                  color_discrete_map={"SEGURO-APROVA": "green", "SUSPEITU-REVIZAUN": "orange", "FRAUDE-BLOKEIA": "red"})
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Top 10 Transasaun Suspeitu")
    st.dataframe(df_f.sort_values("Score Fraude", ascending=False).head(10), use_container_width=True)
else:
    st.subheader("📋 Tabela Transasaun Hotu")
    st.dataframe(df_f, use_container_width=True)
    st.download_button("⬇️ Download CSV", df_f.to_csv(index=False), "transasaun_demo.csv")

st.info("💡 Nee mak Modo Demo. Iha sistema real, dadus mai husi PostgreSQL + Spark Streaming. Regra: SEGURO-APROVA • SUSPEITU-REVIZAUN • FRAUDE-BLOKEIA")
