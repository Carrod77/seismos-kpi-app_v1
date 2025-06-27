import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore

# --- Firestore Init using st.secrets ---
@st.cache_resource
def get_firestore_client():
    if not firebase_admin._apps:
        cred = credentials.Certificate({
            "type": st.secrets["firebase"]["type"],
            "project_id": st.secrets["firebase"]["project_id"],
            "private_key_id": st.secrets["firebase"]["private_key_id"],
            "private_key": st.secrets["firebase"]["private_key"].replace("\\n", "\n"),
            "client_email": st.secrets["firebase"]["client_email"],
            "client_id": st.secrets["firebase"]["client_id"],
            "auth_uri": st.secrets["firebase"]["auth_uri"],
            "token_uri": st.secrets["firebase"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["firebase"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["firebase"]["client_x509_cert_url"]
        })
        firebase_admin.initialize_app(cred)
    return firestore.client()

db = get_firestore_client()

st.set_page_config(page_title="Seismos Viewer", layout="wide")
st.title("📊 Seismos Viewer - KPI & Quality")

jobs = [doc.id for doc in db.collection("jobs").stream()]
selected_job = st.selectbox("📁 Select Job to View", jobs)

if selected_job:
    job_doc = db.collection("jobs").document(selected_job).get()
    job_data = job_doc.to_dict()
    wells = list(job_data.get("wells", {}).keys())

    kpi_docs = db.collection("jobs").document(selected_job).collection("kpi_data").stream()
    kpi_data = [doc.to_dict() for doc in kpi_docs]
    kpi_df = pd.DataFrame(kpi_data)

    if not kpi_df.empty:
        kpi_df["Start time"] = pd.to_datetime(kpi_df["Start time"], errors="coerce")
        kpi_df["End time"] = pd.to_datetime(kpi_df["End time"], errors="coerce")
        kpi_df = kpi_df.dropna(subset=["Start time", "End time"])

        # Timeline Plot
        st.markdown("## ⏱️ Stage Timeline")
        job_start = kpi_df["Start time"].min()
        job_end_estimate = kpi_df["End time"].max()
        job_duration = job_end_estimate - job_start

        st.info(f"**Job Start:** {job_start.strftime('%B %d, %Y @ %I:%M %p')}")
        st.success(f"**Estimated Pad Completion:** {job_end_estimate.strftime('%B %d, %Y @ %I:%M %p')} ({job_duration.total_seconds()/3600:.1f} hrs total)")

        fig = px.scatter(kpi_df, x="Start time", y="Well Name", color="Well Name", hover_data=["Stage"])
        fig.update_traces(mode="lines+markers")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("## 📌 Stage Completion Summary")
        grouped = kpi_df.groupby("Well Name")
        for well_name, group in grouped:
            total_stages = job_data["wells"].get(well_name)

            # Fuzzy match if needed
            if total_stages is None:
                for saved_name in job_data["wells"].keys():
                    if saved_name in well_name or well_name in saved_name:
                        total_stages = job_data["wells"][saved_name]
                        break

            if total_stages is None:
                total_stages = 0

            completed = group.shape[0]
            remaining = total_stages - completed
            st.markdown(
                f"**{well_name}**: {completed} / {total_stages} stages completed — "
                f"<span style='color:limegreen'><strong>{remaining} remaining</strong></span>",
                unsafe_allow_html=True
            )

        st.markdown("## ✅ Well Quality Summary")
        selected_well = st.selectbox("Select Well", wells)

        if selected_well:
            quality_docs = db.collection("jobs").document(selected_job).collection("quality").stream()
            quality_data = [doc.to_dict() for doc in quality_docs if doc.to_dict().get("well") == selected_well]
            quality_df = pd.DataFrame(quality_data)

            if not quality_df.empty:
                pre_counts = quality_df["pre_sand"].value_counts().to_dict()
                post_counts = quality_df["post_sand"].value_counts().to_dict()

                rate_conditions = sorted(set(list(pre_counts.keys()) + list(post_counts.keys())))
                rate_table = []
                for cond in rate_conditions:
                    pre = pre_counts.get(cond, 0)
                    post = post_counts.get(cond, 0)
                    total = pre + post
                    rate_table.append({"Condition": cond, "Pre": pre, "Post": post, "Total": total})
                rate_df = pd.DataFrame(rate_table)

                total_rate = rate_df["Total"].sum()
                rate_df["Percent Total"] = rate_df["Total"] / total_rate * 100

                st.markdown("### 📊 Rate Drops Summary")
                st.dataframe(rate_df.style.format({"Percent Total": "{:.2f}%"}))

                spp_counts = quality_df["spp"].value_counts().to_dict()
                spp_total = sum(spp_counts.values())
                spp_table = [{
                    "Condition": k,
                    "Count": v,
                    "Percent Total": f"{(v/spp_total)*100:.2f}%"
                } for k, v in spp_counts.items()]
                spp_df = pd.DataFrame(spp_table)

                st.markdown("### 🧪 SPP Summary")
                st.dataframe(spp_df)

            else:
                st.warning("No quality checklist data found for this well.")

        with st.expander("🔍 View All KPI Data"):
            st.dataframe(kpi_df)
    else:
        st.warning("No KPI data available for this job.")
