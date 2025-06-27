import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import firestore
from datetime import datetime, timedelta
import json
import os

# Initialize Firestore
@st.cache_resource
def get_firestore_client():
    return firestore.Client()

db = get_firestore_client()

st.set_page_config(page_title="Seismos_KPI_v1", layout="wide")
st.title("🚧 Seismos_KPI_v1 Frac Job Editor")

# --- JOB CREATION SECTION ---
st.markdown("## 📌 Create or Select a Frac Job")
st.markdown("Enter new job details or load an existing one.")

with st.form("job_form"):
    job_id = st.text_input("🔢 Job Number (e.g., 25-052)")
    operator = st.text_input("🏢 Operator Name")
    pad = st.text_input("📍 Pad Name")
    well_count = st.number_input("🔧 Number of Wells", min_value=1, step=1)

    st.markdown("---")
    st.markdown("### 🔁 Well Details")
    wells = {}
    for i in range(well_count):
        cols = st.columns([3, 2])
        well_name = cols[0].text_input(f"Well {i+1} Name")
        stages = cols[1].number_input(f"Stages", min_value=1, step=1, key=f"stages_{i}")
        wells[well_name] = stages

    pattern_type = st.selectbox("🔀 Frack Pattern Type", ["Sequential", "Simul", "Hybrid"])
    simul_group = st.multiselect("🧩 Simul Frac Group (if any)", options=list(wells.keys()))
    solo_wells = [w for w in wells if w not in simul_group]

    submitted = st.form_submit_button("💾 Save Job")
    if submitted and job_id and operator and pad and wells:
        existing_job = db.collection("jobs").document(job_id).get()
        if existing_job.exists:
            st.error(f"❌ Job number '{job_id}' already exists. Please use a different job number.")
        else:
            db.collection("jobs").document(job_id).set({
                "job_id": job_id,
                "operator": operator,
                "pad": pad,
                "wells": wells,
                "pattern_type": pattern_type,
                "simul_group": simul_group,
                "solo_wells": solo_wells,
                "created": datetime.utcnow()
            })
            st.success("✅ Job saved successfully.")

# --- KPI UPLOAD AND TIMELINE ---
st.markdown("## 📄 Upload KPI Report")
st.markdown("Upload the KPI Excel file for your selected job.")

selected_job = st.selectbox("📁 Select Job to Upload KPI", [doc.id for doc in db.collection("jobs").stream()])
uploaded_file = st.file_uploader("📤 Upload KPI Excel File", type=["xls", "xlsx", "xlsm"])

if uploaded_file:
    xls = pd.ExcelFile(uploaded_file)
    if "KPI" in xls.sheet_names:
        kpi_df = xls.parse("KPI")
        kpi_df.columns = kpi_df.columns.str.strip()
        kpi_df["Start time"] = pd.to_datetime(kpi_df["Start time"], errors="coerce")
        kpi_df["End time"] = pd.to_datetime(kpi_df["End time"], errors="coerce")
        kpi_df = kpi_df.dropna(subset=["Start time", "End time"])

        # Save KPI data to Firestore under kpi_data subcollection
        stage_collection = db.collection("jobs").document(selected_job).collection("kpi_data")
        existing_ids = {doc.id for doc in stage_collection.stream()}
        kpi_df["_id"] = kpi_df.apply(lambda row: f"{row['Well Name']}_stage_{int(row['Stage'])}", axis=1)
        new_kpi_df = kpi_df[~kpi_df["_id"].isin(existing_ids)]

        for _, row in new_kpi_df.iterrows():
            doc_id = row["_id"]
            data = row.drop("_id").to_dict()
            stage_collection.document(doc_id).set(data)

        st.success(f"✅ Uploaded {len(new_kpi_df)} new stages to Firestore.")

        st.markdown("### 🕒 Stage Timeline")
        fig = px.scatter(kpi_df, x="Start time", y="Well Name", color="Well Name",
                         hover_data=["Stage"], title="Stage Timeline")
        fig.update_traces(mode="lines+markers")
        st.plotly_chart(fig, use_container_width=True)

        kpi_df["Duration (hrs)"] = (kpi_df["End time"] - kpi_df["Start time"]).dt.total_seconds() / 3600
        grouped = kpi_df.groupby("Well Name")
        job_data = db.collection("jobs").document(selected_job).get().to_dict()

        st.markdown("### 📈 Estimated Pad Completion")
        pad_estimates = []
        for well, group in grouped:
            total_stages = job_data["wells"].get(well, 60)
            stages_done = group.shape[0]
            stages_remaining = total_stages - stages_done
            avg_duration = group["Duration (hrs)"].mean()
            last_end = group["End time"].max()
            est_end = last_end + timedelta(hours=(stages_remaining * avg_duration))

            pad_estimates.append(est_end)
            st.markdown(f"**{well}**: {stages_done}/{total_stages} stages completed")
            st.markdown(f"- Avg stage time: `{avg_duration:.2f} hrs`")
            st.markdown(f"- Estimated well completion: `{est_end.strftime('%B %d, %Y @ %I:%M %p')}`")

        pad_end = max(pad_estimates)
        st.success(f"🟢 **Projected Pad Completion:** {pad_end.strftime('%B %d, %Y @ %I:%M %p')}")

        with st.expander("🧾 Preview KPI Data Table"):
            st.dataframe(kpi_df)

        with st.expander("⏳ Stage Gaps / Idle Time Analysis"):
            for well, group in grouped:
                group = group.sort_values("Start time")
                group["Idle (hrs)"] = group["Start time"].diff().dt.total_seconds() / 3600
                st.markdown(f"#### {well} Idle Times")
                st.dataframe(group[["Stage", "Start time", "End time", "Idle (hrs)"]])

        st.markdown("---")
        st.subheader("📦 Complete Job & Prepare for Archive")
        if st.button("✅ Complete & Archive This Job"):
            try:
                export_dir = f"completed_jobs/{selected_job}"
                os.makedirs(export_dir, exist_ok=True)
                with open(f"{export_dir}/job_metadata.json", "w") as f:
                    json.dump(job_data, f, indent=2, default=str)
                kpi_df.to_csv(f"{export_dir}/kpi_data.csv", index=False)
                quality_entries = db.collection("jobs").document(selected_job).collection("quality").stream()
                quality_data = [q.to_dict() for q in quality_entries]
                pd.DataFrame(quality_data).to_csv(f"{export_dir}/quality_data.csv", index=False)
                st.success(f"✅ Job '{selected_job}' exported to {export_dir}. Ready for upload to Drive.")
            except Exception as e:
                st.error(f"❌ Failed to export job: {e}")

# --- QUALITY CHECKLIST TAB ---
st.markdown("## ✅ Quality Checklist")

jobs_list = [doc.id for doc in db.collection("jobs").stream()]
if jobs_list:
    qc_job = st.selectbox("Select Job for Quality Checklist", jobs_list, key="qc")
    qc_doc = db.collection("jobs").document(qc_job).get().to_dict()
    qc_well = st.selectbox("Well", qc_doc["wells"].keys())
    qc_stage = st.selectbox("Stage #", list(range(1, qc_doc["wells"][qc_well] + 1)))

    with st.form("quality_form"):
        pre_sand = st.selectbox("Pre Sand", ["Good", "Medium", "Bad", "No Usable Drop", "Drop not performed"])
        post_sand = st.selectbox("Post Sand", ["Good", "Medium", "Bad", "No Usable Drop", "No post sand"])
        spp = st.selectbox("SPP", ["Good", "Anomaly", "No Post Sand", "N/A"])
        comments = st.text_area("Comments")
        save_qc = st.form_submit_button("💾 Save Quality Entry")

        if save_qc:
            qc_path = db.collection("jobs").document(qc_job).collection("quality").document(f"{qc_well}_stage_{qc_stage}")
            qc_path.set({
                "well": qc_well,
                "stage": qc_stage,
                "pre_sand": pre_sand,
                "post_sand": post_sand,
                "spp": spp,
                "comments": comments,
                "timestamp": datetime.utcnow()
            })
            st.success("✅ Quality data saved.")

    with st.expander("📋 Saved Quality Entries"):
        entries = db.collection("jobs").document(qc_job).collection("quality").stream()
        data = [e.to_dict() for e in entries]
        if data:
            st.dataframe(pd.DataFrame(data))
        else:
            st.info("No quality checklist data saved yet.")
else:
    st.info("⚠️ No jobs found. Please create one in the section above.")

# --- JOB DELETION ---
st.markdown("## 🗑️ Delete Existing Job")
job_to_delete = st.selectbox("Select a job to delete", jobs_list, key="delete")
if st.button("Delete Selected Job"):
    db.collection("jobs").document(job_to_delete).delete()
    st.success(f"✅ Job '{job_to_delete}' deleted. Refresh the page to update the list.")