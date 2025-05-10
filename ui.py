import streamlit as st
import pandas as pd
from datetime import datetime
import time
from Stream_processing.postgresql_utils.postgresql_client import PostgresSQLClient
from Stream_processing.datastream_api import main as datastream_main
from Stream_processing.kafka_consumer import main as kafka_consumer_main
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize PostgreSQL client
pc = PostgresSQLClient(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

# Set page config
st.set_page_config(
    page_title="Diabetes Prediction",
    page_icon="🟨",
    layout="centered"
)

# Title
st.title("Diabetes Prediction System")

# Form for input
with st.form("prediction_form"):
    st.subheader("Enter Patient Information")
    
    # Create 4 rows with 2 columns each
    col1, col2 = st.columns(2)
    
    with col1:
        pregnancies = st.number_input("Number of Pregnancies", min_value=0, max_value=17)
        blood_pressure = st.number_input("Blood Pressure (mm Hg)", min_value=0, max_value=122)
        insulin = st.number_input("Insulin Level", min_value=0, max_value=846)
        diabetes_pedigree = st.number_input("Diabetes Pedigree Function", min_value=0.078, max_value=2.42,  step=0.001)
    
    with col2:
        glucose = st.number_input("Glucose Level", min_value=0, max_value=199)
        skin_thickness = st.number_input("Skin Thickness (mm)", min_value=0, max_value=99)
        bmi = st.number_input("BMI", min_value=0.0, max_value=67.1, step=0.1)
        age = st.number_input("Age", min_value=21, max_value=81)
    
    # Submit button
    submitted = st.form_submit_button("Predict")

if submitted:
    # Show loading indicator
    with st.spinner("Processing your request..."):

        # Prepare data for insertion
        features = [
            "Pregnancies",
            "Glucose",
            "BloodPressure",
            "SkinThickness",
            "Insulin",
            "BMI",
            "DiabetesPedigreeFunction",
            "Age",
        ]
        
        feature_values = [
            str(pregnancies),
            str(glucose),
            str(blood_pressure),
            str(skin_thickness),
            str(insulin),
            str(bmi),
            str(diabetes_pedigree),
            str(age),
        ]
        
        # Add timestamp and content
        data = [str(round(datetime.now().timestamp() * 1000)), "Hi"] + feature_values
        
        # Insert into database
        query = f"""
            insert into diabetes_new ({",".join(["Created", "Content"] + features)})
            values {tuple(data)}
        """
        
        pc.execute_query(query)
        
        # Wait for Debezium to detect changes
        time.sleep(3)
        
        # Run datastream processing
        st.write("Running datastream processing...")
        # ss = st.session_state
        # if 'run_datastream' not in ss:
        #     datastream_main()
        #     ss.run_datastream = True
        
        # Get prediction from kafka consumer
        result = kafka_consumer_main()
        
        
        # Display final prediction result
        st.success(f"Prediction: {result}")
