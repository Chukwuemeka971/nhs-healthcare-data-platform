from datetime import date,datetime

def create_patient_records():

    return [
        {
            "patient_id": "P001",
            "patient_name": "John Smith",
            "age": 45,
            "gender": "Male",
            "hospital": "St Mary's Hospital",
            "department": "Cardiology",
            "ward": "Ward A",
            "consultant": "Dr Brown",
            "admission_date": date(2026, 1, 10),
            "admission_type": "Emergency",
            "created_at": datetime(2026, 1, 10, 10, 0, 0),
            "updated_at": datetime(2026, 1, 10, 10, 0, 0),
            "pipeline_name": "healthcare_pipeline",
            "batch_id": "batch_001",
        },
        {
            "patient_id": "P002",
            "patient_name": "Mary Jones",
            "age": 37,
            "gender": "Female",
            "hospital": "General Hospital",
            "department": "Orthopaedics",
            "ward": "Ward B",
            "consultant": "Dr White",
            "admission_date": date(2026, 2, 5),
            "admission_type": "Elective",
            "created_at": datetime(2026, 2, 5, 10, 0, 0),
            "updated_at": datetime(2026, 2, 5, 10, 0, 0),
            "pipeline_name": "healthcare_pipeline",
            "batch_id": "batch_001",
        },
    ]