-- ============================================
-- NHS Healthcare Database Initialisation
-- ============================================


-- ============================================
-- Create patient_admissions table
-- ============================================

CREATE TABLE IF NOT EXISTS patient_admissions (

    episode_id VARCHAR(20) PRIMARY KEY,

    patient_id VARCHAR(20) NOT NULL,

    patient_name VARCHAR(255) NOT NULL,

    date_of_birth DATE NOT NULL,

    gender VARCHAR(20),

    hospital VARCHAR(255) NOT NULL,

    department VARCHAR(255),

    ward VARCHAR(100),

    consultant VARCHAR(255),

    admission_date DATE NOT NULL,

    admission_type VARCHAR(50) NOT NULL
);


-- ============================================
-- Insert initial patient admission records
-- ============================================

INSERT INTO patient_admissions (
    episode_id,
    patient_id,
    patient_name,
    date_of_birth,
    gender,
    hospital,
    department,
    ward,
    consultant,
    admission_date,
    admission_type
)
VALUES
(
    'E001',
    'P001',
    'John Smith',
    '1981-05-15',
    'Male',
    'St Mary''s Hospital',
    'Cardiology',
    'Ward A',
    'Dr Brown',
    '2026-01-10',
    'Emergency'
),
(
    'E002',
    'P002',
    'Mary Jones',
    '1985-05-08',
    'Female',
    'General Hospital',
    'Orthopaedics',
    'Ward B',
    'Dr White',
    '2026-02-05',
    'Planned'
);