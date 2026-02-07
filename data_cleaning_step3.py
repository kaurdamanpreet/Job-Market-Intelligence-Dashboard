# ============================================
# STEP 3: Data Cleaning & Preparation
# Project: Job Market Intelligence Dashboard (Canada)
# Author: Damanpreet Kaur
# ============================================

import pandas as pd
import numpy as np
import re

# --------------------------------------------
# 1. Load raw data
# --------------------------------------------
raw_path = "raw_jobs_data.csv"
df = pd.read_csv(raw_path)

# --------------------------------------------
# 2. Standardize column names
# --------------------------------------------
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# --------------------------------------------
# 3. Rename common column variations (SAFE)
# --------------------------------------------
column_map = {
    "employer": "company",
    "company_name": "company",
    "city": "location",
    "job_location": "location",
    "salary_range": "salary",
    "pay": "salary",
    "job_desc": "job_description",
    "description": "job_description",
}

df.rename(columns=column_map, inplace=True)

# --------------------------------------------
# 4. Remove duplicate job postings
# --------------------------------------------
duplicate_cols = [col for col in ["job_title", "company", "location"] if col in df.columns]

if duplicate_cols:
    df = df.drop_duplicates(subset=duplicate_cols)

# --------------------------------------------
# 5. Handle missing salary values
# --------------------------------------------
if "salary" in df.columns:
    df["salary"] = df["salary"].fillna("Not Disclosed")
else:
    df["salary"] = "Not Disclosed"

# --------------------------------------------
# 6. Infer experience level from job title
# --------------------------------------------
def infer_experience(title):
    title = str(title).lower()
    if any(x in title for x in ["junior", "entry", "new grad", "trainee"]):
        return "Entry-Level"
    elif any(x in title for x in ["senior", "lead", "principal"]):
        return "Senior"
    else:
        return "Mid-Level"

if "experience_level" not in df.columns:
    df["experience_level"] = df["job_title"].apply(infer_experience)

# --------------------------------------------
# 7. Extract province from location
# --------------------------------------------
def extract_province(location):
    location = str(location).lower()
    if "ontario" in location or " on" in location:
        return "Ontario"
    elif "british columbia" in location or " bc" in location:
        return "British Columbia"
    elif "new brunswick" in location or " nb" in location:
        return "New Brunswick"
    elif "alberta" in location or " ab" in location:
        return "Alberta"
    elif "quebec" in location or " qc" in location:
        return "Quebec"
    elif "remote" in location:
        return "Remote"
    else:
        return "Other"
# --------------------------------------------
# 7. Ensure location column exists (FAIL-SAFE)
# --------------------------------------------
possible_location_cols = [
    "location",
    "city",
    "job_location",
    "work_location",
    "region"
]

location_col = None
for col in possible_location_cols:
    if col in df.columns:
        location_col = col
        break

if location_col is None:
    # If no location info exists, create dummy column
    df["location"] = "Unknown"
else:
    df["location"] = df[location_col]

# --------------------------------------------
# 8. Extract province from location
# --------------------------------------------
df["province"] = df["location"].apply(extract_province)


# --------------------------------------------
# 8. Clean and normalize salary data (annual CAD)
# --------------------------------------------
def clean_salary(s):
    if s == "Not Disclosed":
        return np.nan

    s = str(s).lower().replace(",", "")
    numbers = re.findall(r"\d+", s)

    if not numbers:
        return np.nan

    numbers = [int(n) for n in numbers]

    if "hour" in s:
        return np.mean(numbers) * 40 * 52
    elif "k" in s:
        return np.mean(numbers) * 1000
    else:
        return np.mean(numbers)

df["salary_avg"] = df["salary"].apply(clean_salary)

# --------------------------------------------
# 9. Extract in-demand skills
# --------------------------------------------
skills = ["python", "sql", "tableau", "power bi", "excel", "aws"]

for skill in skills:
    df[skill.upper()] = df.get("job_description", "").astype(str).str.lower().str.contains(skill)

# --------------------------------------------
# 10. Final dataset for Tableau
# --------------------------------------------
final_columns = [
    "job_title",
    "company",
    "province",
    "salary_avg",
    "experience_level",
    "job_type",
    "posting_date",
    "PYTHON",
    "SQL",
    "TABLEAU",
    "POWER BI",
    "EXCEL",
    "AWS"
]

final_df = df[[col for col in final_columns if col in df.columns]]

# --------------------------------------------
# 11. Export cleaned data
# --------------------------------------------
output_path = "../data/processed/canada_job_market_cleaned.csv"
final_df.to_csv(output_path, index=False)

print("✅ Step 3 completed successfully. Cleaned dataset saved.")
