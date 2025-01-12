# NYC 311 Requests - ETL Pipeline and Analysis
![](https://github.com/kathisnehith/NYC311-requests-pipeline-Analysis/blob/main/img/NYC_311%20tools%20design.png)
# Table of Contents

1. [Objective](#objective)  
2. [Stakeholder Impact](#stakeholder-impact)  
3. [Dataset Description](#dataset-description)  
4. [Data Pipeline Design](#pipeline-design)  
5. [Technologies and Techniques Used](#technologies-and-techniques-used)  
    - [Technologies Used](#technologies-used)  
    - [Techniques Employed](#techniques-employed)  
6. [Challenges](#challenges)  
7. [Outputs and Key Findings](#outputs-and-key-findings)  
    - [Breakdown of Complaints by Location](#breakdown-of-complaints-by-location)  
    - [Trends Over Time and Agency Responses](#trends-over-time-and-agency-responses)  
8. [Recommendations](#recommendations)

## Objective
This project analyzes [NYC 311 data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data) consisting of 38 million records to uncover actionable insights into complaint trends, agency performance, and service efficiency. The goal is to support data-driven decision-making and improve resource allocation, ultimately enhancing public satisfaction.

The analysis focuses on:
- **Complaint Trends:** Identifying hotspots and peak times to enable proactive planning.
- **Agency Performance:** Evaluating response effectiveness across different agencies.
- **Time Efficiency:** Analyzing resolution times to streamline processes.
- **Location Prioritization:** Pinpointing high-priority areas for better resource allocation.



## Stakeholder Impact
- **Operational Efficiency:** Provides NYC 311 with recommendations to improve resource allocation and complaint handling.
- **Enhanced Citizen Satisfaction:** Identifies delays and inefficiencies, enabling quicker resolutions in critical areas.
- **Strategic Insights:** Offers data-backed trends and patterns to guide continuous service improvements.

---

## Dataset Description
The dataset consists of over **38 million records** of 311 Service Requests made to NYC since 2010. It serves as a crucial channel for non-emergency city services and citizen interaction.

### Key Characteristics:
- **Update Frequency:** Automatically updated daily to include new requests.
- **Format:** API endpoint(JSON), CSV, Query, OData(Tableau or Excel) with 41 nested columns.
- **Focus:** The analysis uses data from 2021 to the present for trends and insights.

*A detailed ERD of the final transformed data is provided below for better understanding of relationships among 311 requests.* 
![ERD](link-to-erd-image)

---

## Pipeline Design
![Pipeline Design](https://github.com/kathisnehith/NYC311-requests-pipeline-Analysis/blob/main/img/NYC311_pipeline%20design.png)

The data pipeline integrates NYC 311 data and processes it through the following steps:
- Data is extracted from the NYC311 API by Apache Spark on GCP DataProc and stored in GCP Storage Buckets for processing.
- Apache Spark on GCP DataProc is used to clean, preprocess, and aggregate data and then Load into bigquery and storage buckets 
- Apache airflow on GCP Composer is used for workflow orchestration to do ETL for the updated data. 
- Data is transformed and stored in BigQuery for querying, and Tableau dashboards are used to display actionable insights for stakeholders.

---

## Technologies and Techniques Used

### Technologies Used:
- **Google Cloud Platform (GCP):** DataProc, BigQuery, Composer, and Storage Buckets.
- **Apache Spark:** Scalable data transformations.
- **Tableau:** Interactive dashboards.
- **Python:** Scripting and task orchestration.

### Techniques Employed:
- Optimized batch processing, parallelization, data repartitioning, and shuffle.
- Data cleaning, preprocessing, and quality checks.
- Exploratory analysis, statistical methods, and time-series analysis.

---

## Challenges
- **Compute Limitations:** 
  - Only 2 worker nodes (n-standard-2) and 1 master node.
  - Limited GCP trial resources for orchestration.
- **API Constraints:** 
  - Rate limits with OAuth tokens affected data extraction throughput.
- **Data Quality Issues:** 
  - Incomplete or inconsistent records required extensive cleaning and validation.

---

## Outputs and Key Findings
### Breakdown of Complaints by Location
- **Queens:** The 11368 zip code has a very high number of complaints, with Blocked Driveways being the most common issue (23,655 complaints).
- **Brooklyn:** Illegal Parking is a major issue in Brooklyn, specifically in the 11222, 11215, and 11223 zip codes, with each area having over 23,000 complaints.
- **Bronx and Manhattan:** Noise - Residential is the top complaint in both boroughs.
- **Staten Island:** Illegal Parking is the most frequent complaint.

### Trends Over Time and Agency Responses
- **Complaint Volume:** The number of complaints has been steadily increasing from 2021 to 2024, with 2024 having the highest number of complaints at 3,424,632.
- **Peak Complaint Time:** 10 AM is the busiest time for complaint calls, with 795,427 complaints.
- **Seasonal Complaints:** HEAT/HOT WATER complaints are seasonal, with the highest number in December 2024 (62,702 complaints).
- **Agency Workload:** The NYPD handles the most complaints (5,641,082 complaints). The Department of Housing Preservation and Development (HPD) has the longest average response time at about 14 days.
- **Complaint Resolution:** 12,653,008 complaints have been closed. June is the month with the highest number of closed complaints (1,161,037 complaints closed).
- **Unresolved Complaints:** The Bronx has the lowest rate of unresolved complaints at 1.26%.

---

## Recommendations
1. **Task Force Deployment:**
   - Address prevalent issues like Blocked Driveways in Queens' 11368 zip code, Illegal Parking across Brooklyn and Staten Island, and Noise - Residential complaints in the Bronx and Manhattan.
2. **Specialized Teams:**
   - Assign dedicated teams for categories with high unresolved rates, such as Sweeping/Missed-Inadequate complaints.
3. **Call Center Optimization:**
   - Increase operator capacity during peak times (10 AM) and expand digital self-service tools for common complaints like Illegal Parking or Noise.
4. **Seasonal Preparedness:**
   - Establish a “Winter Response Task Force” in December for HEAT/HOT WATER complaints and conduct regular maintenance of heating systems before winter.
5. **Cross-Agency Collaboration:**
   - Create task forces to manage overlapping issues like Illegal Parking (NYPD, Department of Transportation) or Sweeping complaints.

---

## Acknowledgments
Thank you to NYC Open Data for providing access to the 311 Service Requests dataset. This project was conducted using free-tier GCP resources and open-source tools, showcasing the potential of scalable data analysis even with limited resources.
