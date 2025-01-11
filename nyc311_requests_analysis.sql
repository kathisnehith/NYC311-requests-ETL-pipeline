-- NYC311 Complaints Analysis Queries

-- 1. Locations with the Highest Number of Complaints

-- 1.1. Total complaints by borough
SELECT borough, COUNT(borough) AS count_complaints
FROM `serious-unison-441416-j6.nyc_311requests.location`
GROUP BY borough
ORDER BY COUNT(borough) DESC;

-- 1.2. Complaints by borough with associated zip codes
SELECT borough, COUNT(borough) AS cnt_complaints, ARRAY_AGG(DISTINCT incident_zip) AS zipcodes_borough
FROM `serious-unison-441416-j6.nyc_311requests.location`
GROUP BY borough
ORDER BY COUNT(borough) DESC;

-- 2. Top Complaint Types and Their Frequency
SELECT complaint_type, COUNT(complaint_type) AS complaint_count
FROM `serious-unison-441416-j6.nyc_311requests.complaint`
GROUP BY complaint_type
ORDER BY complaint_count DESC
LIMIT 7;

-- 3. Complaint Types Associated with Specific Locations

-- 3.1. Complaint types by borough and zip code
SELECT
    l.borough,
    l.incident_zip,
    c.complaint_type,
    COUNT(*) AS complaint_count
FROM
    `serious-unison-441416-j6.nyc_311requests.complaint` c
JOIN `serious-unison-441416-j6.nyc_311requests.location` l ON c.unique_key=l.unique_key
GROUP BY
    l.borough, l.incident_zip, c.complaint_type
ORDER BY
    complaint_count DESC;

-- 3.2. Overall most complaint types recorded by borough
WITH topoffens AS (
    SELECT l.borough, c.complaint_type, COUNT(c.complaint_type) AS cnt_complaint
    FROM `serious-unison-441416-j6.nyc_311requests.location` l
    JOIN `serious-unison-441416-j6.nyc_311requests.complaint` c ON l.unique_key = c.unique_key
    GROUP BY l.borough, c.complaint_type
    ORDER BY COUNT(c.complaint_type) DESC
)
SELECT borough, complaint_type
FROM (
    SELECT topoffens.borough, topoffens.complaint_type, topoffens.cnt_complaint,
           RANK() OVER (PARTITION BY topoffens.borough ORDER BY topoffens.cnt_complaint DESC) AS rn
    FROM topoffens
)
WHERE rn = 1
ORDER BY borough ASC;

-- 4. Trends in Complaints Over Time

-- 4.1. Yearly trends
SELECT
    EXTRACT(YEAR FROM c.created_date) AS year,
    COUNT(*) AS total_complaints
FROM `serious-unison-441416-j6.nyc_311requests.case_details` c
GROUP BY year
ORDER BY year DESC;

-- 5. Distribution of Complaints by Time of Day
SELECT
    EXTRACT(HOUR FROM c.created_date) AS hour_of_day,
    COUNT(*) AS total_complaints
FROM `serious-unison-441416-j6.nyc_311requests.case_details` c
GROUP BY hour_of_day
ORDER BY total_complaints DESC;

-- 5.1. Most complaint types recorded by their most frequent month
WITH SAMPLE AS (
    SELECT com.complaint_type,
           (EXTRACT(YEAR FROM c.created_date)) AS yr,
           (EXTRACT(MONTH FROM c.created_date)) AS month_yr,
           COUNT(com.complaint_type) AS Count_complnt
    FROM `serious-unison-441416-j6.nyc_311requests.case_details` c
    JOIN `serious-unison-441416-j6.nyc_311requests.complaint` com ON c.unique_key = com.unique_key
    GROUP BY com.complaint_type, EXTRACT(MONTH FROM c.created_date), EXTRACT(YEAR FROM c.created_date)
    ORDER BY COUNT(com.complaint_type) DESC
), topmnt AS (
    SELECT SAMPLE.complaint_type, SAMPLE.count_complnt AS cnt, SAMPLE.month_yr, SAMPLE.yr,
           ROW_NUMBER() OVER (PARTITION BY complaint_type ORDER BY Count_complnt DESC) AS rn
    FROM SAMPLE
)
SELECT complaint_type, month_yr, yr, cnt
FROM topmnt
WHERE rn = 1
ORDER BY cnt DESC
LIMIT 10;

-- 6. Hotspots for Specific Complaint Types
SELECT
    c.complaint_type,
    c.descriptor,
    l.borough,
    l.incident_zip,
    COUNT(*) AS total_complaints
FROM
    `serious-unison-441416-j6.nyc_311requests.complaint` c
JOIN
    `serious-unison-441416-j6.nyc_311requests.location` l
ON c.unique_key = l.unique_key
GROUP BY
    c.complaint_type, c.descriptor, l.borough, l.incident_zip
ORDER BY
    total_complaints DESC;

-- 7. Agency Handling and Response Rates

-- 7.1. Agencies handling the most complaints
SELECT agency, agency_name, COUNT(*) AS total_complaints
FROM `serious-unison-441416-j6.nyc_311requests.complaint`
GROUP BY agency, agency_name
ORDER BY total_complaints DESC;

-- 7.2. Most complaints handled by each agency
SELECT agency_name, ARRAY_AGG(DISTINCT complaint_type) AS complaints_handles_by_agency, COUNT(agency_name) AS complaint_count
FROM `serious-unison-441416-j6.nyc_311requests.complaint`
GROUP BY agency_name
ORDER BY complaint_count DESC;

-- 7.3. Agency response/clearance time by handled complaints
SELECT com.agency_name, ARRAY_AGG(DISTINCT com.complaint_type) AS complaints_handles_by_agency,
       AVG(DATE_DIFF(c.closed_date, c.created_date, DAY)) AS avg_days_to_resolve,
       AVG(TIMESTAMP_DIFF(c.closed_date, c.created_date, HOUR)) AS avg_hours_to_resolve
FROM `serious-unison-441416-j6.nyc_311requests.case_details` c
JOIN `serious-unison-441416-j6.nyc_311requests.complaint` com ON c.unique_key = com.unique_key
WHERE c.status = 'Closed'
GROUP BY com.agency_name
ORDER BY avg_days_to_resolve ASC
LIMIT 10;

-- 8. Clearance Rate of Complaints
SELECT c.status, com.complaint_type, COUNT(c.status) AS cnt_status,
       ROUND((COUNT(c.status) / SUM(COUNT(c.status)) OVER ()) * 100, 2) AS clearance_rate
FROM `serious-unison-441416-j6.nyc_311requests.case_details` c
JOIN `serious-unison-441416-j6.nyc_311requests.complaint` com ON c.unique_key = com.unique_key
GROUP BY com.complaint_type, c.status
ORDER BY ROUND((COUNT(c.status) / SUM(COUNT(c.status)) OVER ()) * 100, 2) DESC;

-- 9. Frequency of Complaint Status

-- 9.1. Frequency of complaint status
SELECT status, COUNT(status) AS complaint_count
FROM `serious-unison-441416-j6.nyc_311requests.case_details`
GROUP BY status;

-- 9.2. Percentage of unresolved cases by location
SELECT
    c.borough,
    COUNT(CASE WHEN cd.status NOT IN ('Closed', 'Open', 'Started') THEN 1 END) * 100 / COUNT(*) AS unresolved_complaints_perct
FROM `serious-unison-441416-j6.nyc_311requests.location` c
JOIN `serious-unison-441416-j6.nyc_311requests.case_details` cd
ON c.unique_key = cd.unique_key
GROUP BY c.borough
ORDER BY unresolved_complaints_perct ASC;

-- 10. Trends in Case Resolution Over Time

-- 10.1. Monthly trends in case resolution
SELECT
    EXTRACT(MONTH FROM cd.closed_date) AS month,
    COUNT(*) AS resolved_cases
FROM `serious-unison-441416-j6.nyc_311requests.case_details` cd
WHERE closed_date IS NOT NULL AND status = 'Closed'
GROUP BY month
ORDER BY resolved_cases DESC;

-- 10.2. Yearly trends in case resolution
SELECT
    EXTRACT(YEAR FROM cd.closed_date) AS yr,
    COUNT(*) AS resolved_cases
FROM `serious-unison-441416-j6.nyc_311requests.case_details` cd
WHERE closed_date IS NOT NULL AND status = 'Closed'
GROUP BY yr
ORDER BY resolved_cases DESC;

-- 11. Highest Unresolved Rates by Complaint Type or Agency
SELECT
    c.complaint_type,
    c.agency,
    COUNT(CASE WHEN cd.status = 'Open' THEN 1 END) * 100.0 / COUNT(*) AS unresolved_rate
FROM `serious-unison-441416-j6.nyc_311requests.complaint` c
JOIN `serious-unison-441416-j6.nyc_311requests.case_details` cd
ON c.unique_key = cd.unique_key
GROUP BY c.complaint_type, c.agency
ORDER BY unresolved_rate DESC;

-- 12. Variation in Resolution Time by Location or Time of Year
SELECT
    l.borough,
    l.incident_zip,
    EXTRACT(MONTH FROM cd.created_date) AS month,
    AVG(TIMESTAMP_DIFF(cd.closed_date, cd.created_date, HOUR)) AS avg_resolution_time_hours
FROM
    `serious-unison-441416-j6.nyc_311requests.location` l
JOIN
    `serious-unison-441416-j6.nyc_311requests.case_details` cd
ON
    cd.unique_key = l.unique_key
WHERE
    cd.closed_date IS NOT NULL
GROUP BY
    l.borough, l.incident_zip, month
ORDER BY
    avg_resolution_time_hours DESC;

-- 13. Frequency of Cases Involving Vehicles
SELECT COUNT(vehicle_type), vehicle_type
FROM `serious-unison-441416-j6.nyc_311requests.complaint`
WHERE vehicle_type != 'N/A'
GROUP BY vehicle_type;
