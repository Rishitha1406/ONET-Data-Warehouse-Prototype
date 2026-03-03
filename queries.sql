-- O*NET Data Warehouse – SQL Queries

-- QUERY 1: Top 10 Highest-Paying Occupations
-- Joins dim_occupation + fact_wages, orders by median annual wage.
-- Useful for surfacing high-opportunity careers in Tulsa workforce programs.
SELECT
    o.onetsoc_code,
    o.title,
    o.soc_major_group,
    w.annual_median_wage,
    w.hourly_median_wage,
    w.annual_90th_pct - w.annual_10th_pct AS wage_spread
FROM dim_occupation o
JOIN fact_wages w ON o.onetsoc_code = w.onetsoc_code
ORDER BY w.annual_median_wage DESC
LIMIT 10;

/* 
Result:
| onetsoc_code   | title                                          |   soc_major_group |   annual_median_wage |   hourly_median_wage | wage_spread   |
|:---------------|:-----------------------------------------------|------------------:|---------------------:|---------------------:|:--------------|
| 29-1215.00     | Family Medicine Physicians                     |                29 |               238380 |               114.61 |               |
| 29-1216.00     | General Internal Medicine Physicians           |                29 |               236350 |               113.63 |               |
| 53-2011.00     | Airline Pilots, Copilots, And Flight Engineers |                53 |               226600 |               nan    |               |
| 29-1029.00     | Dentists, All Other Specialists                |                29 |               225770 |               108.54 |               |
| 29-1151.00     | Nurse Anesthetists                             |                29 |               223210 |               107.31 |               |
| 29-1221.00     | Pediatricians, General                         |                29 |               210130 |               101.03 |               |
| 11-1011.00     | Chief Executives                               |                11 |               206420 |                99.24 |               |
| 29-1021.00     | Dentists, General                              |                29 |               172790 |                83.07 |               |
| 11-3021.00     | Computer And Information Systems Managers      |                11 |               171200 |                82.31 |               |
| 11-9041.00     | Architectural And Engineering Managers         |                11 |               167740 |                80.64 |               |
*/

-- QUERY 2: Average Skill Importance by Occupation Group
-- Aggregates fact_skills (scale IM = Importance) to the SOC major group.
-- Helps understand which occupation sectors demand the highest skill levels.
SELECT
    o.soc_major_group,
    COUNT(DISTINCT o.onetsoc_code)    AS occupation_count,
    ROUND(AVG(s.data_value), 2)       AS avg_skill_importance,
    ROUND(MAX(s.data_value), 2)       AS max_skill_importance,
    ROUND(MIN(s.data_value), 2)       AS min_skill_importance
FROM fact_skills s
JOIN dim_occupation o ON s.onetsoc_code = o.onetsoc_code
WHERE s.scale_id = 'IM'
GROUP BY o.soc_major_group
ORDER BY avg_skill_importance DESC;

/*
Result:
|   soc_major_group |   occupation_count |   avg_skill_importance |   max_skill_importance |   min_skill_importance |
|------------------:|-------------------:|-----------------------:|-----------------------:|-----------------------:|
|                17 |                 55 |                   2.9  |                   4.25 |                      1 |
|                11 |                 54 |                   2.9  |                   4.88 |                      1 |
|                29 |                 82 |                   2.79 |                   4.62 |                      1 |
|                19 |                 59 |                   2.79 |                   4.75 |                      1 |
|                15 |                 31 |                   2.77 |                   5    |                      1 |
|                49 |                 50 |                   2.72 |                   4.88 |                      1 |
|                21 |                 14 |                   2.72 |                   5    |                      1 |
|                25 |                 61 |                   2.67 |                   4.88 |                      1 |
|                13 |                 45 |                   2.61 |                   4.62 |                      1 |
|                33 |                 25 |                   2.53 |                   4.38 |                      1 |
|                27 |                 38 |                   2.5  |                   4.88 |                      1 |
|                53 |                 49 |                   2.48 |                   4.88 |                      1 |
|                23 |                  7 |                   2.47 |                   5    |                      1 |
|                47 |                 61 |                   2.45 |                   4.75 |                      1 |
|                51 |                107 |                   2.42 |                   4.12 |                      1 |
|                41 |                 21 |                   2.42 |                   4.25 |                      1 |
|                37 |                  8 |                   2.42 |                   4    |                      1 |
|                45 |                 12 |                   2.39 |                   4.12 |                      1 |
|                31 |                 19 |                   2.39 |                   4.12 |                      1 |
|                39 |                 29 |                   2.3  |                   4.12 |                      1 |
|                43 |                 51 |                   2.29 |                   4.38 |                      1 |
|                35 |                 16 |                   2.22 |                   4.12 |                      1 |
*/


-- QUERY 3: Occupations with the Highest "Critical Thinking" Importance
-- Filters fact_skills for element "Critical Thinking" at the Importance scale.
-- Identifies roles that most demand analytical reasoning.
SELECT
    o.title,
    o.onetsoc_code,
    s.element_name,
    s.data_value  AS importance_score,
    w.annual_median_wage
FROM fact_skills s
JOIN dim_occupation o ON s.onetsoc_code = o.onetsoc_code
LEFT JOIN fact_wages w ON o.onetsoc_code = w.onetsoc_code
WHERE s.element_name = 'Critical Thinking'
  AND s.scale_id = 'IM'
ORDER BY s.data_value DESC
LIMIT 10;
/*
Result:
| title                                                    | onetsoc_code   | element_name      |   importance_score |   annual_median_wage |
|:---------------------------------------------------------|:---------------|:------------------|-------------------:|---------------------:|
| Judges, Magistrate Judges, And Magistrates               | 23-1023.00     | Critical Thinking |               4.88 |               156210 |
| Education Administrators, Kindergarten Through Secondary | 11-9032.00     | Critical Thinking |               4.5  |               104070 |
| Lawyers                                                  | 23-1011.00     | Critical Thinking |               4.5  |               151160 |
| Anesthesiologists                                        | 29-1211.00     | Critical Thinking |               4.5  |                  nan |
| Chief Executives                                         | 11-1011.00     | Critical Thinking |               4.38 |               206420 |
| Emergency Medicine Physicians                            | 29-1214.00     | Critical Thinking |               4.38 |                  nan |
| Family Medicine Physicians                               | 29-1215.00     | Critical Thinking |               4.38 |               238380 |
| Obstetricians And Gynecologists                          | 29-1218.00     | Critical Thinking |               4.38 |                  nan |
| Security Managers                                        | 11-3013.01     | Critical Thinking |               4.25 |                  nan |
| Actuaries                                                | 15-2011.00     | Critical Thinking |               4.25 |               125770 |
*/

-- QUERY 4: Wage Distribution by Education Requirement
-- Joins education requirements (category = level of degree) with wages.
-- Category key: 1=Some HS, 2=HS Diploma, 3=Some College, 4=Bachelor's, 5=Graduate
-- Shows wage returns on education investment – useful for advising job seekers.
SELECT
    e.category                           AS education_level_code,
    CASE e.category
        WHEN 1 THEN 'No HS Diploma'
        WHEN 2 THEN 'HS Diploma / GED'
        WHEN 3 THEN 'Some College / Assoc.'
        WHEN 4 THEN "Bachelor's Degree"
        WHEN 5 THEN 'Graduate Degree'
        WHEN 6  THEN 'Post-Baccalaureate'
        WHEN 7  THEN 'Master''s Degree'
        WHEN 8  THEN 'Post-Master''s'
        WHEN 9  THEN 'First Professional Degree'
        WHEN 11 THEN 'Doctoral Degree'
        WHEN 12 THEN 'Post-Doctoral'
    END                                  AS education_label,
    COUNT(DISTINCT e.onetsoc_code)       AS occupations,
    ROUND(AVG(w.annual_median_wage), 0)  AS avg_median_wage,
    ROUND(AVG(w.hourly_median_wage), 2)  AS avg_hourly_wage
FROM fact_education e
JOIN dim_occupation o ON e.onetsoc_code = o.onetsoc_code
JOIN fact_wages w     ON o.onetsoc_code = w.onetsoc_code
WHERE e.element_name = 'Required Level of Education'
  AND e.data_value > 50          -- majority required at this level
GROUP BY e.category
ORDER BY e.category;
/*
Result:
|   education_level_code | education_label           |   occupations |   avg_median_wage |   avg_hourly_wage |
|-----------------------:|:--------------------------|--------------:|------------------:|------------------:|
|                      1 | No HS Diploma             |            26 |             41928 |             20.16 |
|                      2 | HS Diploma / GED          |           188 |             50237 |             24.11 |
|                      3 | Some College / Assoc.     |            18 |             57977 |             27.87 |
|                      4 | Bachelor's Degree         |             2 |             43175 |             20.76 |
|                      5 | Graduate Degree           |            17 |             77444 |             37.23 |
|                      6 | Post-Baccalaureate        |            97 |             89867 |             43.57 |
|                      7 | Master's Degree           |             1 |             73850 |             35.5  |
|                      8 | Post-Master's             |            26 |             86163 |             42.08 |
|                      9 | First Professional Degree |             1 |             86930 |             41.79 |
|                     11 | Doctoral Degree           |            36 |            109516 |             68.52 |
|                     12 | Post-Doctoral             |            12 |            223240 |            107.33 |
*/

-- QUERY 5: Top Knowledge Domains for Technology Occupations (SOC 15-xxxx)
-- Aggregates fact_knowledge for the "15" SOC major group (Computers/IT).
-- Highlights training priorities for workforce development programs.
SELECT
    k.element_name                      AS knowledge_domain,
    COUNT(DISTINCT k.onetsoc_code)      AS occupation_count,
    ROUND(AVG(k.data_value), 2)         AS avg_importance,
    ROUND(MAX(k.data_value), 2)         AS max_importance
FROM fact_knowledge k
JOIN dim_occupation o ON k.onetsoc_code = o.onetsoc_code
WHERE o.soc_major_group = '15'
  AND k.scale_id = 'IM'
GROUP BY k.element_name
ORDER BY avg_importance DESC;
/*
Result:
| knowledge_domain              |   occupation_count |   avg_importance |   max_importance |
|:------------------------------|-------------------:|-----------------:|-----------------:|
| Computers and Electronics     |                 31 |             4.38 |             4.96 |
| English Language              |                 31 |             3.73 |             4.29 |
| Mathematics                   |                 31 |             3.51 |             4.93 |
| Engineering and Technology    |                 31 |             3.16 |             4.25 |
| Customer and Personal Service |                 31 |             3.12 |             4.24 |
| Administration and Management |                 31 |             2.99 |             4    |
| Education and Training        |                 31 |             2.89 |             3.72 |
| Design                        |                 31 |             2.78 |             4.26 |
| Telecommunications            |                 31 |             2.66 |             4.9  |
| Administrative                |                 31 |             2.53 |             3.45 |
| Communications and Media      |                 31 |             2.51 |             4.1  |
| Economics and Accounting      |                 31 |             2.19 |             3.78 |
| Production and Processing     |                 31 |             2.18 |             3.71 |
| Law and Government            |                 31 |             2.18 |             3.45 |
| Public Safety and Security    |                 31 |             2.17 |             3.27 |
| Personnel and Human Resources |                 31 |             2.15 |             3.14 |
| Sales and Marketing           |                 31 |             2    |             3    |
| Psychology                    |                 31 |             1.98 |             3.3  |
| Geography                     |                 31 |             1.73 |             4.71 |
| Sociology and Anthropology    |                 31 |             1.7  |             3    |
| Mechanical                    |                 31 |             1.68 |             3.66 |
| Physics                       |                 31 |             1.63 |             3    |
| Biology                       |                 31 |             1.5  |             3.8  |
| Medicine and Dentistry        |                 31 |             1.48 |             3.7  |
| Transportation                |                 31 |             1.44 |             2.1  |
| Building and Construction     |                 31 |             1.4  |             2.67 |
| Chemistry                     |                 31 |             1.37 |             2.75 |
| Foreign Language              |                 31 |             1.35 |             1.9  |
| Philosophy and Theology       |                 31 |             1.32 |             1.83 |
| History and Archeology        |                 31 |             1.32 |             2.08 |
| Therapy and Counseling        |                 31 |             1.28 |             2.1  |
| Fine Arts                     |                 31 |             1.25 |             2.65 |
| Food Production               |                 31 |             1.08 |             1.33 |
*/


-- QUERY 6: Full Occupation Profile View (sample – Software Developers)
-- Multi-join across all fact tables for a single occupation.
-- Demonstrates warehouse completeness and query flexibility.
SELECT
    o.title,
    o.description,
    w.annual_median_wage,
    w.hourly_median_wage,
    s.element_name   AS top_skill,
    s.data_value     AS skill_importance,
    k.element_name   AS top_knowledge,
    k.data_value     AS knowledge_importance
FROM dim_occupation o
JOIN fact_wages    w ON o.onetsoc_code = w.onetsoc_code
JOIN fact_skills   s ON o.onetsoc_code = s.onetsoc_code AND s.scale_id = 'IM'
JOIN fact_knowledge k ON o.onetsoc_code = k.onetsoc_code AND k.scale_id = 'IM'
WHERE o.onetsoc_code = '15-1252.00'
ORDER BY s.data_value DESC, k.data_value DESC
LIMIT 10;
/*
Result:
title,description,annual_median_wage,hourly_median_wage,top_skill,skill_importance,top_knowledge,knowledge_importance
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Computers and Electronics,4.75
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Mathematics,3.57
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Customer and Personal Service,3.56
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,English Language,3.27
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Education and Training,2.84
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Engineering and Technology,2.8
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Design,2.7
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Telecommunications,2.62
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Public Safety and Security,2.57
Software Developers,"Research, design, and develop computer and network software or specialized utility programs. Analyze user needs and develop software solutions, applying principles and techniques of computer science, engineering, and mathematical analysis. Update software or enhance existing software capabilities. May work with computer hardware engineers to integrate hardware and software systems, and develop specifications and performance requirements. May maintain databases within an application area, working individually or coordinating database development as part of a team.",133080.0,63.98,Programming,4.0,Production and Processing,2.54
*/
