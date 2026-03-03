"""
run_queries.py
Executes the analytical SQL queries against the warehouse and prints results.
"""

import sqlite3
import os
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "warehouse.db")

QUERIES = {
    "Q1 – Top 10 Highest-Paying Occupations": """
        SELECT o.title, w.annual_median_wage, w.hourly_median_wage,
               w.annual_90th_pct - w.annual_10th_pct AS wage_spread
        FROM dim_occupation o
        JOIN fact_wages w ON o.onetsoc_code = w.onetsoc_code
        ORDER BY w.annual_median_wage DESC
        LIMIT 10
    """,

    "Q2 – Avg Skill Importance by SOC Major Group": """
        SELECT o.soc_major_group,
               COUNT(DISTINCT o.onetsoc_code) AS occupation_count,
               ROUND(AVG(s.data_value), 2)    AS avg_skill_importance
        FROM fact_skills s
        JOIN dim_occupation o ON s.onetsoc_code = o.onetsoc_code
        WHERE s.scale_id = 'IM'
        GROUP BY o.soc_major_group
        ORDER BY avg_skill_importance DESC
    """,

    "Q3 – Top Occupations for Critical Thinking": """
        SELECT o.title, s.data_value AS importance_score, w.annual_median_wage
        FROM fact_skills s
        JOIN dim_occupation o ON s.onetsoc_code = o.onetsoc_code
        LEFT JOIN fact_wages w ON o.onetsoc_code = w.onetsoc_code
        WHERE s.element_name = 'Critical Thinking' AND s.scale_id = 'IM'
        ORDER BY s.data_value DESC
        LIMIT 10
    """,

    "Q4 – Wage Distribution by Education Level": """
        SELECT
            e.category AS edu_code,
            CASE e.category
                WHEN 1 THEN 'No HS Diploma'
                WHEN 2 THEN 'HS Diploma / GED'
                WHEN 3 THEN 'Some College / Assoc.'
                WHEN 4 THEN 'Bachelor Degree'
                WHEN 5 THEN 'Graduate Degree'
                ELSE 'Other'
            END AS education_label,
            COUNT(DISTINCT e.onetsoc_code)      AS occupations,
            ROUND(AVG(w.annual_median_wage), 0) AS avg_median_wage
        FROM fact_education e
        JOIN dim_occupation o ON e.onetsoc_code = o.onetsoc_code
        JOIN fact_wages w     ON o.onetsoc_code = w.onetsoc_code
        WHERE e.element_name = 'Required Level of Education'
        GROUP BY e.category
        ORDER BY e.category
    """,

    "Q5 – Top Knowledge Domains for Tech Occupations (SOC 15)": """
        SELECT k.element_name AS knowledge_domain,
               COUNT(DISTINCT k.onetsoc_code) AS occ_count,
               ROUND(AVG(k.data_value), 2)    AS avg_importance
        FROM fact_knowledge k
        JOIN dim_occupation o ON k.onetsoc_code = o.onetsoc_code
        WHERE o.soc_major_group = '15' AND k.scale_id = 'IM'
        GROUP BY k.element_name
        ORDER BY avg_importance DESC
    """,
}


def run():
    conn = sqlite3.connect(DB_PATH)
    pd.set_option("display.max_colwidth", 50)
    pd.set_option("display.width", 120)

    for name, sql in QUERIES.items():
        print(f"\n{'='*60}")
        print(f"  {name}")
        print('='*60)
        df = pd.read_sql_query(sql, conn)
        print(df.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    run()
