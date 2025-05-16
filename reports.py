import datetime
import google.generativeai as genai
from collections import defaultdict
import dotenv as env
from datetime import datetime


def generate_verification_report(common_tables, data_mismatches, mean_mismatches, all_means, failed_fks, output_path="migration_report.md"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report_lines = [f"# Database Migration Verification Report\nGenerated: {timestamp}\n"]

    # Section 1: Tables Migrated
    report_lines.append("## Migrated Tables")
    schema_table_map = defaultdict(list)
    for schema, table in common_tables:
        schema_table_map[schema].append(table)
    for schema in sorted(schema_table_map.keys()):
        tables = ', '.join(sorted(schema_table_map[schema]))
        report_lines.append(f"- **{schema}**: {tables}")

    # Section 2: Row Count Mismatches
    report_lines.append("\n## Row Count Mismatches")
    if data_mismatches:
        for schema, table, src, dst in data_mismatches:
            report_lines.append(f"- {schema}.{table}: SQL Server = {src}, PostgreSQL = {dst}")
    else:
        report_lines.append("All row counts match.")

    # Section 3: Mean Mismatches
    report_lines.append("\n## Column-wise Mean Summary")
    if mean_mismatches:
        for schema, table, col, src_val, dst_val, typ in mean_mismatches:
            report_lines.append(f"- {schema}.{table}.{col} [{typ}]: SQL Server = {src_val}, PostgreSQL = {dst_val}")
    else:
        report_lines.append("All numeric/datetime means match.")

    report_lines.append("\n<details><summary>📊 Full Mean Dump</summary>\n\n```")
    report_lines.extend(all_means)
    report_lines.append("```\n</details>")

    # Section 4: Foreign Key Failures
    report_lines.append("\n## Foreign Keys Failed to Add")
    if failed_fks:
        for (schema, table, col, rs, rt, rc, cname), reason in failed_fks:
            report_lines.append(f"- {schema}.{table}.{col} → {rs}.{rt}.{rc} ({cname}): `{reason}`")
    else:
        report_lines.append("All foreign keys added successfully.")

    # Section 5: Gemini Suggestions
    if failed_fks or mean_mismatches or data_mismatches:
        report_lines.append("\n## 🔧 Suggested Fixes (via Gemini)")
        issues = "\n".join([f"{fk}: {reason}" for fk, reason in failed_fks])
        prompt = (
            f"Suggest fixes for the following database migration errors:\n\n"
            f"{issues}\n\n"
            f"Also, consider these mean mismatches:\n{mean_mismatches}\n"
            f"And these row mismatches:\n{data_mismatches}\n"
            "Give responses in markdown format.\n\n"
        )
        try:
            gemini_key = env.get_key(".env", "GEMINI_API_KEY")
            if gemini_key:
                genai.configure(api_key=gemini_key)
                model = genai.GenerativeModel("gemini-2.0-flash")
                resp = model.generate_content(prompt)
                report_lines.append(resp.text.strip())
            else:
                report_lines.append("_Gemini API key not found. Unable to provide suggestions._")
        except Exception as e:
            report_lines.append(f"_Gemini suggestion failed: {e}_")
    else:
        report_lines.append("\n## ✅ All checks passed without critical issues.")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
