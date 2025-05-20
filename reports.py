import datetime
import google.generativeai as genai
from collections import defaultdict
import dotenv as env
from datetime import datetime

def generate_verification_report(
    common_tables, data_mismatches, total_rows_missed, total_source_rows, total_target_rows,
    mean_mismatches, all_means, failed_fks,
    output_path="migration_report.html", gemini_suggest=False
):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = [f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Database Migration Verification Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #f9f9fa; color: #202124; margin: 2em; }}
        h1, h2, h3 {{ color: #144272; }}
        .section {{ margin-bottom: 2em; }}
        .success {{ color: #207227; }}
        .fail {{ color: #b02a1c; }}
        details {{ background: #f3f4f6; border-radius: 7px; padding: 0.5em 1em; margin-top: 1em; }}
        code, pre {{ background: #ececec; border-radius: 4px; padding: 0.2em 0.5em; }}
        .gemini {{ background: #eaf3fb; border-left: 3px solid #3178c6; padding: 1em; margin-top: 1em; }}
    </style>
</head>
<body>
<h1>Database Migration Verification Report</h1>
<p><b>Generated:</b> {timestamp}</p>
<div class="section">
    <h2>Migrated Tables</h2>
    <ul>
"""]

    schema_table_map = defaultdict(list)
    for schema, table in common_tables:
        schema_table_map[schema].append(table)
    for schema in sorted(schema_table_map.keys()):
        tables = ', '.join(sorted(schema_table_map[schema]))
        html.append(f'<li><b>{schema}</b>: {tables}</li>')
    html.append("</ul></div>")

    html.append('<div class="section"><h2>Row Count Mismatches</h2>')
    html.append(f"<b>Total rows in source:</b> {total_source_rows}<br>")
    html.append(f"<b>Total rows in target:</b> {total_target_rows}<br>")
    html.append(f"<b>Total rows missed:</b> {total_rows_missed}<br>")
    if data_mismatches:
        html.append("<ul><b>Missing rows:</b>")
        for schema, table, src, dst in data_mismatches:
            html.append(f"<li>{schema}.{table}: SQL Server = {src}, PostgreSQL = {dst}</li>")
        html.append("</ul>")
    else:
        html.append('<span class="success">All row counts match.</span>')
    html.append("</div>")

    html.append('<div class="section"><h2>Column-wise Mean Mismatch Summary</h2>')
    if mean_mismatches:
        html.append("<ul>")
        for schema, table, col, src_val, dst_val, typ in mean_mismatches:
            html.append(f"<li>{schema}.{table}.{col} [{typ}]: SQL Server = {src_val}, PostgreSQL = {dst_val}</li>")
        html.append("</ul>")
    else:
        html.append('<span class="success">All numeric/datetime means match.</span>')
    html.append("</div>")

    html.append("""
<div class="section">
<details>
<summary><b>Full Mean Dump (click to expand)</b></summary>
<pre>
""")
    for line in all_means:
        html.append(line)
    html.append("</pre></details></div>")

    html.append('<div class="section"><h2>Foreign Keys Failed to Add</h2>')
    if failed_fks:
        html.append("<ul>")
        for (schema, table, col, rs, rt, rc, cname), reason in failed_fks:
            html.append(f"<li>{schema}.{table}.{col} &rarr; {rs}.{rt}.{rc} (<b>{cname}</b>): <code>{reason}</code></li>")
        html.append("</ul>")
    else:
        html.append('<span class="success">All foreign keys added successfully.</span>')
    html.append("</div>")

    # Gemini Suggestions Section
    if gemini_suggest:
        html.append('<div class="section"><h2>🔧 Suggested Fixes (via Gemini)</h2>')
        if failed_fks or mean_mismatches or data_mismatches:
            issues = "\n".join([f"{fk}: {reason}" for fk, reason in failed_fks])
            prompt = (
                f"Suggest fixes for the following database migration errors:\n\n"
                f"{issues}\n\n"
                f"Also, consider these mean mismatches:\n{mean_mismatches}\n"
                f"And these row mismatches:\n{data_mismatches}\n"
                "Give responses in HTML format.\n\n"
            )
            try:
                gemini_key = env.get_key(".env", "GEMINI_API_KEY")
                if gemini_key:
                    genai.configure(api_key=gemini_key)
                    model = genai.GenerativeModel("gemini-2.0-flash")
                    resp = model.generate_content(prompt)
                    # If markdown, show raw (or parse to html)
                    html.append(f'<div class="gemini"><pre>{resp.text.strip()}</pre></div>')
                else:
                    html.append('<span class="fail">Gemini API key not found. Unable to provide suggestions.</span>')
            except Exception as e:
                html.append(f'<span class="fail">Gemini suggestion failed: {e}</span>')
        else:
            html.append('<span class="success">✅ All checks passed without critical issues.</span>')
        html.append("</div>")

    html.append("</body></html>")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html))
