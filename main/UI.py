import streamlit as st
import subprocess
import sys
import tempfile
import time
from collections import deque
import base64
import os


st.set_page_config(page_title="Data Migration Tool", layout="wide")
st.title("\U0001F4BE Data Migration from SQL Server to PostgreSQL")

with st.form("migration_form"):
    st.subheader("SQL Server Credentials")
    sqlserver = st.text_input("SQL Server Host", "localhost,1433")
    sqlserver_db = st.text_input("SQL Server Database")
    sqlserver_user = st.text_input("SQL Server Username")
    sqlserver_pass = st.text_input("SQL Server Password", type="password")

    st.subheader("PostgreSQL Credentials")
    pg_host = st.text_input("PostgreSQL Host", "localhost")
    pg_port = st.text_input("PostgreSQL Port", "5432")
    pg_db = st.text_input("PostgreSQL Database")
    pg_user = st.text_input("PostgreSQL Username")
    pg_pass = st.text_input("PostgreSQL Password", type="password")

    # ADD THESE CHECKBOXES
    st.subheader("Migration Options")
    auto_download_logs = st.checkbox("Auto download logs after finishing", value=True)
    generate_report = st.checkbox("Generate and auto-open report after finishing", value=True)
    include_gemini = st.checkbox("Include Gemini suggestions in the report")

    submitted = st.form_submit_button("Run Migration")

if submitted:
    with st.spinner("Running migration pipeline..."):
        sys_exe = sys.executable
        with tempfile.NamedTemporaryFile(delete=False, mode="w+t") as log_file:
            log_path = log_file.name

        # MAIN CMD (adjust for your pipeline)
        cmd = [
            sys_exe, r"main/data_migration.py",
            "--sqlserver", sqlserver,
            "--sqlserver-db", sqlserver_db,
            "--sqlserver-user", sqlserver_user,
            "--sqlserver-pass", sqlserver_pass,
            "--pg-host", pg_host,
            "--pg-port", pg_port,
            "--pg-db", pg_db,
            "--pg-user", pg_user,
            "--pg-pass", pg_pass
        ]
        # Pass flags for report/gemini if checked
        if generate_report:
            cmd.append("--generate-report")
        if include_gemini:
            cmd.append("--gemini-suggest")

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            output_lines = []
            last_five_logs = deque(maxlen=5)
            prompt_lines = []
            placeholder = st.empty()
            status_container = st.empty()
            checks = {"schema": False, "data": False, "verify": False}

            for line in process.stdout:
                output_lines.append(line)
                last_five_logs.append(line.strip())

                # Update latest logs
                with placeholder.container():
                    st.markdown("**Latest Logs:**")
                    for log_line in list(last_five_logs):
                        st.markdown(f"`{log_line}`")

                # Mark stages complete
                if "Schemas for all tables written to" in line:
                    checks["schema"] = True
                elif "All export scripts and postprocessing generated" in line:
                    checks["data"] = True
                elif "Comparison completed" in line or "All foreign keys are now in sync" in line:
                    checks["verify"] = True

                with status_container.container():
                    st.markdown("### ✅ Migration Stages")
                    st.markdown(f"- {'✅' if checks['schema'] else '⏳'} Schema Migration")
                    st.markdown(f"- {'✅' if checks['data'] else '⏳'} Data Migration")
                    st.markdown(f"- {'✅' if checks['verify'] else '⏳'} Data Verification")

                if "Do you want to drop constraint" in line:
                    prompt_lines.append(line.strip())

                time.sleep(0.1)

            process.wait()

            if process.returncode == 0:
                st.success("Migration completed successfully!")
            else:
                st.error("Migration failed. See logs below.")

            # ==== LOG FILE DOWNLOAD ====
            full_log = "".join(output_lines)
            b64_log = base64.b64encode(full_log.encode()).decode()
            log_download_html = f'<a href="data:file/txt;base64,{b64_log}" download="migration_log.txt" style="font-size:1.1em; font-weight:bold; color:#1d3557;">📥 Download Migration Log</a>'
            if auto_download_logs:
                st.markdown(
                    f"""
                    <div style="background:#ffe9b0; border-radius:10px; padding: 1em; margin: 1em 0; border: 2px solid #b07d13;">
                        <span style="color:#915c01; font-weight:bold;">Logs ready for download:</span><br>
                        {log_download_html}
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.markdown(log_download_html, unsafe_allow_html=True)
                
            # ==== REPORT DOWNLOAD ====
            report_path = "migration_report.html"
            if generate_report:
                # Check if report exists (should be generated by the verify_migration script)
                if os.path.exists(report_path):
                    with open(report_path, "rb") as report_file:
                        report_bytes = report_file.read()
                        b64_report = base64.b64encode(report_bytes).decode()
                        report_download_html = f'<a href="data:file/html;base64,{b64_report}" download="migration_report.html" style="font-size:1.1em; font-weight:bold; color:#26532b;">📝 Download Migration Report</a>'

                        if auto_download_logs:
                            st.markdown(
                                f"""
                                <div style="background:#d0fad5; border-radius:10px; padding: 1em; margin: 1em 0; border: 2px solid #399749;">
                                    <span style="color:#1e4937; font-weight:bold;">Report ready for download:</span><br>
                                    {report_download_html}
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                            st.markdown(
                                f"""
                                <script>
                                var blob = new Blob([atob("{b64_report}")], {{type: "text/html"}});
                                var url = URL.createObjectURL(blob);
                                window.open(url, "_blank");
                                </script>
                                """,
                                unsafe_allow_html=True
                            )
                        else:
                            st.markdown(report_download_html, unsafe_allow_html=True)
                else:
                    st.warning("Report generation was requested, but the report file was not found.")

        except Exception as e:
            st.error(f"Exception occurred: {e}")
