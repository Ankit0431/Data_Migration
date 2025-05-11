import streamlit as st
import subprocess
import sys
import tempfile
import time
from collections import deque
import base64

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

    submitted = st.form_submit_button("Run Migration")

if submitted:
    with st.spinner("Running migration pipeline..."):
        sys_exe = sys.executable
        with tempfile.NamedTemporaryFile(delete=False, mode="w+t") as log_file:
            log_path = log_file.name

        cmd = [
            sys_exe, "data_migration.py",
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

                # Collect prompt lines for later interaction
                if "Do you want to drop constraint" in line:
                    prompt_lines.append(line.strip())

                time.sleep(0.1)

            process.wait()

            if process.returncode == 0:
                st.success("Migration completed successfully!")
            else:
                st.error("Migration failed. See logs below.")

            full_log = "".join(output_lines)
            st.subheader("Full Logs")
            st.code(full_log, language="bash")

            # Downloadable log
            b64 = base64.b64encode(full_log.encode()).decode()
            href = f'<a href="data:file/txt;base64,{b64}" download="migration_log.txt">📥 Download Log File</a>'
            st.markdown(href, unsafe_allow_html=True)

            # Handle unresolved prompts after subprocess ends
            if prompt_lines:
                st.warning("Some actions require your input. Please review below:")
                for i, prompt in enumerate(prompt_lines):
                    st.radio(f"{prompt}", ["Yes", "No", "All"], key=f"post_prompt_{i}")
                st.button("Acknowledge and proceed")

        except Exception as e:
            st.error(f"Exception occurred: {e}")
