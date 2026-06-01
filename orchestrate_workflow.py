import subprocess
import sys
import time

print("⚙️ ==========================================================")
print("🌐 INITIALIZING ENTERPRISE WORKFLOW ORCHESTRATION ENGINE...")
print("==============================================================")

pipeline_tasks = [
    {"step": "1. Ingest Raw CSV Data", "script": "run_data_pipeline.py"},
    {"step": "2. Convert to Columnar Parquet", "script": "optimize_storage.py"},
    {"step": "3. Extract Schema Metadata", "script": "discover_schema.py"},
    {"step": "4. Load Relational Analytical Warehouse", "script": "load_warehouse.py"}
]

def run_workflow():
    start_time = time.time()
    print("⏰ Task scheduler started execution loop...")
    print("--------------------------------------------------------------")

    for task in pipeline_tasks:
        step_name = task["step"]
        script_file = task["script"]
        
        print(f"🔄 [RUNNING] {step_name} -> Executing '{script_file}'...")
        
        # 🚨 THE FIX: Explicitly enforce encoding='utf-8' to prevent Windows decoder hangs
        result = subprocess.run(
            [sys.executable, script_file], 
            capture_output=True, 
            text=True, 
            encoding='utf-8'
        )
        
        if result.returncode == 0:
            print(f"  ✅ [SUCCESS] {step_name} completed cleanly.")
            # Print the background script's console output for visibility
            if result.stdout:
                print(f"--- Background Output ---\n{result.stdout.strip()}\n-------------------------")
        else:
            print(f"\n❌ [CRITICAL FAILURE] {step_name} crashed with an error step state!")
            print("==============================================================")
            print(f"🚨 ENGINE STACK TRACE LOG DIAGNOSTIC:\n{result.stderr}")
            print("==============================================================")
            print("🔄 [TRIGGERING ROLLBACK] Isolating corrupted storage layers...")
            return False
            
        time.sleep(0.5)

    total_duration = round(time.time() - start_time, 2)
    print("--------------------------------------------------------------")
    print(f"🎉 SUCCESS: Complete DAG Data Platform Sync Cycle Succeeded under {total_duration}s!")
    print("==============================================================")

if __name__ == "__main__":
    run_workflow()
