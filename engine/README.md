# Tauro

**Tauro** is your go-to tool for automating data workflows. Whether you're processing files once a day or continuously streaming data, Tauro makes it simple to build, run, and manage pipelines without complexity.

## What Can Tauro Do?

✅ **Move and transform data** — Read from one place, process, write to another  
✅ **Run on a schedule** — Process data automatically every day, hour, or minute  
✅ **Handle real-time data** — Stream data continuously as it arrives  
✅ **Work with popular formats** — CSV, JSON, Parquet, Delta, and more  
✅ **Easy to configure** — Single settings file for all environments (dev, staging, production)  
✅ **Built-in safety** — Validation and error handling out of the box  
✅ **Web UI** — Visualize pipelines, monitor runs, and view logs in real-time

## Get Started in Minutes

### Step 1: Install Tauro
```bash
pip install tauro
```

**Requirements:**
- Python 3.9 or newer
- (Optional) Spark 3.4+ for large-scale data processing

### Step 1.5: Launch the Web UI (Optional)
```bash
# Start the API server
python -m tauro.api.main

# In another terminal, start the UI
cd tauro/ui
npm install
npm run dev
```

Visit `http://localhost:3000` to:
- 📊 Visualize pipeline DAGs
- 🏃 Monitor running executions
- 📝 View logs in real-time
- ▶️ Trigger pipeline runs

See [`tauro/ui/INTEGRATION.md`](ui/INTEGRATION.md) for details.

### Step 2: Create Your First Project
```bash
tauro --template medallion_basic --project-name my_project
cd my_project
pip install -r requirements.txt
```

### Step 3: See Your Pipelines
```bash
tauro --list-pipelines
```

This shows you all available pipelines ready to run.

### Step 4: Run a Pipeline
```bash
tauro --env dev --pipeline sales_daily_report
```

That's it! Your data pipeline is running.

## Real-World Examples

### Example 1: Daily Sales Report
Process sales data every morning and save a clean report:
```bash
tauro --env production --pipeline daily_sales_summary
```

### Example 2: Verify Before Running
Always test your pipeline before processing real data:
```bash
tauro --env production --pipeline daily_sales_summary --validate-only
```

### Example 3: Process a Specific Date Range
Reprocess historical data from March 1-15:
```bash
tauro --env production --pipeline daily_sales_summary \
  --start-date 2024-03-01 --end-date 2024-03-15
```

### Example 4: Stream Real-Time Data
Continuously ingest data as it arrives:
```bash
tauro --streaming --streaming-command run \
  --streaming-config settings.json \
  --streaming-pipeline real_time_events
```

### Example 5: Check Status of Streaming Job
```bash
tauro --streaming --streaming-command status \
  --streaming-config settings.json
```

### Example 6: Stop a Streaming Job
```bash
tauro --streaming --streaming-command stop \
  --streaming-config settings.json \
  --execution-id my_job_123
```

## Useful Commands at a Glance

| What do you want to do? | Command |
|---|---|
| See all available pipelines | `tauro --list-pipelines` |
| Learn about a specific pipeline | `tauro --pipeline-info sales_pipeline` |
| Get help | `tauro --help` |
| See detailed logs (for troubleshooting) | `tauro --env dev --pipeline my_pipeline --verbose` |
| Quiet mode (less output) | `tauro --env dev --pipeline my_pipeline --quiet` |
| Test without making changes | `tauro --env dev --pipeline my_pipeline --dry-run` |

## How Tauro Organizes Your Work

Tauro uses three simple concepts:

1. **Settings File** — Your master configuration (JSON or YAML). It's like a recipe book.
2. **Environments** — Different settings for different situations (dev for testing, production for real work).
3. **Pipelines** — The actual workflows you want to run (e.g., "daily_sales_report", "customer_etl").

Just point Tauro to your settings file and tell it which environment and pipeline to use. The rest is automatic.

## Best Practices (Keep It Simple)

✓ **Test first** — Always run with `--validate-only` before running for real  
✓ **Use environments** — Keep dev, staging, and production separate  
✓ **Set checkpoints for streaming** — This lets you resume if something fails  
✓ **Atomic formats** — Use Parquet or Delta for reliable production data  
✓ **Dry-run first** — See what will happen before it happens with `--dry-run`

## Troubleshooting

**Problem:** "Command not found"  
**Solution:** Try `python -m tauro --help`

**Problem:** "Can't find my settings file"  
**Solution:** Make sure the file path is correct and use full paths when needed.

**Problem:** Dates are giving errors  
**Solution:** Use YYYY-MM-DD format (e.g., 2024-03-15) and make sure start date is before end date.

**Problem:** My pipeline is failing  
**Solution:** Run with `--verbose` to see detailed logs: 
```bash
tauro --env dev --pipeline my_pipeline --verbose
```

**Problem:** Something went wrong and I need to debug  
**Solution:** Check the generated logs and run with `--verbose` for more details.


## Web UI

Tauro includes a lightweight web interface for visualizing and monitoring your pipelines:

- **Pipeline DAGs**: Visual representation of your pipeline dependencies
- **Run Management**: List, filter, and inspect execution history
- **Real-time Logs**: Stream logs as pipelines execute
- **Quick Actions**: Trigger runs and cancel executions from the UI

**Quick Start:**
```bash
# Backend
python -m tauro.api.main

# Frontend
cd tauro/ui && npm install && npm run dev
```

**Documentation:**
- Architecture: [`tauro/ui/ARCHITECTURE.md`](ui/ARCHITECTURE.md)
- Integration Guide: [`tauro/ui/INTEGRATION.md`](ui/INTEGRATION.md)

## Need More Help?

- Use `tauro --help` to see all commands  
- Use `tauro --pipeline-info <pipeline_name>` to understand a specific pipeline
- Run with `--verbose` to see what's happening  
- Check the logs generated by Tauro for detailed error messages
- Use the Web UI at `http://localhost:3000` for visual monitoring
