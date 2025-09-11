# Deployment Guide

This guide provides detailed instructions for deploying the NYC 311 Data Pipeline using Databricks Asset Bundles.

## Prerequisites

### 1. Databricks Workspace Setup

- Access to a Databricks workspace (Azure, AWS, or GCP)
- Appropriate permissions to create jobs, clusters, and tables
- Access to Unity Catalog (recommended for production)

### 2. Local Environment Setup

**Install Databricks CLI:**
```bash
pip install databricks-cli
```

**Configure Authentication:**

Option A - Using environment variables:
```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

Option B - Using Databricks CLI configuration:
```bash
databricks configure --token
```

### 3. NYC 311 API Setup (Optional)

For better API performance, register for a Socrata API token:
1. Visit [data.cityofnewyork.us](https://data.cityofnewyork.us)
2. Create an account and generate an app token
3. Store the token in Databricks Secrets:
   ```bash
   databricks secrets create-scope nyc311
   databricks secrets put-secret nyc311 app_token
   ```

## Deployment Steps

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd nyc311
```

### Step 2: Review Configuration

Edit `databricks.yml` to customize:
- Cluster configurations
- Schedule timings
- Resource allocations
- Environment-specific settings

### Step 3: Validate Bundle

```bash
databricks bundle validate
```

This checks:
- YAML syntax
- Resource definitions
- Dependency relationships

### Step 4: Deploy to Development

```bash
# Using deployment script (recommended)
./deploy.sh dev

# Or manually
databricks bundle deploy --target dev
```

### Step 5: Test in Development

Run individual jobs to test:

```bash
# Run bronze ingestion
databricks bundle run nyc311_bronze_job --target dev

# Wait for completion, then run silver
databricks bundle run nyc311_silver_job --target dev

# Wait for completion, then run gold
databricks bundle run nyc311_gold_job --target dev
```

### Step 6: Monitor Execution

1. Check job status in Databricks UI
2. Review logs for any errors
3. Validate data quality metrics
4. Check table row counts

### Step 7: Deploy to Production

After successful dev testing:

```bash
./deploy.sh prod
```

## Environment Differences

### Development Environment
- **Cluster Size**: Smaller (2 workers)
- **Data Volume**: Limited batches for faster testing
- **Schedule**: Disabled by default
- **Optimization**: Basic optimization only

### Production Environment
- **Cluster Size**: Larger (4 workers) 
- **Data Volume**: Full data processing
- **Schedule**: Active daily schedules
- **Optimization**: Full optimization with auto-compaction

## Post-Deployment Setup

### 1. Verify Tables

Check that all tables were created:

```sql
-- Check bronze layer
SHOW TABLES IN bronze.nyc311;

-- Check silver layer  
SHOW TABLES IN silver.nyc311;

-- Check gold layer
SHOW TABLES IN gold.nyc311;
```

### 2. Validate Data Quality

Run data quality checks:

```sql
-- Check record counts
SELECT 
  'bronze' as layer,
  COUNT(*) as record_count
FROM bronze.nyc311.nyc311_service_requests

UNION ALL

SELECT 
  'silver' as layer,
  COUNT(*) as record_count  
FROM silver.nyc311.nyc311_service_requests_silver

UNION ALL

SELECT 
  'gold_fact' as layer,
  COUNT(*) as record_count
FROM gold.nyc311.fact_service_requests;
```

### 3. Test Power BI View

```sql
SELECT * FROM gold.nyc311.vw_service_requests_powerbi LIMIT 10;
```

### 4. Schedule Jobs

Enable job schedules for production:

```bash
# Enable bronze job schedule
databricks jobs update-settings --job-id <bronze-job-id> --schedule-enabled true

# Enable silver job schedule  
databricks jobs update-settings --job-id <silver-job-id> --schedule-enabled true

# Enable gold job schedule
databricks jobs update-settings --job-id <gold-job-id> --schedule-enabled true
```

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```
Error: Invalid authentication
```
Solution: Verify DATABRICKS_HOST and DATABRICKS_TOKEN are correct

**2. Permission Errors**
```
Error: User does not have permission to create jobs
```
Solution: Ensure user has workspace admin or appropriate permissions

**3. Cluster Creation Failures**
```
Error: Cannot create cluster
```
Solution: Check workspace limits and node type availability

**4. API Rate Limiting**
```
Error: Too many requests to NYC 311 API
```
Solution: 
- Increase rate_limit_delay in config
- Add Socrata app token
- Reduce batch_size

**5. Schema Evolution Errors**
```
Error: Schema mismatch
```
Solution: Use mergeSchema option or recreate tables

### Debug Commands

**Check bundle status:**
```bash
databricks bundle status --target dev
```

**View job run details:**
```bash
databricks jobs list-runs --job-id <job-id>
```

**Check cluster logs:**
```bash
databricks clusters get-logs --cluster-id <cluster-id>
```

## Rollback Procedures

### Rollback Jobs

```bash
# Destroy current deployment
databricks bundle destroy --target dev --force

# Deploy previous version
git checkout <previous-commit>
databricks bundle deploy --target dev
```

### Rollback Data

```bash
# Restore tables from backup (if available)
RESTORE TABLE bronze.nyc311.nyc311_service_requests TO VERSION AS OF <version>
```

## Performance Optimization

### Production Tuning

1. **Adjust cluster sizes** based on data volume
2. **Enable auto-scaling** for variable workloads
3. **Use photon** for improved performance
4. **Configure table caching** for frequently accessed data

### Cost Optimization

1. **Use job clusters** instead of all-purpose clusters
2. **Enable auto-termination** on clusters
3. **Schedule jobs** during off-peak hours
4. **Use spot instances** where appropriate

## Monitoring and Alerting

### Set up monitoring for:

1. **Job failures** - Alert on any job failures
2. **Data quality** - Monitor null rates and record counts
3. **Performance** - Track job execution times
4. **Costs** - Monitor cluster usage and costs

### Recommended Alerts

```sql
-- Example: Alert if bronze ingestion fails
SELECT 
  'Bronze Ingestion Alert' as alert_type,
  COUNT(*) as records_today
FROM bronze.nyc311.nyc311_service_requests 
WHERE DATE(ingest_ts) = CURRENT_DATE()
HAVING COUNT(*) < 1000; -- Alert if less than expected records
```

## Best Practices

1. **Test in dev first** - Always test changes in development
2. **Use version control** - Track all changes in git
3. **Monitor data quality** - Set up automated quality checks
4. **Document changes** - Keep deployment notes
5. **Backup important data** - Regular table backups
6. **Use CI/CD** - Automate deployments where possible

## Next Steps

After successful deployment:

1. **Connect Power BI** to the gold layer view
2. **Set up monitoring dashboards** 
3. **Create additional analytics views** as needed
4. **Implement data retention policies**
5. **Set up automated testing** for data quality
