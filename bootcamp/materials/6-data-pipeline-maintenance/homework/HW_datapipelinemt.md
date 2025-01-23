Imagine you're in a group of 4 data engineers, you will be in charge of creating the following things:

You are in charge of managing these 5 pipelines that cover the following business areas:
 
- Profit
 - Unit-level profit needed for experiments
 - Aggregate profit reported to investors
- Growth
 - Aggregate growth reported to investors
 - Daily growth needed for experiments
- Engagement 
 - Aggregate engagement reported to investors

You are in charge of figuring out the following things:

- Who is the primary and secondary owners of these pipelines?
- What is an on-call schedule that is fair
  - Think about holidays too!
- Creating run books for all pipelines that report metrics to investors
  - What could potentially go wrong in these pipelines?
  - (you don't have to recommend a course of action though since this is an imaginatione exercise)

----------------

Assume a set of 4 data engineers who will be managing these pipelines: Chen, Smith, Jones, Kim

Pipeline Ownership looks like this:

1. Unit-level Profit Pipeline
   - Primary: Chen
   - Secondary: Smith

2. Aggregate Profit Pipeline
   - Primary: Smith  
   - Secondary: Jones

3. Aggregate Growth Pipeline
   - Primary: Jones
   - Secondary: Kim

4. Daily Growth Pipeline
   - Primary: Kim
   - Secondary: Chen

5. Aggregate Engagement Pipeline
   - Primary: Chen
   - Secondary: Jones

This ownership structure ensures:
- Each engineer is primary owner of 1-2 pipelines
- Each engineer is secondary owner of 1-2 pipelines
- Workload is distributed pretty evenly
- Critical investor-facing pipelines have experienced backups
  
On-Call Schedule: The team will follow a weekly rotation schedule, with each engineer being on-call for one week at a time. This ensures fair distribution of on-call duties.

Holiday Coverage:
- For major holidays, the on-call schedule will be adjusted 3 months in advance
- Engineers can volunteer for holiday coverage in exchange for extra PTO days
- If no volunteers, holiday coverage will be rotated fairly year-over-year
- Backup support will be provided by the secondary owner during holidays

Potential issues on run books for investor facing pipelines:
   - Missing or delayed source data from finance systems
   - Data quality issues in profit calculations
   - Currency conversion errors
   - Performance issues with large data volumes
   - Missing data from key business units
   - Seasonality not properly accounted for
   - Integration failures with reporting tools
   - User activity data gaps

Common monitoring points for all pipelines:
- Data freshness and latency
- Pipeline execution status
- Data quality metrics
- System resource utilization
- Error rates and types


 