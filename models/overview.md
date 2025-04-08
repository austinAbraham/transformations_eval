# Marketplace KPIs dbt Project

This dbt project implements a data model for analyzing marketplace Key Performance Indicators (KPIs) for financial products.

## Data Flow

1. **Source Data**:
   - Product history (customer searches)
   - Leads generated from searches
   - Revenue Per Lead (RPL) forecasts

2. **Staging Layer**:
   - Standardizes naming
   - Applies basic type casting
   - Prepares data for business logic

3. **Intermediate Layer**:
   - Filters data to relevant time periods
   - Classifies leads by type
   - Joins searches with leads
   - Adds revenue forecasts
   - Deduplicates data

4. **Mart Layer**:
   - Aggregates metrics at customer level
   - Calculates KPIs by product type and lead type
   - Creates the final analytical dataset

## Key Metrics

- **Marketplace Searches**: Count of unique searches
- **Marketplace Leads**: Count of unique leads generated
- **Marketplace Revenue**: Forecasted revenue from leads
- **Breakdowns**: All metrics broken down by product type and lead type (regular vs. zero eligibility)