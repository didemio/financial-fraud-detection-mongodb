# Financial Fraud Detection System (MongoDB)

## Overview
This project implements a financial fraud detection system using MongoDB and document-based data modeling.

## Features
- Data cleaning and type correction
- Denormalization using aggregation pipelines
- Risk exposure calculation
- Fraud detection queries

## Technologies
- MongoDB
- NoSQL Data Modeling
- Aggregation Pipeline

## Key Operations
- Converted string fields to numeric types
- Joined collections using $lookup
- Calculated risk exposure using transaction data
- Detected high-risk transactions using aggregation queries

## Project Structure
- scripts/ → MongoDB analysis script

- ## Documentation
- [Full Project Report](report.pdf)

## Sample Query
```javascript
db.transactions_enhanced.aggregate([
  { $match: { risk_exposure: { $gt: 750000 } } },
  { $sort: { risk_exposure: -1 } },
  { $limit: 5 }
])

