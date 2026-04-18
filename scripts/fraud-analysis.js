/* ================================================================================
   STUDENT HEADER
   Course: Data models in database systems
   Practical Work 3: Document databases and hierarchical data model
   
   Student Name, Surname:  [Didem Nur Mutlu]
   Student ID:             [231ADB286]
   Variation #:            19
================================================================================ */

// --- SECTION 2: DATABASE SETUP ---
use('financeDB');
print(">>> Database 'financeDB' selected.");


/* ================================================================================
   SECTION 3: TASK 3 - DATA TYPE CORRECTION
   Purpose: Convert string data types to their correct formats for analytical processing
   Collections affected: merchants, accounts, transactions
================================================================================ */

print("\n>>> Running Task 3: Type Correction...");

// 3.1 Fix Collection 1: merchants
print("\n[3.1] Fixing merchants collection: risk_score String → Int32");
db.merchants.updateMany(
  { risk_score: { $type: "string" } },
  [{ 
    $set: { 
      risk_score: { $toInt: "$risk_score" }
    } 
  }]
);
print("✓ merchants.risk_score converted to Int32");


// 3.2 Fix Collection 2: accounts
print("\n[3.2] Fixing accounts collection: account_limit & balance String → Double");
db.accounts.updateMany(
  { 
    $or: [
      { account_limit: { $type: "string" } },
      { balance: { $type: "string" } }
    ]
  },
  [{
    $set: {
      account_limit: { $toDouble: "$account_limit" },
      balance: { $toDouble: "$balance" }
    }
  }]
);
print("✓ accounts.account_limit and accounts.balance converted to Double");


// 3.3 Fix Collection 3: transactions
print("\n[3.3] Fixing transactions collection: amount String → Double, timestamp String → Date");
db.transactions.updateMany(
  {
    $or: [
      { amount: { $type: "string" } },
      { timestamp: { $type: "string" } }
    ]
  },
  [{
    $set: {
      amount: { $toDouble: "$amount" },
      timestamp: { $toDate: "$timestamp" }
    }
  }]
);
print("✓ transactions.amount converted to Double, transactions.timestamp converted to Date");

print("\nTask 3 Complete: All data types have been corrected.");


/* ================================================================================
   SECTION 4: TASK 4 - DENORMALIZATION
   Purpose: Create an enhanced collection by embedding merchant details into transactions
            and calculating risk exposure for fraud analysis
   Output: New collection "transactions_enhanced"
================================================================================ */

print("\n>>> Running Task 4: Creating transactions_enhanced...");

db.transactions.aggregate([
  {
    $lookup: {
      from: "merchants",
      localField: "merchant_id",
      foreignField: "merchant_id",
      as: "merchant_details"
    }
  },
  {
    $unwind: "$merchant_details"
  },
  {
    $addFields: {
      risk_exposure: {
        $multiply: ["$amount", "$merchant_details.risk_score"]
      }
    }
  },
  {
    $out: "transactions_enhanced"
  }
]);

print("✓ Task 4 Complete: Collection 'transactions_enhanced' created with embedded merchant data and calculated risk_exposure.");


/* ================================================================================
   SECTION 5: TASK 5 - ANALYTICAL QUERIES 
   Purpose: Perform advanced aggregations on the enhanced dataset for fraud detection
================================================================================ */

print("\n>>> Running Task 5 Queries...");

// --- Query 5.1: Category Risk Analysis ---
print("\n[Output 5.1] Category Risk Analysis ------------------------------------------------");

var result_5_1 = db.transactions_enhanced.aggregate([
  {
    $group: {
      _id: "$merchant_details.category",
      total_amount: { $sum: "$amount" }
    }
  },
  {
    $project: {
      _id: 0,
      category: "$_id",
      total_amount: 1
    }
  },
  {
    $sort: { total_amount: -1 }
  },
  {
    $limit: 6
  }
]).toArray();

printjson(result_5_1);


// --- Query 5.2: High-Risk Alert ---
print("\n[Output 5.2] High-Risk Alert ------------------------------------------------");

var result_5_2 = db.transactions_enhanced.aggregate([
  {
    $match: {
      risk_exposure: { $gt: 750000 }
    }
  },
  {
    $project: {
      _id: 0,
      transaction_id: "$_id",
      merchant_name: "$merchant_details.name",
      risk_exposure: 1
    }
  },
  {
    $sort: { risk_exposure: -1 }
  },
  {
    $limit: 5
  }
]).toArray();

printjson(result_5_2);


// --- Query 5.3: City Spending Hotspots ---
print("\n[Output 5.3] City Spending Hotspots ------------------------------------------------");

var result_5_3 = db.transactions_enhanced.aggregate([
  {
    $group: {
      _id: "$merchant_details.city",
      average_amount: { $avg: "$amount" }
    }
  },
  {
    $project: {
      _id: 0,
      city: "$_id",
      average_amount: 1
    }
  },
  {
    $sort: { average_amount: -1 }
  },
  {
    $limit: 3
  }
]).toArray();

printjson(result_5_3);


// --- Query 5.4: Top Merchant Activity ---
print("\n[Output 5.4] Top Merchant Activity ------------------------------------------------");

var result_5_4 = db.transactions_enhanced.aggregate([
  {
    $group: {
      _id: "$merchant_details.name",
      total_transactions: { $count: {} }
    }
  },
  {
    $project: {
      _id: 0,
      merchant_name: "$_id",
      total_transactions: 1
    }
  },
  {
    $sort: { total_transactions: -1 }
  },
  {
    $limit: 1
  }
]).toArray();

printjson(result_5_4);


// End of script
print("\n>>> EXECUTION COMPLETE. END OF FILE. <<<");
