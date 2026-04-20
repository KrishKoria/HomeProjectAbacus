# **WEEK 2 – Data Visibility & Exploration**

## **🎯 Goal**

👉 **Understand data deeply \+ create basic analytics**

---

## **📥 INPUT**

* Bronze tables:  
  * `bronze_claims_raw`  
  * `bronze_provider_raw`  
  * `bronze_diagnosis_raw`  
  * `bronze_cost_raw`

---

## **⚙️ PROCESS**

### **Step 1: Explore Data**

* Check:  
  * top rows  
  * unique values  
  * distribution

---

### **Step 2: Basic SQL Analysis**

* Example:  
  * total claims  
  * claims per provider  
  * avg billed amount  
  * claims per diagnosis

---

### **Step 3: Join Data (Basic)**

* Join:  
  * claims \+ provider  
  * claims \+ diagnosis

---

### **Step 4: Create Simple Views**

* Example:  
  * claims by specialty  
  * claims by region  
  * high-cost claims

---

### **Step 5: Build Dashboard (Basic)**

* Use:  
  * Databricks / Power BI

---

## **📤 OUTPUT**

You must have:

✅ Summary tables  
 ✅ Basic joins working  
 ✅ Dashboard showing:

* total claims  
* cost trends  
* provider activity

---

## **🎯 FINAL OUTPUT**

Raw Data → Insights (no cleaning yet)  
---

# **📄 WEEK 3 – Data Cleaning & Silver Layer**

## **🎯 Goal**

👉 **Clean data \+ create trusted dataset (Silver layer)**

---

## **📥 INPUT**

* Bronze tables

---

## **⚙️ PROCESS**

### **Step 1: Handle Missing Values**

* Fill / remove:  
  * missing diagnosis  
  * missing provider

---

### **Step 2: Remove Duplicates**

* Ensure:  
  * unique claims  
  * unique providers

---

### **Step 3: Fix Data Types**

* Convert:  
  * dates → proper format  
  * amounts → numeric

---

### **Step 4: Standardize Data**

* Fix:  
  * ICD/CPT format  
  * text case  
  * spacing

---

### **Step 5: Create Silver Tables**

Create:

silver\_claims  
silver\_provider  
silver\_diagnosis  
silver\_cost  
---

### **Step 6: Validate Data**

* Check:  
  * no null critical fields  
  * correct joins  
  * consistent values

---

## **📤 OUTPUT**

You must have:

✅ Clean datasets  
 ✅ Silver tables  
 ✅ Data quality improved

---

## **🎯 FINAL OUTPUT**

Raw Data → Clean Data (Ready for features)  
---

# **🔥 FINAL SUMMARY**

| Week | Focus | Output |
| ----- | ----- | ----- |
| Week 2 | Explore data | Insights \+ dashboard |
| Week 3 | Clean data | Silver tables |

---

# **🧠 ONE LINE**

👉  
 **Week 2 \= Understand data**  
 **Week 3 \= Fix data**

