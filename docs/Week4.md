thi

# **📄 WEEK 4 – MACHINE LEARNING (DENIAL PREDICTION)**

## **Project: AI-Powered Claim Denial Prevention System**

---

# **🎯 1\. Week 4 Goal (Simple)**

👉 **Build ML model to predict claim denial**

Till Week 3:

- You have **clean data (Silver)**
- Now you will create **features \+ train model**

---

# **🧠 2\. What You Are Doing in Week 4**

You are converting:

Clean Data → Smart Data → ML Prediction

👉 System will answer:

👉 **“Will this claim be denied?”**

---

# **📥 3\. INPUT (What You Need)**

From Week 3:

### **Silver Tables**

- `silver_claims`
- `silver_provider`
- `silver_diagnosis`
- `silver_cost`

---

### **Required Columns**

From claims:

- claim_id
- provider_id
- diagnosis_code
- procedure_code
- billed_amount

From provider:

- specialty
- location

From diagnosis:

- category
- severity

From cost:

- avg_cost

---

### **Target Column (VERY IMPORTANT)**

👉 You need:

denial_flag (0 \= approved, 1 \= denied)

👉 If not available → create synthetic

---

# **⚙️ 4\. PROCESS (Step-by-Step)**

---

## **Step 1: Join Data (Create Base Table)**

claims \+ provider \+ diagnosis \+ cost

👉 Output table:

gold_claim_base

---

## **Step 2: Feature Engineering (MOST IMPORTANT)**

Create features like:

### **💰 Cost Features**

- billed_amount
- billed vs avg_cost
- high_cost_flag

---

### **👨‍⚕️ Provider Features**

- provider_specialty
- provider_claim_count
- provider_risk_score

---

### **🧬 Diagnosis Features**

- diagnosis_count
- severity_score

---

### **📊 Claim Features**

- claim_frequency
- claim_type

---

👉 Final table:

gold_claim_features

---

## **Step 3: Prepare Dataset**

- Remove nulls
- Encode categorical data
- Split:

Train (70%) / Test (30%)

---

## **Step 4: Train Model**

Start simple:

### **Models:**

- Logistic Regression (basic)
- XGBoost (advanced)

---

## **Step 5: Predict**

Model gives:

Denial Probability \= 0.82

---

## **Step 6: Evaluate Model**

Check:

- Accuracy
- Precision
- Recall
- ROC-AUC

---

## **Step 7: Save Model**

Save for next week:

claim_denial_model.pkl

---

# **📤 5\. OUTPUT (What You Must Deliver)**

---

## **📊 Output 1: Gold Table**

gold_claim_features

✔ Ready for ML

---

## **🤖 Output 2: Trained Model**

- Model trained
- Saved

---

## **📈 Output 3: Model Performance**

Example:

- Accuracy: 85%
- ROC-AUC: 0.90

---

## **📄 Output 4: Feature List**

Document:

- Which features used
- Why used

---

## **🧠 Output 5: Sample Prediction**

Example:

Claim ID: 101  
Risk: HIGH (0.82)

---

# **🎯 6\. FINAL OUTPUT (Simple View)**

Clean Data → Features → ML Model → Risk Score

---

# **🧪 7\. Testing**

Check:

- Model runs without error
- Prediction works for new claim
- No missing feature
- Output makes sense

---

# **⚠️ 8\. Common Mistakes**

❌ Using raw data instead of Silver  
❌ Not creating features  
❌ Training without target  
❌ Overcomplicated model  
❌ No evaluation

---

# **🧩 9\. Week 4 Success Criteria**

You are successful if:

✅ Gold feature table created  
✅ Model trained  
✅ Model evaluated  
✅ Prediction working  
✅ Model saved

---
