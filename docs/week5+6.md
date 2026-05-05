

# **📄 WEEK 5 \+ WEEK 6**

## **Explainable AI \+ RAG (Reason \+ Policy-Based Explanation)**

---

# **🎯 1\. Goal (Simple)**

👉 Build a system that not only predicts risk but also:

Tells WHY claim is risky  
\+  
Shows POLICY behind it

---

# **🧠 2\. What You Are Building**

Convert:

Prediction → Reason → Policy Explanation

👉 System will say:

* Why risky (feature-based)  
* Which rule/policy supports it

---

# **📥 3\. INPUT**

From previous weeks:

### **📊 Data**

* `gold_claim_features`

### **🤖 Model**

* `claim_denial_model`

### **📄 Documents**

* Policy docs (PDF/Text)  
* Insurance rules  
* ICD/CPT guidelines

---

# **⚙️ 4\. PROCESS**

---

## **🔹 PART 1 (Week 5 – Explainable AI)**

### **Step 1: Get Feature Importance**

Find which features impacted prediction

Example:

high\_cost → high impact  
missing\_diagnosis → high impact

---

### **Step 2: Convert to Business Reason**

high\_cost → "Claim amount is higher than average"  
missing\_diagnosis → "Diagnosis is missing"

---

### **Step 3: Create Explanation Table**

gold\_claim\_explanations

---

---

## **🔹 PART 2 (Week 6 – RAG System)**

---

### **Step 4: Load Policy Documents**

Examples:

* Insurance rules  
* Medical necessity guidelines

---

### **Step 5: Convert to Chunks**

Split documents into small parts

---

### **Step 6: Create Embeddings**

* Convert text → vectors

---

### **Step 7: Store in Vector DB**

* FAISS / Databricks vector search

---

### **Step 8: Retrieve Relevant Policy**

Input:

Claim \+ Reason

Output:

Matching policy text

---

### **Step 9: Combine Reason \+ Policy**

Final output:

Reason:  
\- High billing amount

Policy:  
\- Claims above threshold require justification

---

# **📤 5\. OUTPUT**

---

## **📊 Output 1: Explanation Table**

claim\_id | risk | reason\_1 | reason\_2

---

## **📄 Output 2: Policy Table**

claim\_id | policy\_text

---

## **🧠 Output 3: Final Combined Output**

Example:

Claim ID: 101  
Risk: HIGH (0.82)

Reasons:  
\- High billing amount  
\- Missing diagnosis

Policy:  
\- Claims above average cost need justification  
\- Diagnosis must support procedure

---

# **🎯 6\. FINAL FLOW**

Claim  
→ ML Model  
→ Risk Score  
→ Feature Explanation  
→ RAG (Policy Retrieval)  
→ Final Explanation

---

# **🧪 7\. Testing**

Check:

* Reasons match prediction  
* Correct policy retrieved  
* Works for different claims  
* No irrelevant policy

---

# **⚠️ 8\. Common Mistakes**

❌ Showing raw ML features  
❌ Wrong policy retrieval  
❌ No mapping between reason & policy  
❌ Too long explanation

---

# **🧩 9\. Success Criteria**

You are successful if:

✅ Explanation working  
✅ Policy retrieval working  
✅ Combined output ready  
✅ User understands reason clearly

---

# **🧠 FINAL SUMMARY**

👉 Week 5 \+ 6 \= **Explain \+ Justify**

Prediction  
→ Reason (ML)  
→ Policy (RAG)  
→ Full Explanation

---

