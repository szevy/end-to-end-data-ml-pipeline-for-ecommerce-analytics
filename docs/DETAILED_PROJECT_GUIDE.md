# Detailed Project Guide: Architecture, Data Model, and ML Insights

---

## Table of Contents

* [Detailed Project Architecture](#detailed-project-architecture)
    * [Raw Data Ingestion (`kaggle_raw_data`)](#raw-data-ingestion-kaggle_raw_data)
    * [Data Cleaning (`cleaned_dataframes`)](#data-cleaning-cleaned_dataframes)
    * [Raw Data Loading to BigQuery (`bigquery_raw_tables`)](#raw-data-loading-to-bigquery-bigquery_raw_tables)
    * [dbt Transformations (`olist_dbt_models`)](#dbt-transformations-olist_dbt_models)
    * [Customer Segmentation ML Pipeline (`som_kmeans_segmentation_pipeline`)](#customer-segmentation-ml-pipeline-som_kmeans_segmentation_pipeline)
* [Data Model (Star Schema)](#data-model-star-schema)
    * [Common Dimension Tables](#common-dimension-tables)
    * [Sales Mart](#sales-mart)
    * [Operations Mart](#operations-mart)
    * [Finance Mart](#finance-mart)
    * [Customer Service Mart](#customer-service-mart)
* [Customer Segmentation ML Model - In-Depth](#customer-segmentation-ml-model---in-depth)
    * [Model Overview](#model-overview)
    * [Pipeline Steps](#pipeline-steps)
    * [Visualizations](#visualizations)
    * [Hyperparameter Tuning & Justification](#hyperparameter-tuning--justification)
    * [Model Validation Metrics](#model-validation-metrics)
    * [Customer Segment Interpretation and Naming](#segment-naming-and-actionable-insights)

---

## Detailed Project Architecture

The data pipeline is orchestrated using Dagster, providing a clear and observable lineage from raw data ingestion to transformed analytical models and advanced machine learning outputs. The architecture follows a modern ELT (Extract, Load, Transform) approach, leveraging Google BigQuery as the cloud data warehouse and dbt for robust data transformations. **Both the core ELT pipeline (`python_elt_job`) and the customer segmentation ML pipeline (`som_kmeans_segmentation_pipeline`) are fully orchestrated within Dagster.**

### Raw Data Ingestion (`kaggle_raw_data`)

* CSV files from the Olist dataset are extracted directly from Kaggle.
* Raw data is stored locally in the `raw_data` directory.
* The `product_category_name_translation.csv` is specifically placed in the dbt seeds directory for direct use by dbt.

### Data Cleaning (`cleaned_dataframes`)

Raw CSVs are read into Pandas DataFrames, where a comprehensive cleaning process is applied:

* Deduplication of records.
* Standardization of text fields (handling encoding, special characters, newlines).
* Crucially, date columns are converted to proper datetime objects to ensure correct type inference in BigQuery, preventing data type errors in downstream transformations.

### Raw Data Loading to BigQuery (`bigquery_raw_tables`)

* The cleaned Pandas DataFrames are loaded into Google BigQuery as raw tables within the `raw_<PROJECT_NAME>` dataset.
* A `load_timestamp` column - adjusted to Brazilian time for consistency with the dataset’s timestamps, is added to each table to track when the data was ingested into the warehouse.

### dbt Transformations (`olist_dbt_models`)

This stage is the core of the data transformation, where raw data is refined into analytical models:

* **Staging Models:** These are simple transformations on the raw BigQuery tables to clean, standardize, and prepare data for the final analytical layer. They typically involve renaming columns, casting data types, and basic cleaning.

* **Mart Models (Star Schema):** These are the core analytical tables, designed in a star schema fashion to support business intelligence queries efficiently. They include both dimension (`dim_*`) and fact (`fact_*`) tables.

* **Data Quality Checks:** Extensive dbt tests (including `dbt-utils` and `dbt-expectations`) are applied throughout the staging and mart layers to ensure data integrity, validity, and consistency.

This modular design ensures maintainability, testability, and clear separation of concerns, providing a reliable foundation for data analysis.

### Customer Segmentation ML Pipeline (`som_kmeans_segmentation_pipeline`)

This newly integrated pipeline extends the project's capabilities by performing advanced machine learning on the refined data marts.

* **Purpose:** To identify distinct customer groups based on their behavior, providing actionable insights for targeted marketing and business strategy.
* **Starting Point:** This pipeline begins by consuming the already transformed and validated data from the dbt mart models (e.g., `dim_customer`, `fact_orders`, `fact_sales`, etc.).
* **Key Steps:** It orchestrates the entire ML workflow, including feature engineering, Self-Organizing Map (SOM) training, K-Means clustering on SOM neurons, automated model validation, and the materialization of final segmented customer data.
* **Outputs:** Produces `dim_customer_segmented` (with assigned segment IDs) and `dim_segment` (a lookup table for segment details) in the `analytics_<PROJECT_NAME>` BigQuery dataset.
* **Dagster Integration:** Fully defined as a Dagster job, allowing for independent scheduling, monitoring, and automatic dependency resolution with upstream dbt assets.

---

## Data Model (Star Schema)

The project implements a star schema in Google BigQuery, optimized for analytical querying across various business domains. The core of this schema consists of central fact tables surrounded by conformed dimension tables.

### Common Dimension Tables

These dimensions are used across multiple data marts:

* **`dim_date`**: A comprehensive date dimension table, generated programmatically, containing various attributes like year, month, day, day of week, week of year, quarter, holiday flags, and season, enabling flexible time-based analysis.

* **`dim_customer`**: Contains unique customer information, including `customer_unique_id`, geographic details (`zip_code_prefix`, `city`, `state`), essential for understanding customer demographics and behavior.

* **`dim_product`**: Details about products sold, such as `product_category_name`, dimensions (`product_height_cm`, `product_length_cm`, `product_width_cm`), and weight, crucial for product performance analysis.

* **`dim_seller`**: Information about sellers, including their `seller_id` and geographic location (`zip_code_prefix`, `city`, `state`), enabling analysis of seller performance and distribution.

* **`dim_geolocation`**: A granular dimension providing latitude and longitude for zip codes, used to enrich customer and seller locations, enabling spatial analysis.

### Sales Mart

The Sales Mart focuses on transactional sales data, allowing for analysis of sales performance, product popularity, and revenue.

![Sales Mart](../assets/Sales%20Mart.png)

* **`fact_sales`**: This fact table captures key metrics related to individual product sales within an order. It includes `order_id`, `product_id`, `seller_id`, `customer_id`, `price`, `freight value`, `total item value`, `order count`, and a `recognized_revenue` flag. It links to `dim_date` via `order_purchase_date_id`, and to `dim_customer`, `dim_product`, and `dim_seller`.

### Operations Mart

This mart provides insights into the operational aspects of orders, focusing on delivery, approval, and fulfillment timelines.

![Operations Mart](../assets/Operations%20Mart.png)

* **`fact_orders`**: Provides a comprehensive view of each order, including various timestamps (purchase, approved, carrier, customer delivery, estimated delivery), order status, and links to customer, product, and seller dimensions. This allows for tracking order lifecycle and identifying bottlenecks.

* **`fact_fulfillment`**: Focuses on the shipping and delivery performance, including `shipping_limit_date`, `actual_delivery_days`, `estimated_delivery_days`, and a `delivery_delay_flag`. This table is crucial for assessing logistics efficiency.

### Finance Mart

The Finance Mart is designed for analyzing payment methods, installment plans, and total payment values.

![Finance Mart](../assets/Finance%20Mart.png)

* **`fact_payments`**: This fact table details payment transactions, including `payment_type`, `payment_installments`, `payment_value`, and flags for installment and credit card payments. It links to `dim_date` and `dim_customer`, enabling financial performance analysis.

### Customer Service Mart

This mart is dedicated to understanding customer satisfaction and feedback through review data.

![Customer Service Mart](../assets/Customer%20Service%20Mart.png)

* **`fact_reviews`**: Captures details about customer reviews, including `review_score`, `review_comment_message`, `review_creation_date`, `review_answer_date`, and flags for positive/negative reviews. It links to `dim_customer` and `dim_date`, allowing for analysis of customer sentiment and service response times.

---

## Customer Segmentation ML Model - In-Depth

### Model Overview

The Customer Segmentation stage extends the analytical capabilities by identifying distinct customer groups based on their behavior, leveraging the refined data from the dbt mart models.

* **Self-Organizing Map (SOM):** An unsupervised neural network that reduces high-dimensional data into a low-dimensional (typically 2D) grid of neurons while preserving the topological properties of the input space. This helps in visualizing and understanding complex relationships between customer features.

* **K-Means Clustering:** Applied to the weights of the trained SOM neurons. This step groups similar neurons into distinct clusters, which then represent the final customer segments. This two-step approach (SOM for dimensionality reduction and topology preservation, then K-Means for clustering) often yields more robust and interpretable clusters than applying K-Means directly to high-dimensional data.

### Pipeline Steps

1.  **Feature Engineering:** Relevant customer attributes (e.g., RFM - Recency, Frequency, Monetary, order statistics, payment behavior, review scores, product diversity) are engineered from `dim_customer`, `fact_orders`, `fact_sales`, `fact_payments`, `fact_reviews`, and `dim_product`.

2.  **Data Scaling:** Features are scaled using `StandardScaler` to ensure all features contribute equally to the distance calculations in SOM and K-Means.

3.  **Self-Organizing Map (SOM) Training:** A `MiniSom` model is trained on the scaled customer features, mapping customers to a 2D grid of neurons.

4.  **K-Means Clustering on SOM Neurons:** K-Means is applied to the trained SOM neuron weights to identify distinct clusters (segments) within the customer space. The optimal number of clusters (n_clusters=3) was determined using the Elbow Method with `kneed` algorithm.

5.  **Segment Assignment:** Each customer is assigned a `segment_id` based on their Best Matching Unit (BMU) on the SOM and the K-Means cluster of that neuron.

6.  **Materialization to BigQuery:**
    * An updated `dim_customer_segmented` table is created in a dedicated processed BigQuery dataset, including the new `segment_id` for each customer.
    * A new `dim_segment` lookup table is created, providing human-readable names and descriptions for each customer segment, based on in-depth analysis of their defining characteristics.

### Visualizations

Key visualizations are generated to aid in model interpretation and segment naming.

**SOM U-Matrix**

![SOM U-Matrix](../assets/SOM%20U-Matrix.png)

**SOM Component Planes**

![SOM Component Planes](../assets/Component%20Planes.png)

**SOM U-Matrix with Customer Data Points Mapped**

![SOM U-Matrix with Customer Data Points](../assets/SOM%20U-Matrix%20with%20Data%20Points.png)

**SOM Map with Segment Numbers and Names**

![SOM Map with Segment Numbers and Names](../assets/SOM%20Map%20with%20Segments.png)

**Segment Profile Dashboard: Key Feature Distributions**

![Customer Segment Profile Dashboard](../assets/Customer%20Segment%20Profile%20Dashboard.png)

### Hyperparameter Tuning & Justification

The SOM model's performance was optimized by tuning key hyperparameters:

* **m x n (SOM grid dimensions):** Set to 80x40 to provide sufficient resolution for the data.

* **sigma (neighborhood radius):** Explored values around 1.0-1.2. A sigma of 1.2 was chosen as it yielded a significantly improved Topographic Error (TE), indicating better preservation of the data's topological structure.

* **learning_rate:** Adjusted around 0.1-0.2. A `learning_rate` of 0.15 was selected to balance convergence speed and model accuracy.

* **num_iterations:** Increased iteratively from 500,000 up to 1,000,000. 

* **n_clusters (for K-Means):** Set to 3 based on the clear "elbow" observed in the Elbow Method plot on SOM neurons, suggesting 3 distinct and well-separated clusters, and using `kneed` computational tool to confirm.

![Elbow Plot](../assets/Elbow%20Plot.png)

### Model Validation Metrics

The current model demonstrates excellent and well-balanced performance across all key validation metrics, indicating a robust and highly meaningful customer segmentation:

* **Quantization Error (QE):** 0.2444 (Target: <0.25) - This value is comfortably within the desired target range, indicating a very good representation of the high-dimensional data by the SOM map.

* **Topographic Error (TE):** 0.2412 (Acceptable: <0.25, Good: <0.15) - This value is well within the "acceptable" range and very close to the "good" threshold, demonstrating strong preservation of the data's topological relationships on the SOM map.

* **Silhouette Score: 0.6967** (Reasonable: 0.5-0.7, Strong: 0.7-1.0) - This score is at the very high end of the "reasonable" range, nearing the "strong" category. It indicates that the clusters are highly compact and exceptionally well-separated, with data points clearly belonging to their assigned cluster.

* **Davies-Bouldin Index (DBI):** 0.6919 (Good: <1.0, Excellent: <0.5) - This value is comfortably within the "good" range and very close to the "excellent" threshold. It further confirms the distinctness and density of the clusters, reinforcing the high quality of the segmentation.

### Segment Naming and Actionable Insights
---

**Segment 0 (ID: 0 - Purple Cluster): "The Vast Unengaged / Lapsed Base"**

* **Location on SOM Map:** This is the **dominant segment**, covering the vast majority of the SOM map (the large purple area).
* **Key Characteristics (from Component Planes in the purple regions):**
    * **Recency:** Very High (meaning a long time since their last purchase).
    * **Frequency:** Very Low (likely single purchases).
    * **Monetary:** Very Low (small total spending).
    * **Number of Orders:** Very Low (mostly one order).
    * **Avg Order Value, Total Order Items, Total Payment Value:** All very low.
    * **Avg Payment Installments:** Low/None.
    * **Avg Review Score:** Generally average to slightly below average.
    * **Num Reviews:** Low (typically one review).
    * **Avg Product Category Value, Num Unique Products:** Low.

* **Useful Insights & Strategy:**
    * **Insight:** This segment represents a massive pool of customers who made a single, low-value purchase a long time ago and have since become inactive. They are likely "one-and-done" customers who either didn't find compelling reasons to return or whose initial purchase didn't lead to further engagement.
    * **Challenge:** Reactivating this segment is difficult due to their low historical engagement and long dormancy. Their sheer size means even a small success rate can yield results, but efforts must be cost-effective.
    * **Strategy:**
        * **Low-Cost Reactivation Campaigns:** Focus on highly targeted, low-cost campaigns (e.g., email re-engagement with strong discounts on entry-level products, or surveys to understand why they haven't returned). Avoid high-cost advertising for this group unless specifically targeting reactivation.
        * **Churn Prevention (Past):** For newly identified customers falling into this pattern, proactive intervention is key.
        * **Audience Segmentation for Ads:** Exclude this segment from broad, high-cost advertising campaigns to optimize ad spend.
        * **Data Quality Check:** Investigate if some of these are truly inactive or if there are data collection issues.

---

**Segment 1 (ID: 1 - Teal/Green Cluster): "The Engaged & Growing Buyers"**

* **Location on SOM Map:** This segment forms a significant, connected cluster, primarily in the middle-right and bottom-left regions of the map.
* **Key Characteristics (from Component Planes in the teal/green regions):**
    * **Recency:** Low (meaning recent purchases).
    * **Frequency:** Moderate to High (more frequent purchases than Segment 0).
    * **Monetary:** Moderate to High (higher total spending).
    * **Number of Orders:** Higher.
    * **Avg Order Value, Total Order Items, Total Payment Value:** Moderate to High.
    * **Avg Payment Installments:** Higher (indicating use of credit/installments for larger purchases).
    * **Avg Review Score:** High (positive feedback).
    * **Num Reviews:** Higher.
    * **Avg Product Category Value, Num Unique Products:** Moderate to High.

* **Useful Insights & Strategy:**
    * **Insight:** These are active, valuable, and likely growing customers. They purchase recently, more frequently, spend more, and are generally satisfied. They are key to your business's ongoing revenue and have potential for increased loyalty and value.
    * **Opportunity:** Nurture loyalty, maximize Customer Lifetime Value (CLV), and encourage advocacy.
    * **Strategy:**
        * **Loyalty Programs:** Offer exclusive discounts, early access to new products, or loyalty points to reward their engagement.
        * **Personalized Recommendations:** Leverage their purchase history (product categories, unique products) to offer highly relevant cross-sell and up-sell recommendations.
        * **Retention Campaigns:** Focus on ensuring continued satisfaction and repeat purchases through personalized communication.
        * **Feedback Loop:** Actively solicit and respond to reviews and feedback to maintain high satisfaction and address any issues promptly.
        * **Installment Promotion:** If they use installments, highlight flexible payment options for higher-value items or encourage larger basket sizes.

---

**Segment 2 (ID: 2 - Yellow Cluster): "The Recent, Highly Satisfied Explorers"**

* **Location on SOM Map:** This segment appears in smaller, more scattered clusters, notably in the top-right and some isolated spots.
* **Key Characteristics (from Component Planes in the yellow regions):**
    * **Recency:** Very Low (meaning very recent purchases).
    * **Frequency:** Very Low (almost exclusively single purchases).
    * **Monetary:** Low (small total spending).
    * **Number of Orders:** Very Low (likely one order).
    * **Avg Order Value, Total Order Items, Total Payment Value:** All low.
    * **Avg Payment Installments:** Low/None.
    * **Avg Review Score:** **CRITICALLY: Very High (Bright Yellow)** $\rightarrow$ Extremely Positive Reviews. This is their most distinguishing feature.
    * **Num Reviews:** Moderate (more than Segment 0, but less than Segment 1).
    * **Avg Product Category Value, Num Unique Products:** Low.

* **Useful Insights & Strategy:**
    * **Insight:** This segment consists of very recent, likely first-time buyers who, despite low initial spending, had an exceptionally positive experience. Their high review scores indicate strong satisfaction and significant potential for future growth. They are currently "explorers" of your platform.
    * **Opportunity:** Convert these satisfied first-time buyers into repeat customers and eventually into "Engaged & Growing Buyers." Their positive sentiment is a valuable asset.
    * **Strategy:**
        * **Immediate Post-Purchase Nurturing:** Send personalized follow-up emails (not just transactional) after their first purchase, highlighting benefits, complementary products, and encouraging a second purchase.
        * **Incentivize Repeat Purchase:** Offer a small, time-sensitive discount or free shipping on their next order to encourage a quick return.
        * **Feedback Amplification:** Encourage them to share their positive experience on social media, review platforms, or through referral programs.
        * **Onboarding:** Provide tips or guides related to their purchased product category to enhance their experience and demonstrate value beyond the initial purchase.
        * **Monitor Closely:** Track their behavior closely to see if they transition to Segment 1, indicating successful nurturing.

---

**Conclusion: Actionable Segmentation and Future Refinement**

This 3-segment model provides a clear, actionable, and statistically robust breakdown of the customer base. The excellent Silhouette and Davies-Bouldin scores, coupled with the well-organized SOM, indicate that these segments are distinct, internally cohesive, and highly interpretable for strategic decision-making.

It's important to note that for an initial customer segmentation model, starting with a smaller, more manageable number of clusters (like these 3) is often a pragmatic and effective approach. This allows the marketing, sales, and product teams to:

1.  **Gain immediate clarity:** Understand the core customer archetypes without being overwhelmed by excessive granularity.
2.  **Focus resources:** Prioritize and develop distinct, high-impact strategies for each major group.
3.  **Establish a baseline:** Measure the effectiveness of targeted interventions.

As the business implements specific actions based on these identified segments (e.g., reactivation campaigns for "Lapsed Customers," loyalty programs for "Engaged Buyers," nurturing for "Satisfied Explorers"), we should expect to see shifts in customer behavior and engagement patterns. If these interventions are successful, customers within these broad segments may begin to **differentiate further** based on their responses to your strategies.

In subsequent iterations of the segmentation model (e.g., next quarter or year), these behavioral shifts could naturally lead to the **emergence of more granular, distinct sub-segments or entirely new clusters**. This iterative process of applying business actions, observing changes, and then re-segmenting allows for a continuous refinement of business understanding of the customer base, enabling even more precise and effective targeting over time.

---


[Return to README.md](../README.md)