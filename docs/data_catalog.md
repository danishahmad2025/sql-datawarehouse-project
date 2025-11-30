

<h2>1. <code>gold.dim_customers</code></h2>
<p><strong>Purpose:</strong> Stores customer details enriched with demographic and geographic data.</p>

<table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse; width:100%; font-size:14px;">
<tr style="background:#e8e8e8; font-weight:bold;"><th>Column Name</th><th>Data Type</th><th>Description</th></tr>
<tr><td>customer_key</td><td>INT</td><td>Surrogate key uniquely identifying each customer record.</td></tr>
<tr><td>customer_id</td><td>INT</td><td>Unique ID assigned to each customer.</td></tr>
<tr><td>customer_number</td><td>NVARCHAR(50)</td><td>Alphanumeric customer identifier.</td></tr>
<tr><td>first_name</td><td>NVARCHAR(50)</td><td>Customer first name.</td></tr>
<tr><td>last_name</td><td>NVARCHAR(50)</td><td>Customer last name.</td></tr>
<tr><td>country</td><td>NVARCHAR(50)</td><td>Country of residence.</td></tr>
<tr><td>marital_status</td><td>NVARCHAR(50)</td><td>Marital status.</td></tr>
<tr><td>gender</td><td>NVARCHAR(50)</td><td>Gender of the customer.</td></tr>
<tr><td>birthdate</td><td>DATE</td><td>Date of birth.</td></tr>
<tr><td>create_date</td><td>DATE</td><td>Record creation date.</td></tr>
</table>

---

<h2>2. <code>gold.dim_products</code></h2>
<p><strong>Purpose:</strong> Contains product metadata, category, price, and lifecycle details.</p>

<table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse; width:100%; font-size:14px;">
<tr style="background:#e8e8e8; font-weight:bold;"><th>Column Name</th><th>Data Type</th><th>Description</th></tr>
<tr><td>product_key</td><td>INT</td><td>Surrogate key for product.</td></tr>
<tr><td>product_id</td><td>INT</td><td>Unique product ID.</td></tr>
<tr><td>product_number</td><td>NVARCHAR(50)</td><td>Alphanumeric product identifier.</td></tr>
<tr><td>product_name</td><td>NVARCHAR(50)</td><td>Name of the product.</td></tr>
<tr><td>category_id</td><td>INT</td><td>Category identifier.</td></tr>
<tr><td>category</td><td>NVARCHAR(50)</td><td>Category group.</td></tr>
<tr><td>subcategory</td><td>NVARCHAR(50)</td><td>More specific category.</td></tr>
<tr><td>maintenance_required</td><td>NVARCHAR(50)</td><td>Whether maintenance is required.</td></tr>
<tr><td>cost</td><td>INT</td><td>Base cost.</td></tr>
<tr><td>product_line</td><td>NVARCHAR(50)</td><td>Product line.</td></tr>
<tr><td>start_date</td><td>DATE</td><td>Product availability start date.</td></tr>
</table>

---

<h2>3. <code>gold.fact_sales</code></h2>
<p><strong>Purpose:</strong> Stores sales transactions for analytics and reporting.</p>

<table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse; width:100%; font-size:14px;">
<tr style="background:#e8e8e8; font-weight:bold;"><th>Column Name</th><th>Data Type</th><th>Description</th></tr>
<tr><td>order_number</td><td>NVARCHAR(50)</td><td>Sales order identifier.</td></tr>
<tr><td>product_key</td><td>INT</td><td>FK to product dimension.</td></tr>
<tr><td>customer_key</td><td>INT</td><td>FK to customer dimension.</td></tr>
<tr><td>order_date</td><td>DATE</td><td>Date the order was placed.</td></tr>
<tr><td>shipping_date</td><td>DATE</td><td>Date order was shipped.</td></tr>
<tr><td>due_date</td><td>DATE</td><td>Payment due date.</td></tr>
<tr><td>sales_amount</td><td>INT</td><td>Total sale amount.</td></tr>
<tr><td>quantity</td><td>INT</td><td>Quantity ordered.</td></tr>
<tr><td>price</td><td>INT</td><td>Price per unit.</td></tr>
</table>
