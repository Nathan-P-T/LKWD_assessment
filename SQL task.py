
import pandas as pd
from sqlalchemy import text,create_engine

engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/postgres')


#%% Q1 task
"""
1. Write a SQL query to report on the first product sold
   each month.

    Assumptions:
        - 'cancelled'/'on hold' still count as 'sold'
        - only one item requested (otherwise would use dense_rank)
"""

sql="""
select 
    product_code
    ,month_id
    ,year_id
from(
    select
        row_number () over (partition by sd.month_id,sd.year_id order by sd.order_date asc) rank_in_month
        ,*
    from 
        public.sales_data sd)
where 
    rank_in_month=1
order by 
    year_id, month_id asc;

"""
q1_answer = pd.read_sql(sql, engine)

#%% Q2 task
"""
2. Write a SQL query to report on any products that are common between
   the two highest value customers.
    
Assumptions:
    - 'cancelled'/'on hold' still count as 'sold'
"""

sql="""

with top_2_customers as (
    select 
         customer_name
    from
        public.sales_data
    group by
        customer_name
    order by
        sum(sales) desc
    limit 2
),

customer_products as (
    select 
        product_code
        ,customer_name
    from
        public.sales_data
    where
       customer_name in (select customer_name from top_2_customers)
)

select 
    distinct c1p.product_code
from 
    customer_products c1p
group by
    c1p.product_code
having 
    count(distinct customer_name) = 2;

"""
q2_answer = pd.read_sql(sql, engine)

#%% Q3 task

"""
3. Write a SQL query to find all occurrences where a customer ordered an item *AND*
   the quantity ordered was greater than their previous order of the same item.
	 For example, if `Company A` orders 10 `shiny_widget`s on 2025-01-01 and
	 then orders 11 or more `shiny_widget`s on 2025-01-03, then return the 2025-01-03
	 order in your results.

Criteria:     
define repeat orders:
    - orders where:
        - customer name is matching, and
        - product_code is the same, and
        - date is later, and
        - quantity_ordered is greater
- could use rank partitioned by customer name and product code, ordered by date, then self join (where rank=rank+1, etc.)
- could use lag

Assumptions:
    - every single instance that is greater than the previous instance only (not greater than all previous instances)
  
isolate an example customer that has many orders of the same item:
sql=
select 
    customer_name
    ,product_code
    ,count(product_code)
from
    public.sales_data
group by customer_name,product_code
q3_answer = pd.read_sql(sql, engine)

customer_name='Euro Shopping Channel' and product_code = 'S18_3232' -> 8 times

"""

sql="""
select
    order_number
    ,customer_name
    ,product_code
    ,order_date
    ,qty
    ,prev_qty
from (
    select
        order_number
        ,customer_name
        ,product_code
        ,order_date
        ,quantity_ordered as qty
        ,lag(quantity_ordered) over (
            partition by customer_name, product_code --same product AND customer
            order by order_date
        ) as prev_qty
    from public.sales_data
) 
where 
    qty > prev_qty
    -- and customer_name = 'Euro Shopping Channel'  and product_code = 'S18_3232'
"""

q3_answer = pd.read_sql(sql, engine)


#%% Q4 task
"""
4. Write a SQL query to produce a daily rollup for sales.
   * The result should have the following columns:
   	 * date - The date
   	 * product_code - One line per date per product code
   	 * sales_today - The total sales for the current date
   	 * sales_to_date - A running total of sales
   	 * sales_45d - The total sales in the last 45 days
   * The report should be *dense*, meaning that if a product is sold on day `n`
		 and again on day `n+3`, but *not* in-between, there should still be rows
		 for that product on day `n+1` and `n+2` with 0 sales recorded for those days.
		 The `d_date` dimension table has been provided to help with this.
   * If you have time, think about how your query could be used and/or structured
     to *incrementally* update a daily rollup.

"""

sql="""
create table f_sales_daily_rollup as


with all_dates as (
    select date_actual
    from public.d_date
    where date_actual between 
        (select min(date(order_date)) from public.sales_data)
        and 
        (select max(date(order_date)) from public.sales_data)
),

all_products as (
    select
        distinct product_code 
    from 
        public.sales_data),

daily_sales as (
    select 
        d.date_actual
        ,p.product_code
        ,coalesce(sum(sd.sales),0) as total_sales
    from 
        all_dates d
    cross join
        all_products p
    left join
        public.sales_data sd 
    on 
        date(sd.order_date)=d.date_actual
        and p.product_code=sd.product_code
    group by
        d.date_actual,p.product_code)
select
    *
    ,sum(total_sales) over (partition by product_code order by date_actual) as sales_to_date 
    ,sum(total_sales) over (partition by product_code order by date_actual range between interval '44 days' preceding and current row) as sales_45d
from
    daily_sales
order by
    date_actual,product_code
"""
engine.execute(text(sql))
q4_answer = pd.read_sql("select * from public.f_sales_daily_rollup", engine)

#%% Q4 extension

"""

If you have time, think about how your query could be used and/or structured
  to *incrementally* update a daily rollup.

Assumption:
    - previous table saved as f_sales_daily_rollup
    
Steps:
    - define (using f_sales_daily_rollup) the most recent date (most_recent_date_CTE) and the date 45 days prior to that
    - extract from f_sales_daily_rollup the product sales for those dates
    - create product spine as not all products may be in todays sales
    - calculate metrics for today's date (filtering the underlying public.sales_data where order_date>(select max(date_actual) from f_sales_daily_rollup))
    - left join the most_recent_date_CTE to this new table, adding most_recent_date_CTE's cumulative sales to get total_sales_to_date
    - left join most_recent_date_CTE to most recent date - 44 days and minus the most_recent_date_CTE's sales_45d, then add today's sales
    - insert where not exists

"""

sql="""
-- get the day after what's already been calculated
with next_business_day_CTE as (
    select 
        min(date_actual) as date_actual
    from 
        public.d_date
    where 
        date_actual > (select max(date_actual) from f_sales_daily_rollup)
),


-- get the most recent sales per product
most_recent_sales_per_product_CTE as (
    select 
        *
    from
        f_sales_daily_rollup
    where
    date_actual = (select max(date_actual) from f_sales_daily_rollup)
    ),

-- get sales from 44 days ago to adjust 45-day window
sales_45_days_ago_CTE as(
    select
        product_code
        ,total_sales as sales_45d_lagged
    from 
        f_sales_daily_rollup
    where
        date_actual =  (select max(date_actual) from f_sales_daily_rollup) - interval '44 days'
    ),

-- product spine in case some products arent sold today
all_products_CTE as (
    select
        distinct product_code 
    from 
        public.sales_data),

-- calculate todays sales
todays_sales_raw_CTE as(
    select
        (select date_actual from next_business_day_CTE) as date_actual
        ,product_code
        ,sum(sales) as total_sales
    from
        public.sales_data
    where
        order_date = (select date_actual from next_business_day_CTE)
    group by
        product_code
    ),

-- merge calculated sales with product spine
todays_sales_CTE AS (
    select
        (select date_actual from next_business_day_CTE) as date_actual
        ,p.product_code
        ,coalesce(tsr.total_sales, 0) AS total_sales
    from
        all_products_CTE p
    left join
        todays_sales_raw_CTE tsr
    on p.product_code = tsr.product_code
)

-- then using the 2 previous dates (to calculate the 45 days lag) plus a summary of todays sales, insert into the rollup table:
insert into 
    f_sales_daily_rollup(
            date_actual
            ,product_code
            ,total_sales
            ,sales_to_date
            ,sales_45d)
    select
        today.date_actual
        ,today.product_code
        ,today.total_sales
        ,coalesce(prev.sales_to_date, 0) + today.total_sales as sales_to_date
        ,coalesce(prev.sales_45d, 0)-coalesce(lag.sales_45d_lagged, 0) + today.total_sales as sales_45d
    from 
        todays_sales_CTE today
    left join
        most_recent_sales_per_product_CTE prev
    on
        today.product_code = prev.product_code
    left join
        sales_45_days_ago_CTE lag
    on
        today.product_code = lag.product_code
where not exists (
    select 
        1 -- not actually selecting '1'
    from 
        f_sales_daily_rollup existing
    where 
        existing.date_actual = today.date_actual
        and existing.product_code = today.product_code
);
"""
engine.execute(text(sql))
