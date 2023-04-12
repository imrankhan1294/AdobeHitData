# AdobeAssessment

This code reads the CSV file in a Spark DataFrame API to identify how much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue.

The function find_top_ref() takes five parameters: df, hit_time_gmt, user_agent, ip, and page_url. The function filters the DataFrame df based on these parameters, and selects the columns "hit_time_gmt", "user_agent", "ip", "page_url", and "referrer". It then collects a list of referrers from the filtered DataFrame and checks if the first item in the list is not empty. If the referrer is not empty and does not contain "esshopzilla", it returns the referrer. Otherwise, it recursively calls find_top_ref() with the same parameters but with the referrer parameter replacing page_url. If the referrer is empty, the function returns an empty string.

The order_df DataFrame is created by filtering the hits_df DataFrame where the pagename column equals "Order Complete". An empty list called result_dict is created to hold the results.

A loop iterates over each row in the order_df DataFrame. For each row, the find_top_ref() function is called with the hits_df DataFrame and the hit_time_gmt, user_agent, ip, and page_url columns from the order_df DataFrame as arguments. If the result is not empty and does not contain "esshopzilla", the url, keyword, and revenue variables are assigned values based on the referrer and product list columns from the order_df DataFrame. A dictionary with keys "Search Engine Domain", "Search Keyword", and "Revenue" is created with the corresponding values and appended to the result_dict list.

The result_dict list is converted to a DataFrame called final_df using the parallelize() and toDF() methods.

The final_df DataFrame is grouped by the "Search Engine Domain" and "Search Keyword" columns and the "Revenue" column is summed using the groupBy() and agg() methods, respectively.

Finally, the final_df DataFrame is written out to a CSV file using the write() method with options to overwrite any existing file, use tab as the delimiter, and include a header row. The output file path is specified by the output_file variable.


Note: This code is developed in AWS Gluestudio & is generic & is able to deploy without a server.
